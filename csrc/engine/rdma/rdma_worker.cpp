#include "engine/rdma/rdma_worker.h"

#include "engine/rdma/rdma_endpoint_v0.h"
#include "engine/rdma/rdma_utils.h"
#include "logging.h"

#include <emmintrin.h> // for _mm_pause

namespace slime {

RDMAWorker::RDMAWorker(std::string dev_name, int id) 
    : device_name_(std::move(dev_name)), worker_id_(id) {}

RDMAWorker::~RDMAWorker() {
    stop();
}

void RDMAWorker::start() {
    bool expected = false;
    if (running_.compare_exchange_strong(expected, true)) {
        worker_thread_ = std::thread([this]() { this->workerLoop(); });
    }
}

void RDMAWorker::stop() {
    bool expected = true;
    if (running_.compare_exchange_strong(expected, false)) {
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
    }
}

void RDMAWorker::addEndpoint(std::shared_ptr<RDMAEndpointV0> endpoint) {
    std::lock_guard<std::mutex> lock(add_endpoint_mutex_);
    tasks_.push_back(EndpointTask{std::move(endpoint)});
}

void RDMAWorker::workerLoop() {
    // Bind the worker thread to the NUMA node associated with the RDMA device
    // to minimize PCIe and memory latency.
    bindToSocket(socketId(device_name_));
    SLIME_LOG_INFO("RDMA Worker Thread ", worker_id_, " started on ", device_name_);

    // Use a local copy of tasks to avoid locking in the hot path, 
    // assuming tasks are added rarely (control plane) but polled frequently (data plane).
    // Note: If dynamic removal is needed, a more complex lock-free structure or RCU is required.
    // For now, we take the lock only when updating the local list.
    std::vector<EndpointTask> local_tasks;

    while (running_.load(std::memory_order_relaxed)) {
        
        // Sync with main task list if needed (omitted for performance in simple version, 
        // strictly speaking should check a dirty flag or use lock-free queue).
        // Here we just check lock to see if we need to update.
        {
             // Optimization: Double-checked locking or atomic flag could be better here.
             // Given addEndpoint is rare, std::unique_lock with try_lock is a low-overhead check.
             std::unique_lock<std::mutex> lock(add_endpoint_mutex_, std::try_to_lock);
             if (lock.owns_lock() && local_tasks.size() != tasks_.size()) {
                 local_tasks = tasks_;
             }
        }
        
        if (local_tasks.empty()) {
             // Avoid busy loop if no endpoints are registered.
             std::this_thread::sleep_for(std::chrono::milliseconds(10));
             continue;
        }

        int total_work_done = 0;

        // Poll all endpoints.
        for (auto& task : local_tasks) {
            // process() functions now return the number of processed events/bytes
            // or 1 if busy, 0 if idle.
            total_work_done += task.endpoint->sendProcess();
            total_work_done += task.endpoint->recvProcess();
        }

        // Backoff Strategy:
        // Only relax the CPU if ALL endpoints are idle. 
        // This prevents one idle endpoint from slowing down other active endpoints.
        if (total_work_done == 0) {
            cpu_relax(); // _mm_pause()
        }
    }
}

} // namespace slime
