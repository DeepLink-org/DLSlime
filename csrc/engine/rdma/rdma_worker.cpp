#include "engine/rdma/rdma_worker.h"

// Include full definitions for method calls (inlining requires definition)
#include "engine/rdma/rdma_endpoint_v0.h"
#include "engine/rdma/rdma_io_endpoint.h"
#include "engine/rdma/rdma_utils.h"
#include "logging.h"

#include <emmintrin.h>  // for _mm_pause

namespace slime {

RDMAWorker::RDMAWorker(std::string dev_name, int id): device_name_(std::move(dev_name)), worker_id_(id) {}

RDMAWorker::~RDMAWorker()
{
    stop();
}

void RDMAWorker::start()
{
    bool expected = false;
    if (running_.compare_exchange_strong(expected, true)) {
        worker_thread_ = std::thread([this]() { this->workerLoop(); });
    }
}

void RDMAWorker::stop()
{
    bool expected = true;
    if (running_.compare_exchange_strong(expected, false)) {
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
    }
}

void RDMAWorker::addEndpoint(std::shared_ptr<RDMAEndpointV0> endpoint)
{
    std::lock_guard<std::mutex> lock(add_endpoint_mutex_);
    v0_tasks_.push_back(std::move(endpoint));
}

void RDMAWorker::addIOEndpoint(std::shared_ptr<RDMAIOEndpoint> endpoint)
{
    std::lock_guard<std::mutex> lock(add_endpoint_mutex_);
    io_tasks_.push_back(std::move(endpoint));
}

void RDMAWorker::workerLoop()
{
    // Bind the worker thread to the NUMA node associated with the RDMA device
    // to minimize PCIe and memory latency.
    bindToSocket(socketId(device_name_));
    SLIME_LOG_INFO("RDMA Worker Thread ", worker_id_, " started on ", device_name_);

    while (running_.load(std::memory_order_relaxed)) {

        int total_work_done = 0;

        // ============================================================
        // 1. Poll RDMAEndpointV0 (Send/Recv Pattern)
        // ============================================================
        for (auto& ep : v0_tasks_) {
            total_work_done += ep->sendProcess();
            total_work_done += ep->recvProcess();
        }

        // ============================================================
        // 2. Poll RDMAIOEndpoint (Read/Write Pattern)
        // ============================================================
        for (auto& ep : io_tasks_) {
            total_work_done += ep->process();
        }

        // ============================================================
        // 3. Centralized Backoff
        // ============================================================
        // Only relax the CPU if ALL endpoints (both V0 and IO) are idle.
        if (total_work_done == 0) {
            cpu_relax();  // _mm_pause()
        }
    }
}

}  // namespace slime
