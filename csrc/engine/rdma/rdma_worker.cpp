#include "engine/rdma/rdma_worker.h"
#include "engine/rdma/rdma_endpoint_v0.h" // 这里包含 Endpoint 的具体定义
#include "engine/rdma/rdma_utils.h"
#include "logging.h"
#include <emmintrin.h> // for _mm_pause
#include <mutex>

namespace slime {

RDMAWorker::RDMAWorker(std::string dev_name, int id) 
    : device_name_(std::move(dev_name)), worker_id_(id) {}

RDMAWorker::~RDMAWorker() {
    stop();
}

void RDMAWorker::start() {
    if (!running_) {
        running_ = true;
        worker_thread_ = std::thread([this]() { this->workerLoop(); });
    }
}

void RDMAWorker::stop() {
    running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void RDMAWorker::addEndpoint(std::shared_ptr<RDMAEndpointV0> endpoint) {
    tasks_.push_back(EndpointTask{std::move(endpoint)});
}

void RDMAWorker::workerLoop() {
    // 绑定 NUMA 节点
    bindToSocket(socketId(device_name_));
    SLIME_LOG_INFO("RDMA Worker Thread ", worker_id_, " started on ", device_name_);

    while (running_.load(std::memory_order_relaxed)) {
        bool idle = true;

        for (auto& task : tasks_) {
            // 执行 Send 逻辑
            int n_send = task.endpoint->sendProcess();
            
            // 执行 Recv 逻辑
            int n_recv = task.endpoint->recvProcess();

            if (n_send > 0 || n_recv > 0) {
                idle = false;
            }
        }

        // 自适应轮询
        if (idle) {
            _mm_pause();
        }
    }
}

} // namespace slime