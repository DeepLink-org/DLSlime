#include "rdma_worker.h"

// Include full definition of UnifiedRDMAEndpoint
#include "dlslime/logging.h"  // SLIME_LOG_INFO
#include "rdma_endpoint.h"
#include "rdma_utils.h"  // socketId(), bindToSocket()

#include <emmintrin.h>  // for _mm_pause
#include <iterator>     // for std::make_move_iterator

namespace dlslime {

RDMAWorker::RDMAWorker(std::string dev_name, int id): socket_id_(socketId(dev_name)), worker_id_(id) {}

RDMAWorker::RDMAWorker(int32_t socket_id, int id): socket_id_(socket_id), worker_id_(id) {}

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

int64_t RDMAWorker::addEndpoint(std::shared_ptr<RDMAEndpoint> endpoint)
{
    // Generate a new ID
    int64_t new_id = next_endpoint_id_.fetch_add(1, std::memory_order_relaxed);
    endpoint->setId(new_id);

    std::lock_guard<std::mutex> lock(staging_mutex_);
    staging_endpoints_.push_back(std::move(endpoint));

    has_new_endpoints_.store(true, std::memory_order_release);

    return new_id;
}

void RDMAWorker::removeEndpoint(std::shared_ptr<RDMAEndpoint> endpoint)
{
    std::lock_guard<std::mutex> lock(staging_mutex_);
    pending_removals_.push_back(std::move(endpoint));
    has_new_endpoints_.store(true, std::memory_order_release);
}

void RDMAWorker::_merge_new_endpoints()
{
    std::lock_guard<std::mutex> lock(staging_mutex_);

    if (!staging_endpoints_.empty()) {
        endpoints_.insert(endpoints_.end(),
                          std::make_move_iterator(staging_endpoints_.begin()),
                          std::make_move_iterator(staging_endpoints_.end()));

        staging_endpoints_.clear();

        SLIME_LOG_INFO("RDMA Worker ", worker_id_, " added new endpoints. Total: ", endpoints_.size());
    }

    if (!pending_removals_.empty()) {
        for (const auto& rm_ep : pending_removals_) {
            // Remove from endpoints_
            for (auto it = endpoints_.begin(); it != endpoints_.end();) {
                if ((*it).get() == rm_ep.get()) {
                    it = endpoints_.erase(it);
                }
                else {
                    ++it;
                }
            }
        }
        pending_removals_.clear();
    }

    // Reset flag
    has_new_endpoints_.store(false, std::memory_order_release);
}

void RDMAWorker::workerLoop()
{
    if (socket_id_ >= 0) {
        bindToSocket(socket_id_);
    }
    SLIME_LOG_INFO("RDMA Worker Thread ", worker_id_, " started on socket ", socket_id_);

    while (running_.load(std::memory_order_relaxed)) {

        if (has_new_endpoints_.load(std::memory_order_acquire)) {
            _merge_new_endpoints();
        }

        int total_work_done = 0;

        for (auto& ep : endpoints_) {
            total_work_done += ep->process();
        }

        if (total_work_done == 0) {
            _mm_pause();
        }
    }
}

}  // namespace dlslime
