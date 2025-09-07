#pragma once
#include "engine/metrics.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_endpoint.h"

#include <condition_variable>
#include <infiniband/verbs.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "rdma_common.h"

namespace slime {

class RDMAEndpoint;

class RDMABuffer: public std::enable_shared_from_this<RDMABuffer> {
    friend class RDMAEndpoint;

public:
    RDMABuffer(std::shared_ptr<RDMAEndpoint> endpoint, storage_view_batch_t& batch):
        endpoint_(endpoint), storage_view_batch_(std::move(batch))
    {
        
    }

    RDMABuffer(std::shared_ptr<RDMAEndpoint> endpoint,
               std::vector<uintptr_t>        ptrs,
               std::vector<size_t>           offset,
               std::vector<size_t>           data_size)
    {
        metrics_      = std::make_shared<rdma_metrics_t>();
        meta_metrics_ = std::make_shared<rdma_metrics_t>();
        data_metrics_ = std::make_shared<rdma_metrics_t>();
        batch_size_   = ptrs.size();
        ptrs_         = ptrs;
        offset_       = offset;
        data_size_    = data_size;
        for (uint32_t i = 0; i < batch_size_; ++i) {
            storage_view_t view{.data_ptr = ptrs[i], .storage_offset = offset[i], .length = data_size[i]};
            storage_view_batch_.push_back(view);
        }
        endpoint_             = endpoint;
        metrics_->buffer_init = std::chrono::steady_clock::now();
    }

    ~RDMABuffer() = default;

    const size_t batchSize()
    {
        return storage_view_batch_.size();
    }

    const storage_view_batch_t& storageViewBatch()
    {
        return storage_view_batch_;
    }

    void send();
    void recv();

    bool waitSend();
    bool waitRecv();

    void send_done_callback();
    void recv_done_callback();

    void get_time();

private:

    storage_view_batch_t storage_view_batch_;

    std::shared_ptr<RDMAEndpoint> endpoint_;
    std::vector<uintptr_t>        ptrs_;
    std::vector<size_t>           offset_;
    std::vector<size_t>           data_size_;

    size_t batch_size_;

    std::atomic<int> send_pending_{0};
    std::atomic<int> recv_pending_{0};

    std::atomic<int> send_completed_{0};
    std::atomic<int> recv_completed_{0};

    std::condition_variable send_cv_;
    std::condition_variable recv_cv_;

    std::mutex send_mutex_;
    std::mutex recv_mutex_;

    std::shared_ptr<rdma_metrics_t> metrics_;
    std::shared_ptr<rdma_metrics_t> meta_metrics_;
    std::shared_ptr<rdma_metrics_t> data_metrics_;
};

}  // namespace slime
