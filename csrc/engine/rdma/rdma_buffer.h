#pragma once
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_endpoint_v0.h"

#include <condition_variable>
#include <cstdint>
#include <infiniband/verbs.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "logging.h"
#include "rdma_common.h"

namespace slime {

class RDMAEndpoint;

class RDMABuffer: public std::enable_shared_from_this<RDMABuffer> {
    friend class RDMAEndpoint;
    friend class RDMAEndpointV0;

public:
    RDMABuffer(std::shared_ptr<RDMAEndpoint> endpoint, uintptr_t ptr, size_t offset, size_t data_size):
        endpoint_(endpoint),
        ptr_(ptr + offset),
        offset_(0),
        data_size_(data_size),
        view_(storage_view_t{ptr + offset, 0, data_size})
    {
    }

    RDMABuffer(std::shared_ptr<RDMAEndpointV0> endpoint, uintptr_t ptr, size_t offset, size_t data_size):
        endpointv0_(endpoint),
        ptr_(ptr + offset),
        offset_(0),
        data_size_(data_size),
        view_(storage_view_t{ptr + offset, 0, data_size})
    {
    }

    RDMABuffer(std::shared_ptr<RDMAEndpoint> endpoint, storage_view_batch_t& batch):
        endpoint_(endpoint), storage_view_batch_(std::move(batch))
    {
    }

    ~RDMABuffer() = default;

    const size_t batch_size()
    {
        return storage_view_batch_.size();
    }

    const storage_view_batch_t& view_batch()
    {
        return storage_view_batch_;
    }

    void send();
    void recv();

    bool waitSend();
    bool waitRecv();

    void sendDoneCallback();
    void recvDoneCallback();

private:
    std::shared_ptr<RDMAEndpoint> endpoint_;
    std::shared_ptr<RDMAEndpointV0> endpointv0_;

    uintptr_t ptr_;
    size_t    offset_;
    size_t    data_size_;

    storage_view_t view_;

    std::vector<uintptr_t> ptrs_batch_;
    std::vector<size_t>    offset_batch_;
    std::vector<size_t>    data_size_batch_;

    storage_view_batch_t storage_view_batch_;

    std::atomic<int> send_pending_{0};
    std::atomic<int> recv_pending_{0};

    std::atomic<int> send_completed_{0};
    std::atomic<int> recv_completed_{0};

    std::condition_variable send_cv_;
    std::condition_variable recv_cv_;

    std::mutex send_mutex_;
    std::mutex recv_mutex_;

    std::atomic<int32_t> slot_id_{0};
};

}  // namespace slime
