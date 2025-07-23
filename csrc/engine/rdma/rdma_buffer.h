#pragma once
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

namespace slime {

class RDMAEndpoint;

class RDMABuffer {

    //

public:
    RDMABuffer(std::shared_ptr<RDMAEndpoint> end_point,
               std::vector<uintptr_t>        ptrs,
               std::vector<size_t>           data_size,
               size_t                        batch_size)
    {

        for (uint32_t i = 0; i < batch_size; ++i) {
            data_ptrs_.push_back(ptrs[i]);
            data_size_.push_back(data_size[i]);
        }

        batch_size_ = batch_size;
        end_point_  = end_point;
    }

    ~RDMABuffer() = default;

    void send();

    void recv();

    void waitSend();

    void waitRecv();

private:
    std::shared_ptr<RDMAEndpoint> end_point_;

    std::vector<uintptr_t> data_ptrs_;
    std::vector<size_t>    data_size_;
    uint32_t               batch_size_;

    bool send_pending_{false};
    bool recv_pending_{false};

    bool send_completed_{false};
    bool recv_completed_{false};

    std::condition_variable send_cv_;
    std::condition_variable recv_cv_;

    std::mutex send_mutex_;
    std::mutex recv_mutex_;
};

}  // namespace slime
