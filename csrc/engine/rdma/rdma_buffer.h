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

public:
    RDMABuffer(std::shared_ptr<RDMAEndpoint> end_point,
               std::vector<uintptr_t>        ptrs,
               std::vector<size_t>           data_size,
               std::vector<size_t>           offset)
    {
        batch_size_ = ptrs.size();
        for (uint32_t i = 0; i < batch_size_; ++i) {
            data_info.push_back(std::make_tuple(ptrs[i], data_size[i], offset[i]));
        }
        end_point_ = end_point;
    }

    ~RDMABuffer() = default;

    void send();

    void recv();

    void waitSend();

    void waitRecv();

private:
    std::shared_ptr<RDMAEndpoint> end_point_;

    // <data_ptrs_, data_size_, offset>
    // data_ptrs: the pointer of the data
    // data_size: the length of the data
    // offset: you know
    std::vector<std::tuple<uintptr_t, size_t, size_t>> data_info;

    size_t batch_size_;

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
