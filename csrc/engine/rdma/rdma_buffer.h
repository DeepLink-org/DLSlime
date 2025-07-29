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

class RDMABuffer : public std::enable_shared_from_this<RDMABuffer> {

public:
    RDMABuffer(std::shared_ptr<RDMAEndpoint> end_point,
               std::vector<uintptr_t>        ptrs,
               std::vector<size_t>           offset,
               std::vector<size_t>           data_size)
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

    bool waitSend();

    bool waitRecv();

private:
    std::shared_ptr<RDMAEndpoint> end_point_;

    // <tensor_ptrs_, tensor_size_, offset>
    // tensor_ptrs: the pointer of the tensor
    // tensor_size: the length of the tensor
    // offset: the offset of the transmitted tensor
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
