#pragma once

#include <cstdint>
#include <linux/types.h>
#include <memory>
#include <vector>

#include <torch/torch.h>

#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_endpoint.h"
#include "json.hpp"

namespace slime {

using json = nlohmann::json;

typedef struct __attribute__((aligned(64))) M2NTask {

} m2n_task_t;

class M2NIBVerbsRCLLBuffer {

public:
    M2NIBVerbsRCLLBuffer(int64_t     max_bs,
                         int64_t     msg_size,
                         std::string device,
                         int64_t     role,
                         int64_t     m_world_size,
                         int64_t     n_world_size,
                         int64_t     rank,
                         int64_t     num_concurrency,
                         int64_t     qp_num = 2);

    size_t getBufferSize();

    int allocBuffer();

    json bufferInfo();

    int connectFullMesh(const std::vector<json>& all_buffer_info);

    int M2NRecv(int tag);

    int M2NSend(int tag);

    torch::Tensor& localBuffer();

private:
    torch::Tensor buffer_;

    std::vector<std::shared_ptr<RDMAContext>> ctx_;

    int64_t max_bs_;
    int64_t msg_size_;

    std::string device_;

    int64_t role_;

    int64_t m_world_size_;
    int64_t n_world_size_;

    int64_t rank_;

    int64_t num_concurrency_;

    std::vector<std::queue<std::shared_ptr<m2n_task_t>>> posted_recv_queue_;
};
}  // namespace slime
