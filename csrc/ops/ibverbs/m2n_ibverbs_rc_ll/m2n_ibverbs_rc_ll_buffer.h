#pragma once

#include <cstdint>
#include <linux/types.h>
#include <memory>
#include <vector>

#include <torch/torch.h>

#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_endpoint.h"
#include "jring.h"
#include "json.hpp"

namespace slime {

using json = nlohmann::json;

typedef struct __attribute__((aligned(64))) M2NRecvTask {
    std::shared_ptr<RDMASchedulerAssignment> assign;
} m2n_recv_task_t;

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
                         int64_t     qp_num    = 2,
                         std::string link_type = "RoCE");

    ~M2NIBVerbsRCLLBuffer();

    size_t getBufferSize();

    int allocBuffer();

    json bufferInfo();

    int connectFullMesh(const std::vector<json>& all_buffer_info);

    std::shared_ptr<RDMASchedulerAssignment> M2NRecv(int tag);

    std::shared_ptr<RDMASchedulerAssignment> M2NSend(int tag);

private:
    int recvQueuePut();

    m2n_recv_task_t recvQueueGet();

    inline int64_t peerWorldSize();

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

    std::string link_type_;

    jring_t* posted_recv_queue_;
};

}  // namespace slime
