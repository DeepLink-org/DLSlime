#include <cstdint>
#include <cstdlib>
#include <sys/types.h>
#include <torch/torch.h>

#include "ATen/ops/empty.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "jring.h"
#include "json.hpp"
#include "logging.h"

#include "engine/rdma/rdma_context.h"
#include "engine/rdma/utils.h"

#include "m2n_ibverbs_rc_ll_buffer.h"
#include "torch/types.h"

namespace slime {

M2NIBVerbsRCLLBuffer::M2NIBVerbsRCLLBuffer(int64_t     max_bs,
                                           int64_t     msg_size,
                                           std::string device,
                                           int64_t     role,
                                           int64_t     m_world_size,
                                           int64_t     n_world_size,
                                           int64_t     rank,
                                           int64_t     num_concurrency,
                                           int64_t     qp_num,
                                           std::string link_type):
    max_bs_(max_bs),
    msg_size_(msg_size),
    device_(device),
    role_(role),
    m_world_size_(m_world_size),
    n_world_size_(n_world_size),
    rank_(rank),
    num_concurrency_(num_concurrency),
    link_type_(link_type)
{

    // Step 1: Alloc Buffer
    allocBuffer();

    // Step 2. Init all RDMA Context
    size_t ctx_size = role == 0 ? m_world_size : n_world_size;
    for (int i = 0; i < ctx_size; ++i) {
        auto rdma_context = std::make_shared<RDMAContext>(qp_num);
        // TODO (JimyMa): Bynow, set link type to "RoCE", automatically in the future.
        auto nics         = available_nic();
        auto selected_nic = nics[rank % nics.size()];
        // TODO (JimyMa): Affinity of nic and comp device
        rdma_context->init(selected_nic, 1, "RoCE");
        rdma_context->register_memory_region(
            "buffer", reinterpret_cast<uintptr_t>(buffer_.data_ptr()), getBufferSize());
        ctx_.emplace_back(rdma_context);
    }

    // Step 3: Init Recv Queue
    jring_t* ring = reinterpret_cast<jring_t*>(aligned_alloc(sizeof(m2n_recv_task_t), num_concurrency_ + 16));
    jring_init(ring, 16, sizeof(m2n_recv_task_t), 0, 0);
    posted_recv_queue_ = ring;

    // Step 4: Prepost Some recv
    for (int i = 0; i < num_concurrency_; ++i)
        recvQueuePut();
}

inline int64_t M2NIBVerbsRCLLBuffer::peerWorldSize()
{
    if (role_ == 0)
        return n_world_size_;
    else
        return m_world_size_;
}

int M2NIBVerbsRCLLBuffer::recvQueuePut()
{
    // prepost a recv
    RDMAAssignmentSharedPtrBatch assign_batch;

    for (int i = 0; i < peerWorldSize(); ++i) {
        auto batch = AssignmentBatch{};
        // assign_batch.push_back(ctx_[i]->submit(OpCode::RECV, batch));
    }

    auto rdma_sch_assign = std::make_shared<RDMASchedulerAssignment>(assign_batch);

    auto task = m2n_recv_task_t{.assign = rdma_sch_assign};
    while (jring_mp_enqueue_bulk(posted_recv_queue_, &task, 1, nullptr) != 1) {}
    return 0;
}

m2n_recv_task_t M2NIBVerbsRCLLBuffer::recvQueueGet()
{
    m2n_recv_task_t task;
    jring_sc_dequeue_bulk(posted_recv_queue_, &task, 1, nullptr);
    return task;
}

M2NIBVerbsRCLLBuffer::~M2NIBVerbsRCLLBuffer()
{
    free(posted_recv_queue_);
}

size_t M2NIBVerbsRCLLBuffer::getBufferSize()
{
    size_t send_buffer_size = max_bs_ * msg_size_ * m_world_size_;
    size_t recv_buffer_size = max_bs_ * msg_size_ * n_world_size_;
    size_t send_signal_size = n_world_size_;
    size_t recv_signal_size = m_world_size_;
    return std::max(send_buffer_size, recv_buffer_size) + std::max(send_signal_size, recv_signal_size);
}

int M2NIBVerbsRCLLBuffer::allocBuffer()
{
    auto options = torch::TensorOptions().dtype(torch::kInt8).device(device_);
    buffer_      = torch::empty({static_cast<int64_t>(getBufferSize())}, options);
    return 0;
}

json M2NIBVerbsRCLLBuffer::bufferInfo()
{
    json              info{};
    std::vector<json> endpoint_info;

    for (int i = 0; i < ctx_.size(); ++i) {
        endpoint_info.emplace_back(ctx_[i]->endpoint_info());
    }

    info["endpoint_info"] = endpoint_info;

    return info;
}

int M2NIBVerbsRCLLBuffer::connectFullMesh(const std::vector<json>& all_buffer_info)
{
    for (int i = 0; i < ctx_.size(); ++i) {
        ctx_[i]->connect(all_buffer_info[i]["endpoint_info"][rank_]);
    }

    return 0;
}

std::shared_ptr<RDMASchedulerAssignment> M2NIBVerbsRCLLBuffer::M2NRecv(int tag)
{
    m2n_recv_task_t task = recvQueueGet();
    return task.assign;
}

std::shared_ptr<RDMASchedulerAssignment> M2NIBVerbsRCLLBuffer::M2NSend(int tag)
{

    RDMAAssignmentSharedPtrBatch assign_batch;

    for (int i = 0; i < ctx_.size(); ++i) {
        AssignmentBatch batch = {Assignment("buffer", 0, 0, msg_size_ * max_bs_)};
        ctx_[i]->submit(OpCode::WRITE, batch, nullptr, -1, tag);
    }

    auto rdma_sch_assign = std::make_shared<RDMASchedulerAssignment>(assign_batch);

    return rdma_sch_assign;
}

}  // namespace slime
