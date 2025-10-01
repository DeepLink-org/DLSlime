#include <cstdint>
#include <sys/types.h>
#include <torch/torch.h>

#include "ATen/ops/empty.h"
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
                                           int64_t     qp_num):
    max_bs_(max_bs),
    msg_size_(msg_size),
    device_(device),
    role_(role),
    m_world_size_(m_world_size),
    n_world_size_(n_world_size),
    rank_(rank),
    num_concurrency_(num_concurrency)
{
    // Step 1: Alloc Buffer
    allocBuffer();

    // Step 2. Init all RDMA Context
    size_t ctx_size = role == 0 ? m_world_size : n_world_size;
    if (role == 0) {
        for (int i = 0; i < ctx_size; ++i) {
            auto rdma_context = std::make_shared<RDMAContext>(qp_num);
            // TODO (JimyMa): Bynow, set link type to "RoCE", automatically in the future.
            auto nics         = available_nic();
            auto selected_nic = nics[rank % nics.size()];
            // TODO (JimyMa): Affinity of nic and comp device
            rdma_context->init(selected_nic, 1, "RoCE");
            rdma_context->register_memory_region("buffer_local_idx_" + std::to_string(rank_) + "_remote_idx_"
                                                     + std::to_string(i),
                                                 reinterpret_cast<uintptr_t>(buffer_.data_ptr()),
                                                 getBufferSize());
            ctx_.emplace_back(rdma_context);
        }
    }
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
    buffer_     = torch::empty({static_cast<int64_t>(getBufferSize())}, options);
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

torch::Tensor& M2NIBVerbsRCLLBuffer::localBuffer() {
    return buffer_;
}

}  // namespace slime
