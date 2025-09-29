#include <stdexcept>

#include <c10/core/ScalarType.h>
#include <torch/types.h>

#include "logging.h"
#include "ops/nvshmem_api.cuh"

#include "all_gather_inter_ll.h"
#include "all_gather_inter_ll_buffer.h"

namespace slime {

AllGatherInterLLBuffer::AllGatherInterLLBuffer(
    int64_t max_bs, int64_t msg_size, torch::Dtype dtype, int64_t world_size, int64_t rank, int64_t num_concurrency):
    max_bs_(max_bs),
    msg_size_(msg_size),
    dtype_(dtype),
    world_size_(world_size),
    rank_(rank),
    num_concurrency_(num_concurrency)
{
    cudaSetDevice(localRank());
    SLIME_ASSERT((msg_size * itemsize()) % 16 == 0, "By now, msg size must be divided by 16");
}

AllGatherInterLLBuffer::AllGatherInterLLBuffer(int64_t      max_bs,
                                               int64_t      msg_size,
                                               torch::Dtype dtype,
                                               int64_t      world_size,
                                               int64_t      rank,
                                               int64_t      num_concurrency,
                                               bool         allow_nvlink):
    AllGatherInterLLBuffer(max_bs, msg_size, dtype, world_size, rank, num_concurrency)
{
    allow_nvlink_ = allow_nvlink;
}

size_t AllGatherInterLLBuffer::getBufferSize()
{
    size_t buffer_size = static_cast<size_t>(max_bs_) * msg_size_ * itemsize() * world_size_ * num_concurrency_;
    return buffer_size;
}

int64_t AllGatherInterLLBuffer::localRank()
{
    // TODO (JimyMa): By now, Assume local world size is 8
    return rank_ % 8;
}

int64_t AllGatherInterLLBuffer::itemsize()
{
    return torch::elementSize(dtype_);
}

int AllGatherInterLLBuffer::allocSymBuffer()
{
    size_t buffer_size = getBufferSize();

    sym_buffer_          = reinterpret_cast<int8_t*>(nvshmem_api::alloc(buffer_size, nvshmem_alignment));
    SLIME_ASSERT(sym_buffer_ != NULL, "failure of symbuffer allocation!");
    nvshmem_api::barrier();
    sym_signal_ = reinterpret_cast<int*>(nvshmem_api::alloc(world_size_ * sizeof(int), nvshmem_alignment));
    SLIME_ASSERT(sym_signal_ != NULL, "failure of symsignal allocation!");
    nvshmem_api::barrier();
    cudaMemset(sym_buffer_, 0, buffer_size);
    cudaMemset(sym_signal_, 0, world_size_ * sizeof(int));

    return 0;
}

json AllGatherInterLLBuffer::bufferInfo()
{
    json info{};
    info["nvshmem_info"] = {{"unique_id", nvshmem_api::get_unique_id()}};

    return info;
}

int AllGatherInterLLBuffer::connectFullMesh(std::vector<json> all_buffer_info)
{
    auto unique_ids   = all_buffer_info[root_rank]["nvshmem_info"]["unique_id"];
    int  nvshmem_rank = nvshmem_api::init(unique_ids, rank_, world_size_);
    nvshmem_api::barrier();
    allocSymBuffer();
    nvshmem_api::barrier();
    SLIME_ASSERT(nvshmem_rank == rank_, "nvshmem_rank != rank_");
    return 0;
}

std::tuple<torch::Tensor, std::function<void()>> AllGatherInterLLBuffer::allGatherLLHook(torch::Tensor q, int32_t tag)
{

    SLIME_ASSERT(q.dtype() == dtype_, "unmatched data type!");

    auto launcher = [=](int phase) {
        all_gather_inter_ll(
            q, sym_buffer_, sym_signal_, max_bs_, msg_size_, itemsize(), world_size_, rank_, phase, tag, allow_nvlink_);
    };

    launcher(ALL_GATHER_LL_SEND_PHASE);

    auto options = torch::TensorOptions().dtype(dtype_).device(torch::kCUDA);
    auto torch_buffer =
        torch::from_blob(reinterpret_cast<void*>(sym_buffer_ + tag * max_bs_ * msg_size_ * itemsize() * world_size_),
                         {world_size_, max_bs_, msg_size_},
                         options);

    std::function<void()> recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };

    return {torch_buffer, recv_hook};
}

torch::Tensor AllGatherInterLLBuffer::allGatherLL(torch::Tensor q, int32_t tag)
{

    SLIME_ASSERT(q.dtype() == dtype_, "unmatched data type!");
    SLIME_ASSERT(tag < num_concurrency_, "tag (" << tag << ") < num_concurrency (" << num_concurrency_ << ") failed");

    all_gather_inter_ll(q,
                        sym_buffer_,
                        sym_signal_,
                        max_bs_,
                        msg_size_,
                        itemsize(),
                        world_size_,
                        rank_,
                        ALL_GATHER_LL_SEND_PHASE | ALL_GATHER_LL_RECV_PHASE,
                        tag,
                        allow_nvlink_);

    auto options = torch::TensorOptions().dtype(dtype_).device(torch::kCUDA);

    return torch::from_blob(reinterpret_cast<void*>(sym_buffer_ + tag * max_bs_ * msg_size_ * itemsize() * world_size_),
                            {world_size_, max_bs_, msg_size_},
                            options);
}

}  // namespace slime
