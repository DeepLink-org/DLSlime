#include <stdexcept>

#include "logging.h"
#include "ops/nvshmem_api.cuh"

#include "all_gather_inter_ll.h"
#include "all_gather_inter_ll_buffer.h"

namespace slime {

AllGatherInterLLBuffer::AllGatherInterLLBuffer(
    int32_t max_bs, int32_t msg_size, int32_t itemsize, int32_t world_size, int32_t rank):
    max_bs_(max_bs), msg_size_(msg_size), itemsize_(itemsize), world_size_(world_size), rank_(rank)
{
    SLIME_ASSERT((msg_size * itemsize) % 16 == 0, "By now, msg size must be divided by 16");
}

int32_t AllGatherInterLLBuffer::get_buffer_size()
{
    return max_bs_ * msg_size_ * itemsize_ * world_size_;
}

int AllGatherInterLLBuffer::allocBuffer()
{
    int32_t buffer_size = get_buffer_size();

    sym_buffer_ = reinterpret_cast<int8_t*>(nvshmem_api::alloc(buffer_size, nvshmem_alignment));
    sym_signal_ = reinterpret_cast<int*>(nvshmem_api::alloc(world_size_ * sizeof(int), nvshmem_alignment));

    return 0;
}

json AllGatherInterLLBuffer::buffer_info()
{
    json info{};
    info["nvshmem_info"] = {{"unique_id", nvshmem_api::get_unique_id()}};

    return info;
}

int AllGatherInterLLBuffer::connectFullMesh(std::vector<json> all_buffer_info)
{
    auto unique_ids   = all_buffer_info[root_rank]["nvshmem_info"]["unique_id"];
    int  nvshmem_rank = nvshmem_api::init(unique_ids, rank_, world_size_);
    SLIME_ASSERT(nvshmem_rank == rank_, "nvshmem_rank != rank_");
    return 0;
}

torch::Tensor AllGatherInterLLBuffer::allGatherLL(torch::Tensor q)
{
    all_gather_inter_ll(q, sym_buffer_, sym_signal_, max_bs_, msg_size_, itemsize_, world_size_, rank_);
    return torch::from_blob(reinterpret_cast<void*>(sym_buffer_), {world_size_, max_bs_, msg_size_}, q.options());
}

}  // namespace slime
