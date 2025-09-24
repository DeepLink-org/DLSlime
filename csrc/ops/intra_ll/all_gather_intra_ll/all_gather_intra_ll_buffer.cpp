#include <torch/torch.h>

#include "logging.h"

#include "all_gather_intra_ll_buffer.h"

namespace slime {

AllGatherIntraLLBuffer::AllGatherIntraLLBuffer(
    int32_t max_bs, int32_t msg_size, int32_t itemsize, int32_t world_size, int32_t rank):
    max_bs_(max_bs), msg_size_(msg_size), itemsize_(itemsize), world_size_(world_size), rank_(rank)
{

    SLIME_ASSERT((msg_size * itemsize) % 16 == 0, "By now, msg size must be divided by 16");

    allocBuffer();
}

int32_t AllGatherIntraLLBuffer::get_buffer_size()
{
    return max_bs_ * msg_size_ * itemsize_ * world_size_;
}

int AllGatherIntraLLBuffer::allocBuffer()
{
    CUDA_CHECK(cudaMalloc(&buffer_ptrs_, sizeof(int8_t*) * world_size_));
    CUDA_CHECK(cudaMalloc(&signal_ptrs_, sizeof(int*) * world_size_));

    int32_t buffer_size = get_buffer_size();
    CUDA_CHECK(cudaMalloc(&local_buffer_, buffer_size));
    CUDA_CHECK(cudaMemset(local_buffer_, 0, buffer_size));
    cudaIpcGetMemHandle(&local_buffer_ipc_handle_, local_buffer_);

    int32_t signal_size = world_size_ * sizeof(int);
    CUDA_CHECK(cudaMalloc(&local_signal_, signal_size));
    CUDA_CHECK(cudaMemset(local_signal_, 0, signal_size));
    cudaIpcGetMemHandle(&local_signal_ipc_handle_, local_signal_);

    return 0;
}

json AllGatherIntraLLBuffer::buffer_info()
{
    json info = json{};

    info["buffer_ipc_handle"] = std::vector<char>{};
    for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; i++)
        info["buffer_ipc_handle"][i] = local_buffer_ipc_handle_.reserved[i];

    info["signal_ipc_handle"] = std::vector<char>{};
    for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; i++)
        info["signal_ipc_handle"][i] = local_signal_ipc_handle_.reserved[i];

    return info;
}

int AllGatherIntraLLBuffer::connectFullMesh(std::vector<json> all_buffer_info)
{
    int8_t** buffer_ptrs_cpu = (int8_t**)malloc(sizeof(int8_t*) * world_size_);
    for (int gpu_idx = 0; gpu_idx < all_buffer_info.size(); ++gpu_idx) {
        if (gpu_idx == rank_) {
            buffer_ptrs_cpu[gpu_idx] = local_buffer_;
        }
        else {
            cudaIpcMemHandle_t ipc_handle;
            for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; ++i)
                ipc_handle.reserved[i] = all_buffer_info[gpu_idx]["buffer_ipc_handle"][i].get<char>();
            cudaIpcOpenMemHandle((void**)&(buffer_ptrs_cpu[gpu_idx]), ipc_handle, cudaIpcMemLazyEnablePeerAccess);
        }
    }

    CUDA_CHECK(cudaMemcpy(buffer_ptrs_, buffer_ptrs_cpu, sizeof(int8_t*) * world_size_, cudaMemcpyHostToDevice));

    int** signal_ptrs_cpu = (int**)malloc(sizeof(int*) * world_size_);
    for (int gpu_idx = 0; gpu_idx < all_buffer_info.size(); ++gpu_idx) {
        if (gpu_idx == rank_) {
            signal_ptrs_cpu[gpu_idx] = local_signal_;
        }
        else {
            cudaIpcMemHandle_t ipc_handle;
            for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; ++i)
                ipc_handle.reserved[i] = all_buffer_info[gpu_idx]["signal_ipc_handle"][i].get<char>();
            cudaIpcOpenMemHandle((void**)&(signal_ptrs_cpu[gpu_idx]), ipc_handle, cudaIpcMemLazyEnablePeerAccess);
        }
    }
    CUDA_CHECK(cudaMemcpy(signal_ptrs_, signal_ptrs_cpu, sizeof(int*) * world_size_, cudaMemcpyHostToDevice));

    return 0;
}

torch::Tensor AllGatherIntraLLBuffer::allGatherLL(torch::Tensor q)
{
    all_gather_intra_ll(q, buffer_ptrs_, signal_ptrs_, max_bs_, msg_size_, itemsize_, world_size_, rank_);
    return torch::from_blob(reinterpret_cast<void*>(local_buffer_), {world_size_, max_bs_, msg_size_}, q.options());
}

}  // namespace slime
