#include <torch/torch.h>

#include "c10/core/ScalarType.h"
#include "logging.h"

#include "all_to_all_intra_ll_buffer.h"

namespace slime {

AllToAllIntraLLBuffer::AllToAllIntraLLBuffer(
    int32_t max_dispatch_per_msg, int32_t max_bs, int32_t rank, int32_t world_size, int64_t local_buffer_size):
    max_dispatch_per_msg_(max_dispatch_per_msg),
    max_bs_(max_bs),
    rank_(rank),
    world_size_(world_size),
    local_buffer_size_(local_buffer_size)
{
    SLIME_LOG_INFO("Initialize AllToAll Intra LL Buffer. "
                   "local_buffer_size="
                   << local_buffer_size
                   << " bytes, "
                      "max_bs="
                   << max_bs
                   << ", "
                      "rank="
                   << rank
                   << ", "
                      "world_size="
                   << world_size << ".");
    allocBuffer(local_buffer_size);
}

int64_t AllToAllIntraLLBuffer::get_buffer_size_hint(int32_t max_bs,
                                                    int32_t max_dispatch_per_msg,
                                                    int32_t max_msg_size,
                                                    int32_t itemsize)
{
    return max_bs * max_msg_size * itemsize * max_dispatch_per_msg;
}

int AllToAllIntraLLBuffer::allocBuffer(std::optional<int64_t> local_buffer_size_opt)
{
    CUDA_CHECK(cudaMalloc(&buffer_ptrs_, sizeof(int8_t*) * world_size_));
    CUDA_CHECK(cudaMalloc(&signal_ptrs_, sizeof(int*) * world_size_));

    SLIME_ASSERT(local_buffer_size_opt.has_value(), "null default value is forbidden");
    int64_t buffer_size = local_buffer_size_opt.value();

    CUDA_CHECK(cudaMalloc(&local_buffer_, buffer_size));
    CUDA_CHECK(cudaMemset(local_buffer_, 0, buffer_size));
    cudaIpcGetMemHandle(&local_buffer_ipc_handle_, local_buffer_);

    int32_t signal_size = world_size_ * sizeof(int);
    CUDA_CHECK(cudaMalloc(&local_signal_, signal_size));
    CUDA_CHECK(cudaMemset(local_signal_, 0, signal_size));
    cudaIpcGetMemHandle(&local_signal_ipc_handle_, local_signal_);

    return 0;
}

json AllToAllIntraLLBuffer::buffer_info()
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

int AllToAllIntraLLBuffer::connectFullMesh(std::vector<json> all_buffer_info)
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

torch::Tensor AllToAllIntraLLBuffer::allToAllLL2D(torch::Tensor x, bool is_transpose, c10::optional<torch::Tensor> mask)
{

    auto shape = x.sizes();
    SLIME_ASSERT(shape.size() == 2, "Input must be a 2D tensor");
    auto msg_size = shape[1];
    SLIME_ASSERT(msg_size * x.itemsize() % 16 == 0, "msg_size must be divided by 16");

    all_to_all_intra_ll(
        x, buffer_ptrs_, signal_ptrs_, max_dispatch_per_msg_, max_bs_, rank_, world_size_, is_transpose, mask);

    // assuming device is already set
    auto options = torch::TensorOptions().dtype(x.dtype()).device(torch::kCUDA);

    SLIME_ASSERT(world_size_ * max_bs_ * shape[1] * x.itemsize() <= local_buffer_size_,
                 "local_buffer_size_ (" << local_buffer_size_ << "), demand components: "
                                        << "world_size_=" << world_size_ << ", "
                                        << "max_bs_=" << max_bs_ << ", "
                                        << "shape[1]=" << shape[1] << ", "
                                        << "x.itemsize()=" << x.itemsize() << ", "
                                        << "total demand=" << (world_size_ * max_bs_ * shape[1] * x.itemsize()) << ".");

    return torch::from_blob(reinterpret_cast<void*>(local_buffer_), {world_size_, max_bs_, msg_size}, options);
}

torch::Tensor AllToAllIntraLLBuffer::getLocalBuffer(int32_t max_bs,
                                                    int32_t max_dispatch_per_msg,
                                                    int32_t max_msg_size,
                                                    int32_t itemsize,
                                                    c10::ScalarType dtype)
{
    auto options = torch::TensorOptions().dtype(dtype).device(torch::kCUDA);

    return torch::from_blob(reinterpret_cast<void*>(local_buffer_), {get_buffer_size_hint(max_bs, max_dispatch_per_msg, max_msg_size, itemsize)}, options);
}

}  // namespace slime
