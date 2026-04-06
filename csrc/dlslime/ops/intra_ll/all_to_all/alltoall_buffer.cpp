#include "alltoall_buffer.h"

#include <cstring>
#include <stdexcept>

#include <torch/extension.h>

#define CHECK_CUDA(x) TORCH_CHECK((x).is_cuda(), #x " must be a CUDA tensor")
#define CHECK_CONTIGUOUS(x) TORCH_CHECK((x).is_contiguous(), #x " must be contiguous")
#define CUDA_SAFE_CALL(x)                                                            \
    do {                                                                             \
        cudaError_t err = (x);                                                       \
        if (err != cudaSuccess) {                                                    \
            throw std::runtime_error(std::string("CUDA Error: ") + cudaGetErrorString(err)); \
        }                                                                            \
    } while (0)

namespace dlslime {

void intranode_alltoall(torch::Tensor                x,
                        void**                       buffer_ptr,
                        void**                       signal_ptr,
                        int                          batch_size,
                        int                          max_batch_size,
                        int                          n_heads,
                        int                          hidden_dim,
                        int                          rank,
                        int                          world_size,
                        int*                         device_semaphore_ptr,
                        bool                         is_transpose,
                        c10::optional<torch::Tensor> mask    = c10::nullopt,
                        c10::optional<torch::Tensor> offsets = c10::nullopt);

AllToAllBuffer::AllToAllBuffer(
    int32_t rank, int32_t world_size, int32_t max_batch_size, int64_t buffer_size_bytes):
    rank_(rank), world_size_(world_size), max_batch_size_(max_batch_size), buffer_size_bytes_(buffer_size_bytes)
{
    allocate();
}

AllToAllBuffer::~AllToAllBuffer()
{
    cudaDeviceSynchronize();
    free_resources();
}

void AllToAllBuffer::free_resources()
{
    for (void* ptr : ipc_opened_ptrs_cpu_) {
        if (ptr) {
            cudaIpcCloseMemHandle(ptr);
        }
    }
    ipc_opened_ptrs_cpu_.clear();

    if (buffer_ptrs_gpu_)
        cudaFree(buffer_ptrs_gpu_);
    if (signal_ptrs_gpu_)
        cudaFree(signal_ptrs_gpu_);
    if (local_buffer_)
        cudaFree(local_buffer_);
    if (local_signal_)
        cudaFree(local_signal_);
    if (device_semaphore_)
        cudaFree(device_semaphore_);

    buffer_ptrs_gpu_    = nullptr;
    signal_ptrs_gpu_    = nullptr;
    local_buffer_       = nullptr;
    local_signal_       = nullptr;
    device_semaphore_   = nullptr;
    is_allocated_       = false;
}

void AllToAllBuffer::allocate()
{
    if (is_allocated_) {
        return;
    }

    CUDA_SAFE_CALL(cudaMalloc(&local_buffer_, buffer_size_bytes_));
    CUDA_SAFE_CALL(cudaMemset(local_buffer_, 0, buffer_size_bytes_));
    CUDA_SAFE_CALL(cudaIpcGetMemHandle(&local_buffer_handle_, local_buffer_));

    size_t signal_size = world_size_ * sizeof(int);
    CUDA_SAFE_CALL(cudaMalloc(&local_signal_, signal_size));
    CUDA_SAFE_CALL(cudaMemset(local_signal_, 0, signal_size));
    CUDA_SAFE_CALL(cudaIpcGetMemHandle(&local_signal_handle_, local_signal_));

    CUDA_SAFE_CALL(cudaMalloc(&device_semaphore_, signal_size));
    CUDA_SAFE_CALL(cudaMemset(device_semaphore_, 0, signal_size));

    CUDA_SAFE_CALL(cudaMalloc(&buffer_ptrs_gpu_, world_size_ * sizeof(int8_t*)));
    CUDA_SAFE_CALL(cudaMalloc(&signal_ptrs_gpu_, world_size_ * sizeof(int*)));

    is_allocated_ = true;
}

json AllToAllBuffer::get_ipc_handle_info()
{
    json info;
    std::vector<char> buffer_handle(sizeof(local_buffer_handle_), 0);
    std::memcpy(buffer_handle.data(), &local_buffer_handle_, sizeof(local_buffer_handle_));

    std::vector<char> signal_handle(sizeof(local_signal_handle_), 0);
    std::memcpy(signal_handle.data(), &local_signal_handle_, sizeof(local_signal_handle_));

    info["buffer_handle"] = buffer_handle;
    info["signal_handle"] = signal_handle;
    return info;
}

void AllToAllBuffer::connect_full_mesh(const std::vector<json>& all_handles)
{
    TORCH_CHECK(all_handles.size() == world_size_, "Handle count must match world size");

    std::vector<int8_t*> temp_buffers(world_size_);
    std::vector<int*>    temp_signals(world_size_);

    for (int i = 0; i < world_size_; ++i) {
        if (i == rank_) {
            temp_buffers[i] = local_buffer_;
            temp_signals[i] = local_signal_;
            continue;
        }

        auto buffer_handle = all_handles[i]["buffer_handle"].get<std::vector<char>>();
        auto signal_handle = all_handles[i]["signal_handle"].get<std::vector<char>>();

        cudaIpcMemHandle_t buffer_ipc_handle;
        cudaIpcMemHandle_t signal_ipc_handle;
        std::memcpy(&buffer_ipc_handle, buffer_handle.data(), sizeof(buffer_ipc_handle));
        std::memcpy(&signal_ipc_handle, signal_handle.data(), sizeof(signal_ipc_handle));

        void* open_buffer = nullptr;
        void* open_signal = nullptr;
        CUDA_SAFE_CALL(cudaIpcOpenMemHandle(&open_buffer, buffer_ipc_handle, cudaIpcMemLazyEnablePeerAccess));
        CUDA_SAFE_CALL(cudaIpcOpenMemHandle(&open_signal, signal_ipc_handle, cudaIpcMemLazyEnablePeerAccess));

        temp_buffers[i] = static_cast<int8_t*>(open_buffer);
        temp_signals[i] = static_cast<int*>(open_signal);
        ipc_opened_ptrs_cpu_.push_back(open_buffer);
        ipc_opened_ptrs_cpu_.push_back(open_signal);
    }

    CUDA_SAFE_CALL(cudaMemcpy(
        buffer_ptrs_gpu_, temp_buffers.data(), world_size_ * sizeof(int8_t*), cudaMemcpyHostToDevice));
    CUDA_SAFE_CALL(
        cudaMemcpy(signal_ptrs_gpu_, temp_signals.data(), world_size_ * sizeof(int*), cudaMemcpyHostToDevice));
}

void AllToAllBuffer::reset_semaphore()
{
    CUDA_SAFE_CALL(cudaMemset(device_semaphore_, 0, world_size_ * sizeof(int)));
}

torch::Tensor AllToAllBuffer::all_to_all(
    torch::Tensor                x,
    KernelImpl                  impl,
    bool                        is_transpose,
    c10::optional<torch::Tensor> mask,
    c10::optional<torch::Tensor> offsets)
{
    switch (impl) {
        case KernelImpl::Basic:
            return dispatch_basic(x, is_transpose, mask, offsets);
        case KernelImpl::TMA:
            TORCH_CHECK(false, "KernelImpl::TMA is not enabled in this DLSlime build");
        default:
            TORCH_CHECK(false, "Unknown KernelImpl");
    }
}

torch::Tensor AllToAllBuffer::dispatch_basic(torch::Tensor                x,
                                             bool                         is_transpose,
                                             c10::optional<torch::Tensor> mask,
                                             c10::optional<torch::Tensor> offsets)
{
    CHECK_CUDA(x);
    CHECK_CONTIGUOUS(x);
    TORCH_CHECK(x.dim() == 2, "AllToAllBuffer expects a 2D tensor");

    auto shape      = x.sizes();
    int  total_rows = shape[0];
    int  msg_size   = shape[1];

    const bool use_offsets = offsets.has_value();
    int        batch_size  = 0;

    if (use_offsets) {
        auto offsets_tensor = offsets.value();
        CHECK_CUDA(offsets_tensor);
        CHECK_CONTIGUOUS(offsets_tensor);
        TORCH_CHECK(offsets_tensor.device() == x.device(), "Offsets must be on the same device as x");
        TORCH_CHECK(offsets_tensor.scalar_type() == torch::kInt32, "Offsets must be int32");
        TORCH_CHECK(offsets_tensor.dim() == 1, "Offsets shape must be [world_size + 1]");
        TORCH_CHECK(offsets_tensor.numel() == world_size_ + 1,
                    "Offsets shape must be [world_size + 1], got numel=",
                    offsets_tensor.numel(),
                    " world_size=",
                    world_size_);
        TORCH_CHECK(!is_transpose, "AllToAllBuffer offsets only support non-transpose all-to-all");
        batch_size = total_rows;
    }

    if (mask.has_value()) {
        CHECK_CUDA(mask.value());
        CHECK_CONTIGUOUS(mask.value());
        TORCH_CHECK(mask.value().device() == x.device(), "Mask must be on the same device as x");
        TORCH_CHECK(mask.value().dim() == 2, "Mask shape must be [world_size, batch_size]");
        TORCH_CHECK(mask.value().size(0) == world_size_, "Mask shape must be [world_size, batch_size]");
        TORCH_CHECK(mask.value().scalar_type() == torch::kInt32, "Mask must be int32");
        if (use_offsets) {
            TORCH_CHECK(mask.value().size(1) == max_batch_size_,
                        "Offsets path expects mask shape [world_size, max_batch_size], got batch_size=",
                        mask.value().size(1),
                        " max_batch_size=",
                        max_batch_size_);
            TORCH_CHECK(total_rows <= max_batch_size_,
                        "Offsets input rows (",
                        total_rows,
                        ") must be <= max_batch_size (",
                        max_batch_size_,
                        ")");
        }
        else {
            batch_size = mask.value().size(1);
        }

        if (is_transpose) {
            TORCH_CHECK(
                total_rows == world_size_ * batch_size,
                "Transpose masked input shape must be [world_size * batch_size, msg], got rows=",
                total_rows,
                " world_size=",
                world_size_,
                " batch_size=",
                batch_size);
        }
        else {
            TORCH_CHECK(
                total_rows == batch_size,
                "Non-transpose masked input shape must be [batch_size, msg], got rows=",
                total_rows,
                " batch_size=",
                batch_size);
        }
    }
    else {
        if (use_offsets) {
            TORCH_CHECK(total_rows <= max_batch_size_,
                        "Offsets input rows (",
                        total_rows,
                        ") must be <= max_batch_size (",
                        max_batch_size_,
                        ")");
        }
        else {
            TORCH_CHECK(
                total_rows % world_size_ == 0,
                "Input rows (",
                total_rows,
                ") must be divisible by world_size (",
                world_size_,
                ")");
            batch_size = total_rows / world_size_;
        }
    }

    TORCH_CHECK(
        batch_size <= max_batch_size_,
        "Batch size (",
        batch_size,
        ") exceeds max_batch_size (",
        max_batch_size_,
        ")");
    TORCH_CHECK((msg_size * x.itemsize()) % 16 == 0, "Message size must be 16-byte aligned");
    TORCH_CHECK(
        static_cast<int64_t>(world_size_) * max_batch_size_ * msg_size * x.itemsize() <= buffer_size_bytes_,
        "AllToAllBuffer capacity (",
        buffer_size_bytes_,
        " bytes) is too small for output view [",
        world_size_,
        ", ",
        max_batch_size_,
        ", ",
        msg_size,
        "]");

    intranode_alltoall(
        x,
        reinterpret_cast<void**>(buffer_ptrs_gpu_),
        reinterpret_cast<void**>(signal_ptrs_gpu_),
        batch_size,
        max_batch_size_,
        1,
        msg_size,
        rank_,
        world_size_,
        device_semaphore_,
        is_transpose,
        mask,
        offsets);

    auto options = torch::TensorOptions().dtype(x.dtype()).device(x.device());
    return torch::from_blob(local_buffer_, {world_size_, max_batch_size_, msg_size}, options);
}

torch::Tensor AllToAllBuffer::get_local_buffer()
{
    auto options = torch::TensorOptions().dtype(torch::kInt8).device(torch::kCUDA);
    return torch::from_blob(local_buffer_, {buffer_size_bytes_}, options);
}

}  // namespace dlslime
