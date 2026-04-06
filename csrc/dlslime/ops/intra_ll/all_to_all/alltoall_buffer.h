#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <cuda_runtime.h>
#include <torch/torch.h>

#include "dlslime/json.hpp"

using json = nlohmann::json;

namespace dlslime {

enum class KernelImpl {
    Basic,
    TMA,
};

class AllToAllBuffer {

public:
    AllToAllBuffer(int32_t rank, int32_t world_size, int32_t max_batch_size, int64_t buffer_size_bytes);
    ~AllToAllBuffer();

    AllToAllBuffer(const AllToAllBuffer&)            = delete;
    AllToAllBuffer& operator=(const AllToAllBuffer&) = delete;

    void allocate();
    json get_ipc_handle_info();
    void connect_full_mesh(const std::vector<json>& all_handles);
    void reset_semaphore();
    torch::Tensor get_local_buffer();

    torch::Tensor all_to_all(torch::Tensor                x,
                             KernelImpl                  impl         = KernelImpl::Basic,
                             bool                        is_transpose = true,
                             c10::optional<torch::Tensor> mask         = c10::nullopt,
                             c10::optional<torch::Tensor> offsets      = c10::nullopt);

private:
    torch::Tensor dispatch_basic(torch::Tensor                x,
                                 bool                         is_transpose,
                                 c10::optional<torch::Tensor> mask,
                                 c10::optional<torch::Tensor> offsets);
    void          free_resources();

private:
    int32_t rank_;
    int32_t world_size_;
    int32_t max_batch_size_;
    int64_t buffer_size_bytes_;
    bool    is_allocated_ = false;

    int8_t* local_buffer_     = nullptr;
    int*    local_signal_     = nullptr;
    int*    device_semaphore_ = nullptr;

    cudaIpcMemHandle_t local_buffer_handle_;
    cudaIpcMemHandle_t local_signal_handle_;

    int8_t** buffer_ptrs_gpu_ = nullptr;
    int**    signal_ptrs_gpu_ = nullptr;

    std::vector<void*> ipc_opened_ptrs_cpu_;
};

}  // namespace dlslime
