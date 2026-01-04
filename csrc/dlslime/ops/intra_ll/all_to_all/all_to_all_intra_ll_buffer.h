#pragma once

#include <torch/torch.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <vector>

#include "all_to_all_intra_ll.h"
#include "dlslime/json.hpp"
#include "dlslime/ops/exception.cuh"
#include "torch/types.h"

using json = nlohmann::json;

namespace dlslime {

class AllToAllIntraLLBuffer {

public:
    AllToAllIntraLLBuffer(
        int32_t max_dispatch_per_msg, int32_t max_bs, int32_t rank, int32_t world_size, int64_t local_buffer_size);

    static int64_t
    get_buffer_size_hint(int32_t max_bs, int32_t max_dispatch_per_msg, int32_t max_msg_size, int32_t itemsize);

    json buffer_info();

    int connectFullMesh(std::vector<json> all_buffer_info);

    int allocBuffer(std::optional<int64_t> local_buffer_size = std::nullopt);

    torch::Tensor
    allToAllLL2D(torch::Tensor q, bool is_transpose = false, c10::optional<torch::Tensor> mask = c10::nullopt);

    torch::Tensor getLocalBuffer();

    int setMaxBs(int32_t bs)
    {
        max_bs_ = bs;
        return 0;
    }

private:
    int8_t** buffer_ptrs_;
    int**    signal_ptrs_;

    int8_t*            local_buffer_;
    cudaIpcMemHandle_t local_buffer_ipc_handle_;

    int*               local_signal_;
    cudaIpcMemHandle_t local_signal_ipc_handle_;

    int32_t max_bs_;
    int32_t max_dispatch_per_msg_;

    int32_t world_size_;
    int32_t rank_;

    int64_t local_buffer_size_;
};
}  // namespace dlslime
