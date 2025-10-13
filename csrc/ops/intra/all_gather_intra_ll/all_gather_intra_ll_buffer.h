#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <vector>

#include <torch/torch.h>

#include "json.hpp"
#include "ops/exception.cuh"

#include "all_gather_intra_ll.h"
#include "torch/types.h"

using json = nlohmann::json;

namespace slime {

class AllGatherIntraLLBuffer {

public:
    AllGatherIntraLLBuffer(
        int32_t max_bs, int32_t msg_size, torch::Dtype dtype, int32_t world_size, int32_t rank);

    int32_t itemsize();

    int32_t get_buffer_size();

    json buffer_info();

    int connectFullMesh(std::vector<json> all_buffer_info);

    int allocBuffer();

    torch::Tensor allGatherLL(torch::Tensor q);

private:
    int8_t** buffer_ptrs_;
    int**    signal_ptrs_;

    int8_t*            local_buffer_;
    cudaIpcMemHandle_t local_buffer_ipc_handle_;

    int*               local_signal_;
    cudaIpcMemHandle_t local_signal_ipc_handle_;

    int32_t max_bs_;
    int32_t msg_size_;

    torch::Dtype dtype_;

    int32_t world_size_;
    int32_t rank_;
};
}  // namespace slime
