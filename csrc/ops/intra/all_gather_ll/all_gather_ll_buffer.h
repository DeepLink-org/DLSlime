#pragma once

#include "utils/cuda_common.h"
#include "utils/json.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <vector>

#include "ops/intra/all_gather_ll/all_gather_ll.h"

using json = nlohmann::json;

namespace slime {

class AllGatherLLBuffer {

public:
    AllGatherLLBuffer(
        int32_t max_bs, int32_t num_head, int32_t head_size, int32_t itemsize, int32_t world_size, int32_t rank);

    int32_t get_buffer_size();

    json ipc_info();

    int connectFullMesh(std::vector<json> all_ipc_info);

    int allocBuffer();

    void allGatherLL(uintptr_t q);

    int8_t** buffer_ptrs_;
    int**    signal_ptrs_;

    int8_t*            local_buffer_;
    cudaIpcMemHandle_t local_buffer_ipc_handle_;

    int*               local_signal_;
    cudaIpcMemHandle_t local_signal_ipc_handle_;

    int32_t max_bs_;
    int32_t num_head_;
    int32_t head_size_;
    int32_t itemsize_;
    int32_t world_size_;
    int32_t rank_;
};
}  // namespace slime
