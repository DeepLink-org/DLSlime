#pragma once

#include <cstdint>
#include <vector>

#include <torch/torch.h>

#include "json.hpp"
#include "ops/configs.cuh"
#include "ops/nvshmem_common.cuh"
#include "torch/types.h"

namespace slime {

using json = nlohmann::json;

class AllGatherInterLLBuffer {

    static constexpr int32_t nvshmem_alignment = 16;
    static constexpr int32_t root_rank         = 0;

public:
    AllGatherInterLLBuffer(int32_t max_bs, int32_t msg_size, torch::Dtype dtype, int32_t world_size, int32_t rank);

    int32_t getBufferSize();

    int32_t itemsize();

    json buffer_info();

    int connectFullMesh(std::vector<json> all_ipc_info);

    int allocBuffer();

    torch::Tensor allGatherLL(torch::Tensor q);

private:
    int8_t* sym_buffer_;
    int*    sym_signal_;

    int32_t max_bs_;
    int32_t msg_size_;

    torch::Dtype dtype_;

    int32_t world_size_;
    int32_t rank_;
};

}  // namespace slime
