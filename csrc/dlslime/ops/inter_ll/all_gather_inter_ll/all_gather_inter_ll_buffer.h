#pragma once

#include <torch/torch.h>

#include <cstdint>
#include <functional>
#include <tuple>
#include <vector>

#include "dlslime/json.hpp"
#include "ops/configs.cuh"
#include "ops/nvshmem_common.cuh"
#include "torch/types.h"

namespace dlslime {

using json = nlohmann::json;

class AllGatherInterLLBuffer {

    static constexpr int32_t nvshmem_alignment = 16;
    static constexpr int32_t root_rank         = 0;

public:
    AllGatherInterLLBuffer(int64_t      max_bs,
                           int64_t      msg_size,
                           torch::Dtype dtype,
                           int64_t      world_size,
                           int64_t      rank,
                           int64_t      num_concurrency);
    AllGatherInterLLBuffer(int64_t      max_bs,
                           int64_t      msg_size,
                           torch::Dtype dtype,
                           int64_t      world_size,
                           int64_t      rank,
                           int64_t      num_concurrency,
                           bool         allow_nvlink);

    size_t getBufferSize();

    int64_t itemsize();

    int64_t localRank();

    json bufferInfo();

    int connectFullMesh(std::vector<json> all_ipc_info);

    int allocSymBuffer();

    torch::Tensor                                    allGatherLL(torch::Tensor q, int32_t tag = 0);
    std::tuple<torch::Tensor, std::function<void()>> allGatherLLHook(torch::Tensor q, int32_t tag = 0);

private:
    int8_t* sym_buffer_;
    int*    sym_signal_;

    int64_t max_bs_;
    int64_t msg_size_;

    torch::Dtype dtype_;

    int64_t world_size_;
    int64_t rank_;

    int64_t num_concurrency_;

    bool allow_nvlink_{false};
};

}  // namespace dlslime
