#pragma once

#include <cstdint>
#include <cstdlib>

#include <vector>

#include "utils/cuda_common.h"
#include "utils/json.hpp"

using json = nlohmann::json;

namespace slime {

class AllGatherLLBuffer {

public:
    AllGatherLLBuffer(
        size_t max_bs, size_t num_head, size_t head_size, size_t itemsize, size_t world_size, size_t rank):
        max_bs_(max_bs),
        num_head_(num_head),
        head_size_(head_size),
        itemsize_(itemsize),
        world_size_(world_size),
        rank_(rank)
    {
        size_t buffer_size = get_buffer_size();
        allocBuffer(buffer_size);
    }

    size_t get_buffer_size() {
        return max_bs_ * num_head_ * head_size_ * itemsize_;
    }

    json endpoint_info();

    int connectFullMesh(std::vector<json> all_endpoint_info);

    int allocBuffer(size_t size) {
        CUDA_CHECK(cudaMalloc(&buffer_ptrs[nvl_rank], num_nvl_bytes + barrier_signal_bytes + buffer_ptr_bytes + barrier_signal_ptr_bytes));
    }

    void allGatherLL(uintptr_t q);

private:
    size_t max_bs_;
    size_t num_head_;
    size_t head_size_;
    size_t itemsize_;
    size_t world_size_;
    size_t rank_;

    int8_t** buffer_ptrs_;
};
}  // namespace slime
