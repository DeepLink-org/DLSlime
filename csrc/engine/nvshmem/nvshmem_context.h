#pragma once

#include "engine/nvshmem/kernels/api.cuh"
#include "utils/json.hpp"
#include "utils/logging.h"

#include <cstdint>
#include <iostream>
#include <vector>

#include <cuda_runtime.h>

namespace slime {

using json = nlohmann::json;

class NVShmemContext {

    inline static constexpr int NUM_P2P_RANKS = 2;

public:
    NVShmemContext(const int rank, const int world_size, const int gpu_device_id);

    json getLocalNVShmemUniqueId() const;

    int connectFullMesh(const std::vector<json> remote_info, int root_id = 0);

    void* allocBuffer(size_t size, size_t alignment)
    {
        return nvshmem_engine::internode::alloc(size, alignment);
    }

    void send(uintptr_t data, size_t offset, size_t length, int dst){
        // TODO: Hao Liu
        // TODO: DeepEP LL Send
        // internode::send<block, thread>();
    };

    void recv(uintptr_t data, size_t offset, size_t length, int src)
    {
        // TODO: Hao Liu
        // TODO: DeepEP LL Recv
        // internode::recv<block, thread>();
    }

private:
    int rank_;
    int world_size_;

    int gpu_device_id_;
    int num_device_sms_;

    void** buffers_;
};
}  // namespace slime
