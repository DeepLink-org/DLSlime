#pragma once

#include <cuda_runtime.h>

#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

#include "dlslime/engine/nvshmem/kernels/api.cuh"
#include "dlslime/engine/nvshmem/kernels/internode_ll.cuh"
#include "dlslime/json.hpp"
#include "dlslime/logging.h"

namespace dlslime {

using json = nlohmann::json;

class NVShmemContext {

    inline static constexpr int NUM_P2P_RANKS     = 2;
    inline static constexpr int MSG_SIZE_PER_WARP = 4096;
    inline static constexpr int NUM_WARP_PER_SM   = 32;
    inline static constexpr int NVSHMEM_ALIGNMENT = 4096;

    typedef struct NVShmemContextBuffer {
        explicit NVShmemContextBuffer() = default;
        std::string mr_key;
        uintptr_t   data_ptr;
        int         offset;
        size_t      length;
        void*       nvshmem_data_buffer;
        void*       nvshmem_signal_buffer;
    } nvshmem_context_buffer_t;

public:
    NVShmemContext(const int rank, const int world_size, const int gpu_device_id);

    json getLocalNVShmemUniqueId() const;

    int connectFullMesh(const std::vector<json> remote_info, int root_id = 0);

    void* allocBuffer(size_t size, size_t alignment);

    DLManagedTensor* allocDLPackTensor(size_t size, size_t alignment);

    void
    registerMemoryRegion(const std::string& mr_key, const uintptr_t data_ptr, const int offset, const size_t length);

    void send(std::string mr_key, int dst);

    void recv(std::string mr_key, int src);

    void barrier();

    int gpu_device_id()
    {
        return gpu_device_id_;
    }

private:
    int rank_;
    int world_size_;

    int gpu_device_id_;
    int num_device_sms_;

    std::map<std::string, std::shared_ptr<nvshmem_context_buffer_t>> memory_pool_;
};
}  // namespace dlslime
