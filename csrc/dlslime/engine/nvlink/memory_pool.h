#pragma once

#include <cstdint>
#include <unordered_map>

#include "cuda_common.cuh"
#include "dlslime/json.hpp"

namespace dlslime {
using json = nlohmann::json;

typedef struct nvlink_mr {
    uintptr_t          mr_key;
    uintptr_t          addr;
    uint64_t           offset;
    size_t             length;
    cudaIpcMemHandle_t ipc_handle;
    const json         json_info() const
    {
        json mr_info          = json{{"addr", addr}, {"offset", offset}, {"length", length}};
        mr_info["mr_key"]     = mr_key;
        mr_info["ipc_handle"] = std::vector<char>{};
        for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; i++)
            mr_info["ipc_handle"][i] = ipc_handle.reserved[i];

        return mr_info;
    }
} nvlink_mr_t;

class NVLinkMemoryPool {
public:
    NVLinkMemoryPool() = default;

    int register_memory_region(const uintptr_t& mr_key, uintptr_t addr, uint64_t offset, size_t length);
    int unregister_memory_region(const uintptr_t& mr_key);

    int register_remote_memory_region(const uintptr_t& mr_key, const json& mr_info);
    int unregister_remote_memory_region(const uintptr_t& mr_key);

    inline nvlink_mr_t get_mr(const uintptr_t& mr_key)
    {
        return mrs_[mr_key];
    }

    inline nvlink_mr_t get_remote_mr(const uintptr_t& mr_key)
    {
        return remote_mrs_[mr_key];
    }

    const json mr_info() const;
    const json remote_mr_info() const;

private:
    std::unordered_map<uintptr_t, nvlink_mr_t> mrs_;
    std::unordered_map<uintptr_t, nvlink_mr_t> remote_mrs_;
};
}  // namespace dlslime
