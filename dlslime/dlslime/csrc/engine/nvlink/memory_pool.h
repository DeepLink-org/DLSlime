#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cuda_common.cuh"
#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/logging.h"

namespace dlslime {
using json = nlohmann::json;

typedef struct nvlink_mr {
    uintptr_t          addr;
    uint64_t           offset;
    size_t             length;
    cudaIpcMemHandle_t ipc_handle;

    const json json_info(const std::string& name) const
    {
        json mr_info;
        mr_info["name"]       = name;
        mr_info["addr"]       = addr;
        mr_info["offset"]     = offset;
        mr_info["length"]     = length;
        mr_info["ipc_handle"] = std::vector<char>{};
        for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; i++)
            mr_info["ipc_handle"][i] = ipc_handle.reserved[i];

        return mr_info;
    }
} nvlink_mr_t;

class NVLinkMemoryPool {
public:
    NVLinkMemoryPool() = default;

    int32_t register_memory_region(uintptr_t                  addr,
                                   uint64_t                   offset,
                                   size_t                     length,
                                   std::optional<std::string> name = std::nullopt);
    int32_t unregister_memory_region(int32_t handle);

    int32_t register_remote_memory_region(const json& mr_info, std::optional<std::string> name = std::nullopt);
    int32_t unregister_remote_memory_region(int32_t handle);

    inline nvlink_mr_t get_mr_fast(int32_t handle)
    {
        if (handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size()) {
            return handle_to_mr_[handle];
        }
        return nvlink_mr_t{};
    }

    inline nvlink_mr_t get_remote_mr_fast(int32_t handle)
    {
        if (handle >= 0 && static_cast<size_t>(handle) < remote_handle_to_mr_.size()) {
            return remote_handle_to_mr_[handle];
        }
        return nvlink_mr_t{};
    }

    int32_t get_mr_handle(const std::string& name);
    int32_t get_mr_handle(uintptr_t data_ptr);
    int32_t get_remote_mr_handle(const std::string& name);

    const json mr_info() const;
    const json remote_mr_info() const;

private:
    std::unordered_map<std::string, int32_t> name_to_handle_;
    std::unordered_map<uintptr_t, int32_t>   ptr_to_handle_;
    std::vector<nvlink_mr_t>                 handle_to_mr_;
    std::vector<std::string>                 handle_to_name_;

    std::unordered_map<std::string, int32_t> remote_name_to_handle_;
    std::vector<nvlink_mr_t>                 remote_handle_to_mr_;
    std::vector<std::string>                 remote_handle_to_name_;
};
}  // namespace dlslime
