#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "adxl/adxl_types.h"
#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/logging.h"

namespace dlslime {
using json = nlohmann::json;

struct ascend_local_mr_t {
    uintptr_t       addr;
    uint64_t        offset;
    size_t          length;
    adxl::MemHandle handle;

    const json json_info(const std::string& name) const
    {
        json mr_info;
        mr_info["name"]   = name;
        mr_info["addr"]   = addr;
        mr_info["offset"] = offset;
        mr_info["length"] = length;
        return mr_info;
    }
};

class AscendLocalMemoryPool {
public:
    AscendLocalMemoryPool()  = default;
    ~AscendLocalMemoryPool() = default;

    int32_t register_memory_region(uintptr_t                  addr,
                                   uint64_t                   offset,
                                   size_t                     length,
                                   adxl::MemHandle            mem_handle,
                                   std::optional<std::string> name = std::nullopt);

    int32_t unregister_memory_region(int32_t handle);

    inline ascend_local_mr_t get_mr_fast(int32_t handle) const
    {
        if (handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size()) {
            return handle_to_mr_[handle];
        }
        return ascend_local_mr_t{};
    }

    inline bool has_mr(int32_t handle) const
    {
        return handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size();
    }

    int32_t get_mr_handle(const std::string& name) const;
    int32_t get_mr_handle(uintptr_t data_ptr) const;

    inline adxl::MemHandle get_adxl_handle(int32_t handle) const
    {
        if (handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size()) {
            return handle_to_mr_[handle].handle;
        }
        return adxl::MemHandle{};
    }

    const json mr_info() const;

    inline size_t size() const
    {
        return handle_to_mr_.size();
    }

private:
    std::unordered_map<std::string, int32_t> name_to_handle_;
    std::unordered_map<uintptr_t, int32_t>   ptr_to_handle_;
    std::vector<ascend_local_mr_t>           handle_to_mr_;
    std::vector<std::string>                 handle_to_name_;
};

}  // namespace dlslime
