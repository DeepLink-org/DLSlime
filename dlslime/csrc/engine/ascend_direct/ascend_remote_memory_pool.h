#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/logging.h"

namespace dlslime {
using json = nlohmann::json;

struct ascend_remote_mr_t {
    uintptr_t addr;
    uint64_t  offset;
    size_t    length;

    const json json_info(const std::string& name) const
    {
        json mr_info;
        mr_info["name"]   = name;
        mr_info["addr"]   = addr;
        mr_info["offset"] = offset;
        mr_info["length"] = length;
        return mr_info;
    }

    static ascend_remote_mr_t from_json(const json& mr_info)
    {
        ascend_remote_mr_t mr{};
        mr.addr   = mr_info.value("addr", 0UL);
        mr.offset = mr_info.value("offset", 0UL);
        mr.length = mr_info.value("length", 0UL);
        return mr;
    }
};

class AscendRemoteMemoryPool {
public:
    AscendRemoteMemoryPool()  = default;
    ~AscendRemoteMemoryPool() = default;

    int32_t register_remote_memory_region(const json& mr_info, std::optional<std::string> name = std::nullopt);

    int32_t unregister_remote_memory_region(int32_t handle);

    inline ascend_remote_mr_t get_remote_mr_fast(int32_t handle) const
    {
        if (handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size()) {
            return handle_to_mr_[handle];
        }
        return ascend_remote_mr_t{};
    }

    inline bool has_remote_mr(int32_t handle) const
    {
        return handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size();
    }

    int32_t get_mr_handle(const std::string& name) const;

    const json remote_mr_info() const;

    inline size_t size() const
    {
        return handle_to_mr_.size();
    }

private:
    std::unordered_map<std::string, int32_t> name_to_handle_;
    std::vector<ascend_remote_mr_t>          handle_to_mr_;
    std::vector<std::string>                 handle_to_name_;
};

}  // namespace dlslime
