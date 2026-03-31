#include "ascend_remote_memory_pool.h"

#include <cstdint>
#include <string>

#include "dlslime/csrc/logging.h"

namespace dlslime {

int32_t AscendRemoteMemoryPool::register_remote_memory_region(const json& mr_info, std::optional<std::string> name)
{
    std::string mr_name = name.value_or(mr_info.value("name", ""));
    if (mr_name.empty()) {
        SLIME_LOG_ERROR("Remote MR registration requires a name (via parameter or JSON)");
        return -1;
    }

    if (name_to_handle_.count(mr_name)) {
        SLIME_LOG_WARN("Remote memory region name=", mr_name, " already registered, handle=", name_to_handle_[mr_name]);
        return name_to_handle_[mr_name];
    }

    if (!mr_info.contains("addr") || !mr_info.contains("offset") || !mr_info.contains("length")) {
        SLIME_LOG_ERROR("Invalid remote mr_info JSON: missing required fields (addr, offset, length)");
        return -1;
    }

    ascend_remote_mr_t remote_mr = ascend_remote_mr_t::from_json(mr_info);

    int32_t handle = static_cast<int32_t>(handle_to_mr_.size());
    handle_to_mr_.push_back(remote_mr);
    handle_to_name_.push_back(mr_name);
    name_to_handle_[mr_name] = handle;

    SLIME_LOG_INFO("Registered REMOTE memory region: name=",
                   mr_name,
                   " handle=",
                   handle,
                   " addr=0x",
                   std::hex,
                   remote_mr.addr,
                   std::dec,
                   " offset=",
                   remote_mr.offset,
                   " length=",
                   remote_mr.length);

    return handle;
}

int32_t AscendRemoteMemoryPool::unregister_remote_memory_region(int32_t handle)
{
    if (handle < 0 || static_cast<size_t>(handle) >= handle_to_mr_.size()) {
        SLIME_LOG_WARN("Attempted to unregister non-existent REMOTE memory region handle: ", handle);
        return -1;
    }

    if (static_cast<size_t>(handle) < handle_to_name_.size()) {
        name_to_handle_.erase(handle_to_name_[handle]);
    }
    handle_to_mr_[handle] = ascend_remote_mr_t{};

    SLIME_LOG_INFO("Unregistered REMOTE memory region: handle=", handle);
    return 0;
}

int32_t AscendRemoteMemoryPool::get_mr_handle(const std::string& name) const
{
    auto it = name_to_handle_.find(name);
    if (it != name_to_handle_.end())
        return it->second;
    return -1;
}

const json AscendRemoteMemoryPool::remote_mr_info() const
{
    json all_remote_mr_info;
    for (const auto& [name, handle] : name_to_handle_) {
        all_remote_mr_info[name] = handle_to_mr_[handle].json_info(name);
    }
    return all_remote_mr_info;
}

}  // namespace dlslime
