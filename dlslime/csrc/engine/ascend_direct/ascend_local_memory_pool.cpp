#include "ascend_local_memory_pool.h"

#include <cstdint>
#include <sstream>
#include <string>

#include "dlslime/csrc/logging.h"

namespace dlslime {

static std::string auto_name_from_ptr(uintptr_t ptr)
{
    std::ostringstream oss;
    oss << "auto_0x" << std::hex << ptr;
    return oss.str();
}

int32_t AscendLocalMemoryPool::register_memory_region(
    uintptr_t addr, uint64_t offset, size_t length, adxl::MemHandle mem_handle, std::optional<std::string> name)
{
    if (ptr_to_handle_.count(addr)) {
        int32_t h = ptr_to_handle_[addr];
        if (name.has_value()) {
            name_to_handle_[name.value()] = h;
            handle_to_name_[h]            = name.value();
        }
        SLIME_LOG_WARN("LOCAL memory region addr=0x", std::hex, addr, std::dec, " already registered, handle=", h);
        return h;
    }

    ascend_local_mr_t mr{};
    mr.addr   = addr;
    mr.offset = offset;
    mr.length = length;
    mr.handle = mem_handle;

    int32_t     handle  = static_cast<int32_t>(handle_to_mr_.size());
    std::string mr_name = name.value_or(auto_name_from_ptr(addr));

    handle_to_mr_.push_back(mr);
    handle_to_name_.push_back(mr_name);
    ptr_to_handle_[addr]     = handle;
    name_to_handle_[mr_name] = handle;

    SLIME_LOG_INFO("Registered LOCAL memory region: name=",
                   mr_name,
                   " handle=",
                   handle,
                   " addr=0x",
                   std::hex,
                   addr,
                   std::dec,
                   " offset=",
                   offset,
                   " length=",
                   length);

    return handle;
}

int32_t AscendLocalMemoryPool::unregister_memory_region(int32_t handle)
{
    if (handle < 0 || static_cast<size_t>(handle) >= handle_to_mr_.size()) {
        SLIME_LOG_WARN("Attempted to unregister non-existent LOCAL memory region handle: ", handle);
        return -1;
    }

    auto& mr = handle_to_mr_[handle];
    ptr_to_handle_.erase(mr.addr);
    if (static_cast<size_t>(handle) < handle_to_name_.size()) {
        name_to_handle_.erase(handle_to_name_[handle]);
    }
    mr = ascend_local_mr_t{};

    SLIME_LOG_INFO("Unregistered LOCAL memory region: handle=", handle);
    return 0;
}

int32_t AscendLocalMemoryPool::get_mr_handle(const std::string& name) const
{
    auto it = name_to_handle_.find(name);
    if (it != name_to_handle_.end())
        return it->second;
    return -1;
}

int32_t AscendLocalMemoryPool::get_mr_handle(uintptr_t data_ptr) const
{
    auto it = ptr_to_handle_.find(data_ptr);
    if (it != ptr_to_handle_.end())
        return it->second;
    return -1;
}

const json AscendLocalMemoryPool::mr_info() const
{
    json all_mr_info;
    for (const auto& [name, handle] : name_to_handle_) {
        all_mr_info[name] = handle_to_mr_[handle].json_info(name);
    }
    return all_mr_info;
}

}  // namespace dlslime
