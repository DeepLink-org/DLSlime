#include "memory_pool.h"

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include "cuda_common.cuh"

namespace dlslime {

static std::string auto_name_from_ptr(uintptr_t ptr)
{
    std::ostringstream oss;
    oss << "auto_0x" << std::hex << ptr;
    return oss.str();
}

int32_t NVLinkMemoryPool::register_memory_region(uintptr_t                  addr,
                                                 uint64_t                   offset,
                                                 size_t                     length,
                                                 std::optional<std::string> name)
{
    if (ptr_to_handle_.count(addr)) {
        int32_t handle = ptr_to_handle_[addr];
        if (name.has_value()) {
            name_to_handle_[name.value()] = handle;
            handle_to_name_[handle]       = name.value();
        }
        return handle;
    }

    cudaIpcMemHandle_t ipc_handle;
    CUDACHECK(cudaIpcGetMemHandle(&ipc_handle, (char*)addr));

    nvlink_mr_t mr{};
    mr.addr       = addr;
    mr.offset     = offset;
    mr.length     = length;
    mr.ipc_handle = ipc_handle;

    int32_t     handle  = static_cast<int32_t>(handle_to_mr_.size());
    std::string mr_name = name.value_or(auto_name_from_ptr(addr));

    handle_to_mr_.push_back(mr);
    handle_to_name_.push_back(mr_name);
    ptr_to_handle_[addr]     = handle;
    name_to_handle_[mr_name] = handle;

    return handle;
}

int32_t NVLinkMemoryPool::unregister_memory_region(int32_t handle)
{
    if (handle < 0 || static_cast<size_t>(handle) >= handle_to_mr_.size()) {
        return -1;
    }
    auto& mr = handle_to_mr_[handle];
    ptr_to_handle_.erase(mr.addr);
    if (static_cast<size_t>(handle) < handle_to_name_.size()) {
        name_to_handle_.erase(handle_to_name_[handle]);
    }
    mr = nvlink_mr_t{};
    return 0;
}

int32_t NVLinkMemoryPool::register_remote_memory_region(const json& mr_info, std::optional<std::string> name)
{
    std::string mr_name = name.value_or(mr_info.value("name", ""));
    if (mr_name.empty()) {
        SLIME_LOG_ERROR("Remote MR registration requires a name (via parameter or JSON)");
        return -1;
    }

    if (remote_name_to_handle_.count(mr_name)) {
        return remote_name_to_handle_[mr_name];
    }

    char*              remote_ptr;
    cudaIpcMemHandle_t ipc_handle;
    for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; ++i)
        ipc_handle.reserved[i] = mr_info["ipc_handle"][i].get<char>();
    cudaIpcOpenMemHandle((void**)&remote_ptr, ipc_handle, cudaIpcMemLazyEnablePeerAccess);

    nvlink_mr_t mr{};
    mr.addr       = (uintptr_t)remote_ptr;
    mr.offset     = mr_info["offset"];
    mr.length     = mr_info["length"];
    mr.ipc_handle = ipc_handle;

    int32_t handle = static_cast<int32_t>(remote_handle_to_mr_.size());
    remote_handle_to_mr_.push_back(mr);
    remote_handle_to_name_.push_back(mr_name);
    remote_name_to_handle_[mr_name] = handle;

    return handle;
}

int32_t NVLinkMemoryPool::unregister_remote_memory_region(int32_t handle)
{
    if (handle < 0 || static_cast<size_t>(handle) >= remote_handle_to_mr_.size()) {
        return -1;
    }
    auto& mr = remote_handle_to_mr_[handle];
    cudaIpcCloseMemHandle((char*)mr.addr);
    if (static_cast<size_t>(handle) < remote_handle_to_name_.size()) {
        remote_name_to_handle_.erase(remote_handle_to_name_[handle]);
    }
    mr = nvlink_mr_t{};
    return 0;
}

int32_t NVLinkMemoryPool::get_mr_handle(const std::string& name)
{
    auto it = name_to_handle_.find(name);
    if (it != name_to_handle_.end())
        return it->second;
    return -1;
}

int32_t NVLinkMemoryPool::get_mr_handle(uintptr_t data_ptr)
{
    auto it = ptr_to_handle_.find(data_ptr);
    if (it != ptr_to_handle_.end())
        return it->second;
    return -1;
}

int32_t NVLinkMemoryPool::get_remote_mr_handle(const std::string& name)
{
    auto it = remote_name_to_handle_.find(name);
    if (it != remote_name_to_handle_.end())
        return it->second;
    return -1;
}

const json NVLinkMemoryPool::mr_info() const
{
    json info;
    for (auto const& [name, handle] : name_to_handle_) {
        info[name] = handle_to_mr_[handle].json_info(name);
    }
    return info;
}

const json NVLinkMemoryPool::remote_mr_info() const
{
    json info;
    for (auto const& [name, handle] : remote_name_to_handle_) {
        info[name] = remote_handle_to_mr_[handle].json_info(name);
    }
    return info;
}
}  // namespace dlslime
