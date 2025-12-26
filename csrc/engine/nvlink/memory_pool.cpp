#include <cstdint>
#include <vector>

#include "cuda_common.cuh"
#include "memory_pool.h"

namespace slime {
int NVLinkMemoryPool::register_memory_region(const uintptr_t& mr_key, uintptr_t addr, uint64_t offset, size_t length)
{
    cudaIpcMemHandle_t ipc_handle;
    CUDACHECK(cudaIpcGetMemHandle(&ipc_handle, (char*)addr));
    mrs_[mr_key] = nvlink_mr({mr_key, addr, offset, length, ipc_handle});
    return 0;
}

int NVLinkMemoryPool::unregister_memory_region(const uintptr_t& mr_key)
{
    mrs_.erase(mr_key);
    return 0;
}

int NVLinkMemoryPool::register_remote_memory_region(const uintptr_t& mr_key, const json& mr_info)
{
    char*              remote_ptr;
    cudaIpcMemHandle_t ipc_handle;
    for (int i = 0; i < CUDA_IPC_HANDLE_SIZE; ++i)
        ipc_handle.reserved[i] = mr_info["ipc_handle"][i].get<char>();
    cudaIpcOpenMemHandle((void**)&remote_ptr, ipc_handle, cudaIpcMemLazyEnablePeerAccess);
    remote_mrs_[mr_key] = nvlink_mr({mr_key, (uintptr_t)remote_ptr, mr_info["offset"], mr_info["length"], ipc_handle});

    return 0;
}

int NVLinkMemoryPool::unregister_remote_memory_region(const uintptr_t& mr_key)
{
    cudaIpcCloseMemHandle((char*)remote_mrs_[mr_key].addr);
    remote_mrs_.erase(mr_key);
    return 0;
}

const json NVLinkMemoryPool::mr_info() const
{
    json mr_info;
    for (auto& mr : mrs_) {
        mr_info[std::to_string(mr.first)] = mr.second.json_info();
    }
    return mr_info;
}

const json NVLinkMemoryPool::remote_mr_info() const
{
    json remote_mr_info;
    for (auto& mr : remote_mrs_) {
        remote_mr_info[std::to_string(mr.first)] = mr.second.json_info();
    }
    return remote_mr_info;
}
}  // namespace slime
