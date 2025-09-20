#include "engine/rdma/memory_pool.h"

#include "logging.h"

#include <cstdint>
#include <cstdlib>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <unordered_map>

namespace slime {
int RDMAMemoryPool::register_memory_region(const std::string& mr_key, uintptr_t data_ptr, uint64_t length)
{
    std::unique_lock<std::mutex> lock(mrs_mutex_);
    if (mrs_.count(mr_key)) {
        SLIME_LOG_DEBUG("mr_key ", mr_key, " has already been registered.");
        ibv_dereg_mr(mrs_[mr_key]);
    }
    /* MemoryRegion Access Right = 777 */
    const static int access_rights = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    ibv_mr*          mr            = ibv_reg_mr(pd_, (void*)data_ptr, length, access_rights);

    SLIME_ASSERT(mr, " Failed to register memory " << data_ptr);

    SLIME_LOG_DEBUG("Memory region: " << mr_key << ", " << (void*)data_ptr << " -- " << (void*)(data_ptr + length)
                                      << ", Device name: " << pd_->context->device->dev_name << ", Length: " << length
                                      << " (" << length / 1024 / 1024 << " MB)"
                                      << ", Permission: " << access_rights << ", LKey: " << mr->lkey
                                      << ", RKey: " << mr->rkey);

    mrs_[mr_key] = mr;
    return 0;
}

int RDMAMemoryPool::unregister_memory_region(const std::string& mr_key)
{
    std::unique_lock<std::mutex> lock(mrs_mutex_);
    ibv_dereg_mr(mrs_[mr_key]);
    mrs_.erase(mr_key);
    return 0;
}

int RDMAMemoryPool::register_remote_memory_region(const std::string& mr_key,
                                                  uintptr_t          addr,
                                                  size_t             length,
                                                  uint32_t           rkey)
{
    std::unique_lock<std::mutex> lock(remote_mrs_mutex_);
    remote_mrs_[mr_key] = remote_mr_t(addr, length, rkey);
    SLIME_LOG_DEBUG("Remote memory region registered: " << mr_key << ", " << addr << ", " << length << ", " << rkey << ".");
    return 0;
}

int RDMAMemoryPool::register_remote_memory_region(const std::string& mr_key, const json& mr_info)
{
    std::unique_lock<std::mutex> lock(remote_mrs_mutex_);
    remote_mrs_[mr_key] =
        remote_mr_t(mr_info["addr"].get<uintptr_t>(), mr_info["length"].get<size_t>(), mr_info["rkey"].get<uint32_t>());
    SLIME_LOG_DEBUG("Remote memory region registered: " << mr_key << ", " << mr_info << ".");
    return 0;
}

int RDMAMemoryPool::unregister_remote_memory_region(const std::string& mr_key)
{
    std::unique_lock<std::mutex> lock(remote_mrs_mutex_);
    remote_mrs_.erase(mr_key);
    return 0;
}

json RDMAMemoryPool::mr_info()
{
    std::unique_lock<std::mutex> lock(mrs_mutex_);
    json mr_info;
    for (auto& mr : mrs_) {
        mr_info[mr.first] = {
            {"addr", (uintptr_t)mr.second->addr},
            {"rkey", mr.second->rkey},
            {"length", mr.second->length},
        };
    }
    return mr_info;
}

json RDMAMemoryPool::remote_mr_info()
{
    std::unique_lock<std::mutex> lock(remote_mrs_mutex_);
    json mr_info;
    for (auto& mr : remote_mrs_) {
        mr_info[mr.first] = {{"addr", mr.second.addr}, {"rkey", mr.second.rkey}, {"length", mr.second.length}};
    }
    return mr_info;
}

}  // namespace slime
