#include "dlslime/csrc/engine/rdma/memory_pool.h"

#include <infiniband/verbs.h>
#include <sys/types.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <unordered_map>

#include "dlslime/csrc/logging.h"

namespace dlslime {

int32_t RDMAMemoryPool::registerMemoryRegion(uintptr_t data_ptr, uint64_t length, std::optional<std::string> name)
{
    std::unique_lock<std::mutex> lock(name_mutex_);

    // Check if pointer is already registered
    if (ptr_to_handle_.count(data_ptr)) {
        int32_t        handle   = ptr_to_handle_[data_ptr];
        struct ibv_mr* existing = id_to_mr_[handle];

        if (existing->length >= length) {
            // Existing MR covers the requested range — reuse it
            if (name.has_value()) {
                if (name_to_id_.count(name.value()) && name_to_id_[name.value()] != handle) {
                    SLIME_LOG_ERROR("Name ", name.value(), " registered to diff handle.");
                    return -1;
                }
                name_to_id_[name.value()] = handle;
            }
            return handle;
        }

        // Existing MR is too small (address reused for larger buffer) — re-register
        SLIME_LOG_INFO(
            "Re-registering MR at ", (void*)data_ptr, ": old length=", existing->length, ", new length=", length);
        ibv_dereg_mr(existing);

        int     access_rights = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
        ibv_mr* mr            = ibv_reg_mr(pd_, (void*)data_ptr, length, access_rights);
        SLIME_ASSERT(mr, " Failed to re-register memory " << data_ptr);
        id_to_mr_[handle] = mr;

        if (name.has_value()) {
            name_to_id_[name.value()] = handle;
        }
        return handle;
    }

    // New Registration
    int     access_rights = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    ibv_mr* mr            = ibv_reg_mr(pd_, (void*)data_ptr, length, access_rights);
    SLIME_ASSERT(mr, " Failed to register memory " << data_ptr);

    int32_t handle = id_to_mr_.size();
    id_to_mr_.push_back(mr);
    ptr_to_handle_[data_ptr] = handle;

    if (name.has_value()) {
        if (name_to_id_.count(name.value())) {
            SLIME_LOG_ERROR("Name ", name.value(), " exists but ptr mismatch.");
            return -1;
        }
        name_to_id_[name.value()] = handle;
    }

    // SLIME_LOG_DEBUG("Registered MR: Handle=", handle, ", Ptr=", (void*)data_ptr, ", Name=", (name.has_value() ?
    // name.value() : "None"));
    if (name.has_value()) {
        SLIME_LOG_INFO("Registered Local MR: Name=", name.value(), ", Handle=", handle, ", Ptr=", (void*)data_ptr);
    }
    else {
        SLIME_LOG_INFO("Registered Local MR: Handle=", handle, ", Ptr=", (void*)data_ptr);
    }
    return handle;
}

int32_t RDMAMemoryPool::get_mr_handle(const std::string& name)
{
    std::unique_lock<std::mutex> lock(name_mutex_);
    auto                         it = name_to_id_.find(name);
    if (it != name_to_id_.end()) {
        SLIME_LOG_INFO("Lookup Local MR Name=", name, " -> Handle=", it->second);
        return it->second;
    }
    SLIME_LOG_WARN("Lookup Local MR Name=", name, " FAILED");
    return -1;
}

int32_t RDMAMemoryPool::get_mr_handle(uintptr_t data_ptr)
{
    std::unique_lock<std::mutex> lock(name_mutex_);
    if (ptr_to_handle_.count(data_ptr)) {
        return ptr_to_handle_[data_ptr];
    }
    return -1;
}

int RDMAMemoryPool::unregisterMemoryRegion(const uintptr_t& mr_key)
{
    std::unique_lock<std::mutex> lock(mrs_mutex_);
    if (mrs_.count(mr_key)) {
        ibv_dereg_mr(mrs_[mr_key]);
        mrs_.erase(mr_key);
    }
    // Note: We don't currently support unregistering by name or cleaning up id_to_mr_ easily
    // without leaving holes, but for this use case (static topology) it's likely fine.
    return 0;
}

json RDMAMemoryPool::mr_info()
{
    std::unique_lock<std::mutex> lock(name_mutex_);
    json                         mr_info;
    for (auto const& [name, handle] : name_to_id_) {
        struct ibv_mr* mr = id_to_mr_[handle];
        mr_info[name]     = {
            {"handle", handle},
            {"addr", (uintptr_t)mr->addr},
            {"rkey", mr->rkey},
            {"length", mr->length},
        };
        SLIME_LOG_INFO("Exporting MR Info: Name=", name, ", Handle=", handle, ", RKey=", mr->rkey);
    }
    return mr_info;
}

}  // namespace dlslime
