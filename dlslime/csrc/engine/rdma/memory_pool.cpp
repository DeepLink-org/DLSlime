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
#include "dlslime/csrc/observability/obs.h"

namespace dlslime {

int32_t RDMAMemoryPool::registerMemoryRegion(uintptr_t data_ptr, uint64_t length, std::optional<std::string> name)
{
    std::unique_lock<std::mutex> lock(name_mutex_);

    // Observability: classify this MR as a system (internal) or
    // user-visible registration based on the "sys." name prefix.
    const bool is_system = name.has_value() && name.value().rfind("sys.", 0) == 0;

    // Check if pointer is already registered
    if (ptr_to_handle_.count(data_ptr)) {
        int32_t        handle   = ptr_to_handle_[data_ptr];
        struct ibv_mr* existing = handle_to_mr_[handle];

        if (existing->length >= length) {
            // Existing MR covers the requested range — reuse it
            if (name.has_value()) {
                if (name_to_handle_.count(name.value()) && name_to_handle_[name.value()] != handle) {
                    SLIME_LOG_ERROR("Name ", name.value(), " registered to diff handle.");
                    return -1;
                }
                name_to_handle_[name.value()] = handle;
            }
            return handle;
        }

        // Existing MR is too small (address reused for larger buffer) — re-register.
        // Only the delta (length - existing->length) should be added to the
        // MR-bytes counter; the old length was already counted at initial register.
        uint64_t old_length = existing->length;
        SLIME_LOG_INFO("Re-registering MR at ", (void*)data_ptr, ": old length=", old_length, ", new length=", length);
        ibv_dereg_mr(existing);

        int     access_rights = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
        ibv_mr* mr            = ibv_reg_mr(pd_, (void*)data_ptr, length, access_rights);
        SLIME_ASSERT(mr, " Failed to re-register memory " << data_ptr);
        handle_to_mr_[handle] = mr;

        if (name.has_value()) {
            name_to_handle_[name.value()] = handle;
        }

        // Carry forward the user/system classification of the original
        // registration — the handle's identity does not change.
        bool handle_is_system = false;
        auto sys_it           = handle_to_is_system_.find(handle);
        if (sys_it != handle_to_is_system_.end()) {
            handle_is_system = sys_it->second;
        }
        if (length > old_length) {
            obs::obs_record_mr_grow(length - old_length, handle_is_system);
        }
        return handle;
    }

    // New Registration
    int     access_rights = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    ibv_mr* mr            = ibv_reg_mr(pd_, (void*)data_ptr, length, access_rights);
    SLIME_ASSERT(mr, " Failed to register memory " << data_ptr);

    int32_t handle = handle_to_mr_.size();
    handle_to_mr_.push_back(mr);
    ptr_to_handle_[data_ptr]     = handle;
    handle_to_is_system_[handle] = is_system;

    if (name.has_value()) {
        if (name_to_handle_.count(name.value())) {
            SLIME_LOG_ERROR("Name ", name.value(), " exists but ptr mismatch.");
            return -1;
        }
        name_to_handle_[name.value()] = handle;
    }

    if (name.has_value()) {
        SLIME_LOG_INFO("Registered Local MR: Name=", name.value(), ", Handle=", handle, ", Ptr=", (void*)data_ptr);
    }
    else {
        SLIME_LOG_INFO("Registered Local MR: Handle=", handle, ", Ptr=", (void*)data_ptr);
    }

    obs::obs_record_mr_register(length, is_system);

    return handle;
}

int32_t RDMAMemoryPool::get_mr_handle(const std::string& name)
{
    std::unique_lock<std::mutex> lock(name_mutex_);
    auto                         it = name_to_handle_.find(name);
    if (it != name_to_handle_.end()) {
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
    auto                         it = mrs_.find(mr_key);
    if (it == mrs_.end() || it->second == nullptr) {
        SLIME_LOG_WARN("Attempted to unregister non-existent Local MR key=", mr_key);
        return -1;
    }

    int rc = ibv_dereg_mr(it->second);
    if (rc != 0) {
        SLIME_LOG_ERROR("Failed to unregister Local MR key=", mr_key, ", rc=", rc, ", errno=", errno);
        return rc;
    }
    mrs_.erase(it);
    return 0;
}

int RDMAMemoryPool::unregisterMemoryRegion(int32_t handle)
{
    std::unique_lock<std::mutex> lock(name_mutex_);
    if (handle < 0 || static_cast<size_t>(handle) >= handle_to_mr_.size() || handle_to_mr_[handle] == nullptr) {
        SLIME_LOG_WARN("Attempted to unregister non-existent Local MR handle=", handle);
        return -1;
    }

    // Capture length and user/system classification before deregistration
    // frees the MR struct / removes the map entry.
    uint64_t mr_length = handle_to_mr_[handle] ? handle_to_mr_[handle]->length : 0;
    bool     is_system = false;
    auto     sys_it    = handle_to_is_system_.find(handle);
    if (sys_it != handle_to_is_system_.end()) {
        is_system = sys_it->second;
    }

    int rc = ibv_dereg_mr(handle_to_mr_[handle]);
    if (rc != 0) {
        SLIME_LOG_ERROR("Failed to unregister Local MR handle=", handle, ", rc=", rc, ", errno=", errno);
        return rc;
    }

    obs::obs_record_mr_unregister(mr_length, is_system);

    handle_to_mr_[handle] = nullptr;
    handle_to_is_system_.erase(handle);

    for (auto it = name_to_handle_.begin(); it != name_to_handle_.end();) {
        if (it->second == handle) {
            it = name_to_handle_.erase(it);
        }
        else {
            ++it;
        }
    }
    for (auto it = ptr_to_handle_.begin(); it != ptr_to_handle_.end();) {
        if (it->second == handle) {
            it = ptr_to_handle_.erase(it);
        }
        else {
            ++it;
        }
    }
    return 0;
}

int RDMAMemoryPool::unregisterMemoryRegion(const std::string& name)
{
    int32_t handle = -1;
    {
        std::unique_lock<std::mutex> lock(name_mutex_);
        auto                         it = name_to_handle_.find(name);
        if (it == name_to_handle_.end()) {
            SLIME_LOG_WARN("Attempted to unregister non-existent Local MR name=", name);
            return -1;
        }
        handle = it->second;
    }
    return unregisterMemoryRegion(handle);
}

json RDMAMemoryPool::mrInfo()
{
    std::unique_lock<std::mutex> lock(name_mutex_);
    json                         mr_info;
    for (auto const& [name, handle] : name_to_handle_) {
        struct ibv_mr* mr = handle_to_mr_[handle];
        if (mr == nullptr) {
            continue;
        }
        mr_info[name] = {
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
