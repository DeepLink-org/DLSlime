#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/logging.h"
#include "rdma_common.h"

namespace dlslime {

using json = nlohmann::json;

class RDMARemoteMemoryPool {
public:
    RDMARemoteMemoryPool() = default;
    ~RDMARemoteMemoryPool()
    {
        handle_to_mr_.clear();
    }

    // Register generic remote MR (Handle-based)
    // Returns handle
    int32_t registerRemoteMemoryRegion(const std::string& name, const json& mr_info)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        uintptr_t                    addr   = mr_info["addr"].get<uintptr_t>();
        size_t                       length = mr_info["length"].get<size_t>();
        uint32_t                     rkey   = mr_info["rkey"].get<uint32_t>();

        remote_mr_t mr(addr, length, rkey);

        if (name_to_handle_.count(name)) {
            int32_t handle = name_to_handle_[name];
            if (handle >= 0 && static_cast<size_t>(handle) < handle_to_mr_.size()) {
                remote_mr_t& existing = handle_to_mr_[handle];
                if (existing.addr != addr || existing.length != length || existing.rkey != rkey) {
                    existing = mr;
                    SLIME_LOG_INFO("Updated Remote MR: Name=",
                                   name,
                                   ", Handle=",
                                   handle,
                                   ", Addr=",
                                   (void*)addr,
                                   ", Len=",
                                   length,
                                   ", RKey=",
                                   rkey);
                }
                else {
                    SLIME_LOG_INFO("Remote MR Name ", name, " already registered (Handle: ", handle, ")");
                }
                return handle;
            }
            SLIME_LOG_WARN("Remote MR Name ", name, " has stale handle ", handle, "; re-registering");
        }

        int32_t handle = handle_to_mr_.size();
        handle_to_mr_.push_back(mr);
        name_to_handle_[name] = handle;

        SLIME_LOG_INFO("Registered Remote MR: Name=",
                       name,
                       ", Handle=",
                       handle,
                       ", Addr=",
                       (void*)addr,
                       ", Len=",
                       length,
                       ", RKey=",
                       rkey);
        return handle;
    }

    int32_t registerRemoteMemoryRegion(uintptr_t addr, size_t length, uint32_t rkey)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        remote_mr_t                  mr(addr, length, rkey);
        int32_t                      handle = handle_to_mr_.size();
        handle_to_mr_.push_back(mr);
        SLIME_LOG_INFO(
            "Registered Raw Remote MR: Handle=", handle, ", Addr=", (void*)addr, ", Len=", length, ", RKey=", rkey);
        return handle;
    }

    // Get Remote MR by Handle (Fast Path)
    inline remote_mr_t get_remote_mr_fast(int32_t handle)
    {
        if (handle >= 0 && handle < handle_to_mr_.size()) {
            return handle_to_mr_[handle];
        }
        return remote_mr_t();
    }

    int32_t get_mr_handle(const std::string& name)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (name_to_handle_.count(name)) {
            SLIME_LOG_INFO("Lookup Remote MR Name=", name, " -> Handle=", name_to_handle_[name]);
            return name_to_handle_[name];
        }
        SLIME_LOG_WARN("Lookup Remote MR Name=", name, " FAILED");
        return -1;
    }

private:
    std::mutex                               mutex_;
    std::unordered_map<std::string, int32_t> name_to_handle_;
    std::vector<remote_mr_t>                 handle_to_mr_;
};

}  // namespace dlslime
