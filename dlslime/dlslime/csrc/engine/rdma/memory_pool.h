#pragma once

#include <infiniband/verbs.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/engine/rdma/rdma_config.h"
#include "dlslime/csrc/engine/rdma/rdma_context.h"
#include "dlslime/csrc/logging.h"

namespace dlslime {

using json = nlohmann::json;

// RDMAMemoryPool handles Local Memory Region management.
class RDMAMemoryPool {
    friend class RDMAChannel;

public:
    RDMAMemoryPool(std::shared_ptr<RDMAContext> ctx): ctx_(ctx), owns_pd_(true)
    {
        SLIME_LOG_DEBUG("init memory Pool");
        /* Alloc Protected Domain (PD) */
        pd_ = ibv_alloc_pd(ctx->ib_ctx_);
        if (!pd_) {
            SLIME_LOG_ERROR("Failed to allocate PD");
        }
    }

    // Borrow PD from parent pool (same PD, no allocation)
    RDMAMemoryPool(std::shared_ptr<RDMAMemoryPool> parent_pool):
        pd_(parent_pool->pd_), ctx_(parent_pool->ctx_), owns_pd_(false)
    {
        SLIME_LOG_DEBUG("init meta memory Pool (borrowed PD)");
    }

    std::shared_ptr<RDMAContext> context() const
    {
        return ctx_;
    }

    ~RDMAMemoryPool()
    {
        for (auto& mr : mrs_) {
            if (mr.second)
                ibv_dereg_mr(mr.second);
        }
        mrs_.clear();

        for (auto* mr : handle_to_mr_) {
            if (mr)
                ibv_dereg_mr(mr);
        }
        handle_to_mr_.clear();
        ptr_to_handle_.clear();
        name_to_handle_.clear();

        if (pd_ && owns_pd_)
            ibv_dealloc_pd(pd_);
    }

    // Register a memory region with a name (Slow Path)
    // Return: handle (int32_t) to be used in fast path
    // Unified Registration (Fast Path)
    // Returns: handle (int32_t)
    // Unified Registration (Fast Path)
    // Returns: handle (int32_t)
    int32_t registerMemoryRegion(uintptr_t data_ptr, uint64_t length, std::optional<std::string> name = std::nullopt);

    int32_t get_mr_handle(const std::string& name);
    int32_t get_mr_handle(uintptr_t data_ptr);

    // Fast path: get MR by handle
    inline struct ibv_mr* get_mr_fast(int32_t handle)
    {
        if (handle >= 0 && handle < handle_to_mr_.size()) {
            return handle_to_mr_[handle];
        }
        return nullptr;
    }

    // Legacy method: get_mr by pointer key (slow check in a map or we can deprecate it)
    // We retain it for existing code compatibility, but we should probably encourage handle usage.
    // However, existing code uses `get_mr` heavily. We will keep `mrs_` map for now.
    inline struct ibv_mr* get_mr(const uintptr_t& mr_key)
    {
        {
            std::unique_lock<std::mutex> lock(mrs_mutex_);
            if (mrs_.find(mr_key) != mrs_.end()) {
                return mrs_[mr_key];
            }
        }

        // Fallback to new logic: ptr -> handle -> mr
        {
            std::unique_lock<std::mutex> lock(name_mutex_);
            if (ptr_to_handle_.count(mr_key)) {
                int32_t handle = ptr_to_handle_[mr_key];
                if (handle >= 0 && handle < handle_to_mr_.size()) {
                    return handle_to_mr_[handle];
                }
            }
        }

        SLIME_LOG_DEBUG("mr_key: ", mr_key, " not found in mrs_ or by handle");
        return nullptr;
    }

    int unregisterMemoryRegion(const uintptr_t& mr_key);
    int unregisterMemoryRegion(int32_t handle);
    int unregisterMemoryRegion(const std::string& name);

    json mrInfo();

private:
    ibv_pd*                      pd_;
    std::shared_ptr<RDMAContext> ctx_;
    bool                         owns_pd_;

    std::mutex mrs_mutex_;

    // Legacy map: Key -> MR
    std::unordered_map<uintptr_t, struct ibv_mr*> mrs_;

    // New: Name -> Handle
    std::mutex                               name_mutex_;  // Maps
    std::unordered_map<std::string, int32_t> name_to_handle_;
    std::unordered_map<uintptr_t, int32_t>   ptr_to_handle_;

    std::vector<struct ibv_mr*> handle_to_mr_;

    // Observability: remember whether each registered MR is a system
    // (internal "sys.*") MR or a user-visible MR. Looked up on unregister
    // so user/sys counters don't drift.
    std::unordered_map<int32_t, bool> handle_to_is_system_;
};
}  // namespace dlslime
