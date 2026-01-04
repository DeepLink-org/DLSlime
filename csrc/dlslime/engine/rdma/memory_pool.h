#pragma once

#include <infiniband/verbs.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <unordered_map>

#include "dlslime/engine/rdma/rdma_config.h"
#include "dlslime/engine/rdma/rdma_context.h"
#include "dlslime/json.hpp"
#include "dlslime/logging.h"

namespace dlslime {

using json = nlohmann::json;

typedef struct remote_mr {
    remote_mr() = default;
    remote_mr(uintptr_t addr, size_t length, uint32_t rkey): addr(addr), length(length), rkey(rkey) {}

    uintptr_t addr{(uintptr_t) nullptr};
    size_t    length{0};
    uint32_t  rkey{0};
} remote_mr_t;

class RDMAMemoryPool {
    friend class RDMAChannel;

public:
    RDMAMemoryPool(std::shared_ptr<RDMAContext> ctx)
    {
        SLIME_LOG_DEBUG("init memory Pool");
        /* Alloc Protected Domain (PD) */
        pd_ = ibv_alloc_pd(ctx->ib_ctx_);
        if (!pd_) {
            SLIME_LOG_ERROR("Failed to allocate PD");
        }
    };

    ~RDMAMemoryPool()
    {
        for (auto& mr : mrs_) {
            if (mr.second)
                ibv_dereg_mr(mr.second);
        }
        mrs_.clear();
        if (pd_)
            ibv_dealloc_pd(pd_);
    }

    int registerMemoryRegion(const uintptr_t& mr_key, uintptr_t data_ptr, uint64_t length);

    int unregisterMemoryRegion(const uintptr_t& mr_key);

    int registerRemoteMemoryRegion(const uintptr_t& mr_key, uintptr_t addr, size_t length, uint32_t rkey);

    int registerRemoteMemoryRegion(const uintptr_t& mr_key, const json& mr_info);

    int unregisterRemoteMemoryRegion(const uintptr_t& mr_key);

    inline struct ibv_mr* get_mr(const uintptr_t& mr_key)
    {
        std::unique_lock<std::mutex> lock(mrs_mutex_);
        if (mrs_.find(mr_key) != mrs_.end()) {
            return mrs_[mr_key];
        }
        SLIME_LOG_DEBUG("mr_key: ", mr_key, " not found in mrs_");
        return nullptr;
    }

    inline remote_mr_t get_remote_mr(const uintptr_t& mr_key)
    {
        std::unique_lock<std::mutex> lock(remote_mrs_mutex_);
        if (remote_mrs_.find(mr_key) != remote_mrs_.end()) {
            return remote_mrs_[mr_key];
        }
        SLIME_LOG_DEBUG("mr_key: ", mr_key, " not found in remote_mrs_");
        return remote_mr_t();
    }

    json mr_info();
    json remote_mr_info();

private:
    ibv_pd* pd_;

    std::mutex mrs_mutex_;
    std::mutex remote_mrs_mutex_;

    std::unordered_map<uintptr_t, struct ibv_mr*> mrs_;
    std::unordered_map<uintptr_t, remote_mr_t>    remote_mrs_;
};
}  // namespace dlslime
