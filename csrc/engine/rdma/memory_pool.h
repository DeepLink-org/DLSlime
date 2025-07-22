#pragma once

#include "engine/rdma/rdma_config.h"

#include "utils/json.hpp"
#include "utils/logging.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <infiniband/verbs.h>
#include <string>
#include <sys/types.h>
#include <unordered_map>

namespace slime {

using json = nlohmann::json;

typedef struct remote_mr {
    remote_mr() = default;
    remote_mr(uintptr_t addr, size_t length, uint32_t rkey): addr(addr), length(length), rkey(rkey) {}

    uintptr_t addr{(uintptr_t) nullptr};
    size_t    length{};
    uint32_t  rkey{};
} remote_mr_t;

class RDMAMemoryPool {
public:
    RDMAMemoryPool() = default;
    RDMAMemoryPool(ibv_pd* pd): pd_(pd) {}

    ~RDMAMemoryPool()
    {
        for (auto& mr : mrs_) {
            if (mr.second)
                ibv_dereg_mr(mr.second);
        }
        mrs_.clear();
    }

    int register_memory_region(const std::string& mr_key, uintptr_t data_ptr, uint64_t length);
    int unregister_memory_region(const std::string& mr_key);

    int register_remote_memory_region(const std::string& mr_key, uintptr_t addr, size_t length, uint32_t rkey);
    int register_remote_memory_region(const std::string& mr_key, const json& mr_info);
    int unregister_remote_memory_region(const std::string& mr_key);

    inline struct ibv_mr* get_mr(const std::string& mr_key)
    {
        if (mrs_.find(mr_key) != mrs_.end())
            return mrs_[mr_key];
        SLIME_LOG_ERROR("mr_key: ", mr_key, "not found in mrs_");
        return nullptr;
    }
    inline remote_mr_t get_remote_mr(const std::string& mr_key)
    {
        if (remote_mrs_.find(mr_key) != remote_mrs_.end())
            return remote_mrs_[mr_key];
        SLIME_LOG_ERROR("mr_key: ", mr_key, " not found in remote_mrs_");
        return remote_mr_t();
    }

    json mr_info() const;
    json remote_mr_info() const;

private:
    ibv_pd*                                         pd_;
    std::unordered_map<std::string, struct ibv_mr*> mrs_;
    std::unordered_map<std::string, remote_mr_t>    remote_mrs_;
};
}  // namespace slime
