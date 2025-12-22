#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <infiniband/verbs.h>

#include "engine/assignment.h"

#include "memory_pool.h"
#include "rdma_assignment.h"
#include "rdma_config.h"
#include "rdma_env.h"

#include "jring.h"
#include "json.hpp"
#include "logging.h"

namespace slime {

class RDMAChannel;

using json = nlohmann::json;

class RDMAContext: public std::enable_shared_from_this<RDMAContext> {

    friend class RDMAChannel;
    friend class RDMAEndpoint;
    friend class RDMAMsgEndpoint;
    friend class RDMAIOEndpoint;

public:
    /*
      A context of rdma QP.
    */

    RDMAContext() = default;

    ~RDMAContext();

    struct ibv_mr* get_mr(const uintptr_t& mr_key)
    {
        return memory_pool_->get_mr(mr_key);
    }

    remote_mr_t get_remote_mr(const uintptr_t& mr_key)
    {
        return memory_pool_->get_remote_mr(mr_key);
    }

    /* Initialize */
    int64_t init(const std::string& dev_name, uint8_t ib_port, const std::string& link_type);

    /* Memory Allocation */
    inline int64_t registerOrAccessMemoryRegion(const uintptr_t& mr_key, uintptr_t data_ptr, size_t length)
    {
        memory_pool_->registerMemoryRegion(mr_key, data_ptr, length);
        return 0;
    }

    inline int registerOrAccessRemoteMemoryRegion(const uintptr_t& mr_key, uintptr_t addr, size_t length, uint32_t rkey)
    {
        memory_pool_->registerRemoteMemoryRegion(mr_key, addr, length, rkey);
        return 0;
    }

    inline int64_t registerOrAccessRemoteMemoryRegion(const uintptr_t& mr_key, json mr_info)
    {
        memory_pool_->registerRemoteMemoryRegion(mr_key, mr_info);
        return 0;
    }

    inline int64_t unregisterMemoryRegion(const uintptr_t& mr_key)
    {
        memory_pool_->unregisterMemoryRegion(mr_key);
        return 0;
    }

    int64_t reloadMemoryPool()
    {
        memory_pool_ = std::make_unique<RDMAMemoryPool>(pd_);
        return 0;
    }

    void launch_future();
    void stop_future();

    std::string get_dev_ib() const
    {
        return "@" + device_name_ + "#" + std::to_string(ib_port_);
    }

    bool validate_assignment()
    {
        // TODO: validate if the assignment is valid
        return true;
    }

private:
    inline static constexpr int      UNDEFINED_QPI            = -1;
    inline static constexpr uint32_t UNDEFINED_IMM_DATA       = -1;
    inline static constexpr uint32_t BACKPRESSURE_BUFFER_SIZE = 8192;

    std::string device_name_ = "";

    /* RDMA Configuration */
    struct ibv_context*      ib_ctx_       = nullptr;
    struct ibv_pd*           pd_           = nullptr;
    struct ibv_comp_channel* comp_channel_ = nullptr;
    struct ibv_cq*           cq_           = nullptr;
    uint8_t                  ib_port_      = -1;
    size_t                   max_num_inline_data_{0};
    uint16_t                 lid_;
    enum ibv_mtu             active_mtu_;
    union ibv_gid            gid_;
    int64_t                  gidx_;

    std::unique_ptr<RDMAMemoryPool> memory_pool_;

    int32_t num_qp_;
    int32_t last_qp_selection_{-1};

    std::vector<int> select_qpi(int num)
    {
        std::vector<int> agg_qpi;
        // Simplest round robin, we could enrich it in the future

        for (int i = 0; i < num; ++i) {
            last_qp_selection_ = (last_qp_selection_ + 1) % num_qp_;
            agg_qpi.push_back(last_qp_selection_);
        }

        return agg_qpi;
    }

    typedef struct cq_management {
        // TODO: multi cq handlers.
    } cq_management_t;

    /* async cq handler */
    std::thread       cq_thread_;
    std::atomic<bool> stop_cq_thread_{false};

    /* Completion Queue Polling */
    int64_t cq_poll_handle();

    int64_t service_level_{0};
};

}  // namespace slime
