#pragma once

#include "engine/assignment.h"
#include "engine/rdma/affinity.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_env.h"

#include "jring.h"
#include "json.hpp"
#include "logging.h"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <infiniband/verbs.h>

namespace slime {

using json = nlohmann::json;

class RDMAContext {

    friend class RDMAEndpoint;  // RDMA Endpoint need to use the register memory pool in context
    friend class RDMAEndpointV0;
    friend class RDMAWorker;

public:
    /*
      A context of rdma QP.
    */

    RDMAContext(size_t max_num_inline_data = 0)
    {
        max_num_inline_data_ = max_num_inline_data;

        /* random initialization for psn configuration */
        srand48(time(NULL));
    }

    ~RDMAContext()
    {
        stop_future();

        if (cq_)
            ibv_destroy_cq(cq_);

        if (pd_)
            ibv_dealloc_pd(pd_);

        if (ib_ctx_)
            ibv_close_device(ib_ctx_);

        SLIME_LOG_DEBUG("RDMAContext deconstructed")
    }

    struct ibv_mr* get_mr(const uintptr_t& mr_key)
    {
        return memory_pool_->get_mr(mr_key);
    }

    ibv_cq* get_cq()
    {
        return cq_;
    }

    remote_mr_t get_remote_mr(const uintptr_t& mr_key)
    {
        return memory_pool_->get_remote_mr(mr_key);
    }

    /* Initialize */
    int64_t init(const std::string& dev_name, uint8_t ib_port, const std::string& link_type);

    /* Memory Allocation */
    inline int64_t registerMemoryRegion(const uintptr_t& mr_key, uintptr_t data_ptr, size_t length)
    {
        memory_pool_->registerMemoryRegion(mr_key, data_ptr, length);
        return 0;
    }

    inline int registerRemoteMemoryRegion(const uintptr_t& mr_key, uintptr_t addr, size_t length, uint32_t rkey)
    {
        memory_pool_->registerRemoteMemoryRegion(mr_key, addr, length, rkey);
        return 0;
    }

    inline int64_t registerRemoteMemoryRegion(const uintptr_t& mr_key, json mr_info)
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

    json local_rdma_info() const
    {
        json local_info{};
        return local_info;
    }

    json remote_rdma_info() const
    {
        json remote_info{};
        return remote_info;
    }

    json endpoint_info() const
    {
        json endpoint_info = json{{"rdma_info", local_rdma_info()}, {"mr_info", memory_pool_->mr_info()}};
        return endpoint_info;
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

    uint32_t      psn_;
    enum ibv_mtu  active_mtu_;
    uint16_t      lid_;
    union ibv_gid gid_;
    int32_t       gidx_;

    size_t max_num_inline_data_{0};

    std::unique_ptr<RDMAMemoryPool> memory_pool_;

    typedef struct cq_management {
        // TODO: multi cq handlers.
    } cq_management_t;

    /* State Management */
    bool initialized_ = false;
    bool connected_   = false;

    /* async cq handler */
    std::thread       cq_thread_;
    std::atomic<bool> stop_cq_thread_{false};

    /* Completion Queue Polling */
    int64_t cq_poll_handle();

    int64_t service_level_{0};
};

}  // namespace slime
