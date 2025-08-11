#pragma once

#include "engine/assignment.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_env.h"

#include "utils/json.hpp"
#include "utils/logging.h"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <infiniband/verbs.h>

namespace slime {

using json = nlohmann::json;

class RDMAContext {

    friend class RDMAEndpoint;  // RDMA Endpoint need to use the register memory pool in context

public:
    /*
      A context of rdma QP.
    */
    RDMAContext(): RDMAContext(0) {}

    RDMAContext(size_t qp_num): RDMAContext(qp_num, false) {}

    RDMAContext(size_t qp_num, bool diable_submit): disable_submit_(diable_submit)
    {
        if (!qp_num)
            qp_num = SLIME_QP_NUM;
        SLIME_LOG_DEBUG("Initializing qp management, num qp: " << qp_num);

        qp_list_len_   = qp_num;
        qp_management_ = new qp_management_t*[qp_list_len_];
        for (int qpi = 0; qpi < qp_list_len_; qpi++) {
            qp_management_[qpi]                  = new qp_management_t();
            qp_management_[qpi]->disable_submit_ = diable_submit;
        }

        cq_list_len_   = SLIME_CQ_NUM;
        cq_management_ = new cq_management_t*[cq_list_len_];
        for (int qpi = 0; qpi < cq_list_len_; qpi++) {
            cq_management_[qpi] = new cq_management_t();
        }

        /* random initialization for psn configuration */
        srand48(time(NULL));
    }

    ~RDMAContext()
    {
        stop_future();
        for (int qpi = 0; qpi < qp_list_len_; qpi++) {
            delete qp_management_[qpi];
        }
        delete[] qp_management_;

        for (int qpi = 0; qpi < cq_list_len_; qpi++) {
            delete cq_management_[qpi];
        }
        delete[] cq_management_;

        if (pd_)
            ibv_dealloc_pd(pd_);

        if (ib_ctx_)
            ibv_close_device(ib_ctx_);

        SLIME_LOG_DEBUG("RDMAContext deconstructed")
    }

    struct ibv_mr* get_mr(const std::string& mr_key)
    {
        return memory_pool_->get_mr(mr_key);
    }

    /* Initialize */
    int64_t init(const std::string& dev_name, uint8_t ib_port, const std::string& link_type);

    /* Memory Allocation */
    inline int64_t register_memory_region(std::string mr_key, uintptr_t data_ptr, size_t length)
    {
        memory_pool_->register_memory_region(mr_key, data_ptr, length);
        return 0;
    }

    inline int register_remote_memory_region(const std::string& mr_key, uintptr_t addr, size_t length, uint32_t rkey)
    {
        memory_pool_->register_remote_memory_region(mr_key, addr, length, rkey);
        return 0;
    }

    inline int64_t register_remote_memory_region(std::string mr_key, json mr_info)
    {
        memory_pool_->register_remote_memory_region(mr_key, mr_info);
        return 0;
    }

    inline int64_t unregister_memory_region(std::string mr_key)
    {
        memory_pool_->unregister_memory_region(mr_key);
        return 0;
    }

    int64_t reload_memory_pool()
    {
        memory_pool_ = std::make_unique<RDMAMemoryPool>(pd_);
        return 0;
    }

    bool is_disable_submit()
    {
        return disable_submit_;
    }

    /* RDMA Link Construction */
    int64_t connect(const json& endpoint_info_json);
    /* Submit an assignment */
    RDMAAssignmentSharedPtr submit(OpCode           opcode,
                                   AssignmentBatch& assignment,
                                   callback_fn_t    callback = nullptr,
                                   int              qpi      = UNDEFINED_QPI,
                                   int32_t          imm_data = UNDEFINED_IMM_DATA);

    void launch_future();
    void stop_future();

    json local_rdma_info() const
    {
        json local_info{};
        for (int qpi = 0; qpi < qp_list_len_; qpi++)
            local_info[qpi] = qp_management_[qpi]->local_rdma_info_.to_json();
        return local_info;
    }

    json remote_rdma_info() const
    {
        json remote_info{};
        for (int qpi = 0; qpi < qp_list_len_; qpi++)
            remote_info[qpi] = qp_management_[qpi]->remote_rdma_info_.to_json();
        return remote_info;
    }

    json endpoint_info() const
    {
        json endpoint_info = json{{"rdma_info", local_rdma_info()}, {"mr_info", memory_pool_->mr_info()}};
        return endpoint_info;
    }

    std::string get_dev_ib() const
    {
        return "@" + device_name_ + "#" + std::to_string(ib_port_);
    }

    bool validate_assignment()
    {
        // TODO: validate if the assignment is valid
        return true;
    }

    /* Async RDMA SendRecv */
    int64_t post_send_batch(int qpi, RDMAAssignmentSharedPtr assign);
    int64_t post_recv_batch(int qpi, RDMAAssignmentSharedPtr assign);

    /* Async RDMA Read */
    int64_t post_rc_oneside_batch(int qpi, RDMAAssignmentSharedPtr assign);

private:
    inline static constexpr int      UNDEFINED_QPI      = -1;
    inline static constexpr uint32_t UNDEFINED_IMM_DATA = -1;

    std::string device_name_ = "";

    /* RDMA Configuration */
    struct ibv_context* ib_ctx_  = nullptr;
    struct ibv_pd*      pd_      = nullptr;
    uint8_t             ib_port_ = -1;

    std::unique_ptr<RDMAMemoryPool> memory_pool_;

    typedef struct qp_management {
        /* queue peer list */
        struct ibv_qp* qp_{nullptr};

        /* RDMA Exchange Information */
        rdma_info_t remote_rdma_info_;
        rdma_info_t local_rdma_info_;

        /* Send Mutex */
        std::mutex rdma_post_send_mutex_;

        /* Assignment Queue */
        std::mutex                          assign_queue_mutex_;
        std::queue<RDMAAssignmentSharedPtr> assign_queue_;
        std::atomic<int>                    outstanding_rdma_reads_{0};

        bool disable_submit_{false};

        /* Has Runnable Assignment */
        std::condition_variable has_runnable_event_;

        /* async wq handler */
        std::thread       wq_future_;
        std::atomic<bool> stop_wq_future_{false};

        ~qp_management()
        {
            if (qp_)
                ibv_destroy_qp(qp_);
        }
    } qp_management_t;

    size_t            qp_list_len_{1};
    qp_management_t** qp_management_;

    int last_qp_selection_{-1};
    int select_qpi()
    {
        // Simplest round robin, we could enrich it in the future
        last_qp_selection_ = (last_qp_selection_ + 1) % qp_list_len_;
        return last_qp_selection_;
    }

    typedef struct cq_management {
        struct ibv_cq* cq_ = nullptr;
        /* async cq handler */
        std::thread              cq_future_;
        std::atomic<bool>        stop_cq_future_{false};
        struct ibv_comp_channel* comp_channel_ = nullptr;

        bool initialized_ = false;

        ~cq_management()
        {
            if (cq_)
                ibv_destroy_cq(cq_);
        }
    } cq_management_t;

    size_t            cq_list_len_{1};
    cq_management_t** cq_management_;

    /* Use raw api of RDMA */
    bool disable_submit_{false};

    /* State Management */
    bool initialized_{false};
    bool connected_{false};

    /* Completion Queue Polling */
    int64_t cq_poll_handle(int qpi);
    /* Working Queue Dispatch */
    int64_t wq_dispatch_handle(int qpi);
};

}  // namespace slime
