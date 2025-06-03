#pragma once

#include "engine/assignment.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_env.h"
#include "engine/rdma/rdma_config.h"

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
public:
    /*
      A context of rdma QP.
    */
    RDMAContext()
    {
        SLIME_LOG_DEBUG("Initializing qp management, num qp: " << SLIME_QP_NUM);

        qp_list_len_ = SLIME_QP_NUM;
        qp_management_ = new qp_management_t*[qp_list_len_];
        for (int qpi = 0; qpi < qp_list_len_; qpi++) {
            qp_management_[qpi] = new qp_management_t();
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

        if (cq_)
            ibv_destroy_cq(cq_);

        if (pd_)
            ibv_dealloc_pd(pd_);

        if (ib_ctx_)
            ibv_close_device(ib_ctx_);

        SLIME_LOG_DEBUG("RDMAContext deconstructed")
    }

    /* Initialize */
    int64_t init(const std::string& dev_name, uint8_t ib_port, const std::string& link_type);

    /* Memory Allocation */
    int64_t register_memory_region(std::string mr_key, uintptr_t data_ptr, size_t length)
    {
        memory_pool_->register_memory_region(mr_key, data_ptr, length);
        return 0;
    }

    int64_t register_remote_memory_region(std::string mr_key, json mr_info)
    {
        memory_pool_->register_remote_memory_region(mr_key, mr_info);
        return 0;
    }

    /* RDMA Link Construction */
    int64_t connect(const json& endpoint_info_json);

    /* Submit an assignment */
    RDMAAssignmentSharedPtr submit(OpCode opcode, AssignmentBatch& assignment, callback_fn_t callback = nullptr);

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
        json endpoint_info =  json{{"rdma_info", local_rdma_info()}, {"mr_info", memory_pool_->mr_info()}};
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

private:
    std::string device_name_ = "";

    /* RDMA Configuration */
    struct ibv_context*      ib_ctx_       = nullptr;
    struct ibv_pd*           pd_           = nullptr;
    struct ibv_comp_channel* comp_channel_ = nullptr;
    struct ibv_cq*           cq_           = nullptr;
    uint8_t                  ib_port_      = -1;

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

        /* Has Runnable Assignment */
        std::condition_variable has_runnable_event_;

        /* async wq handler */
        std::future<void> wq_future_;
        std::atomic<bool> stop_wq_future_{false};

        ~qp_management() {
            if (qp_)
                ibv_destroy_qp(qp_);
        }
    } qp_management_t;

    size_t            qp_list_len_{1};
    qp_management_t** qp_management_;

    int last_qp_selection_ = -1;
    int select_qpi()
    {
        // Simplest round robin, we could enrich it in the future
        last_qp_selection_ = (last_qp_selection_ + 1) % qp_list_len_;
        return last_qp_selection_;
    }

    typedef struct cq_management {
        // TODO: multi cq handlers.
    } cq_management_t;

    /* State Management */
    bool initialized_ = false;
    bool connected_   = false;

    /* async cq handler */
    std::future<void> cq_future_;
    std::atomic<bool> stop_cq_future_{false};

    /* Completion Queue Polling */
    int64_t cq_poll_handle();
    /* Working Queue Dispatch */
    int64_t wq_dispatch_handle(int qpi);

    /* Async RDMA SendRecv */
    int64_t post_send_batch(int qpi, RDMAAssignmentSharedPtr assign);
    int64_t post_recv_batch(int qpi, RDMAAssignmentSharedPtr assign);

    /* Async RDMA Read */
    int64_t post_rc_oneside_batch(int qpi, RDMAAssignmentSharedPtr assign);

};

}  // namespace slime
