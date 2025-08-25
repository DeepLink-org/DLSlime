#include "engine/rdma/rdma_context.h"
#include "engine/assignment.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_env.h"

#include "utils/ibv_helper.h"
#include "utils/logging.h"
#include "utils/utils.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <infiniband/verbs.h>
#include <stdexcept>

namespace slime {

typedef struct callback_info_with_qpi {
    typedef enum: int {
        SUCCESS                   = 0,
        ASSIGNMENT_BATCH_OVERFLOW = 400,
        UNKNOWN_OPCODE            = 401,
        TIME_OUT                  = 402,
        FAILED                    = 403,
    } CALLBACK_STATUS;

    std::shared_ptr<callback_info_t> callback_info_;
    int                              qpi_;
} callback_info_with_qpi_t;

int64_t RDMAContext::init(const std::string& dev_name, uint8_t ib_port, const std::string& link_type)
{
    device_name_ = dev_name;
    uint16_t      lid;
    enum ibv_mtu  active_mtu;
    union ibv_gid gid;
    int64_t       gidx;
    uint32_t      psn;

    SLIME_LOG_INFO("Initializing RDMA Context ...");
    SLIME_LOG_DEBUG("device name: " << dev_name);
    SLIME_LOG_DEBUG("ib port: " << int{ib_port});
    SLIME_LOG_DEBUG("link type: " << link_type);

    if (initialized_) {
        SLIME_LOG_ERROR("Already initialized.");
        return -1;
    }

    /* Get RDMA Device Info */
    struct ibv_device** dev_list;
    struct ibv_device*  ib_dev;
    int                 num_devices;
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        SLIME_LOG_ERROR("Failed to get RDMA devices list");
        return -1;
    }

    if (!num_devices) {
        SLIME_LOG_ERROR("No RDMA devices found.")
        return -1;
    }

    for (int i = 0; i < num_devices; ++i) {
        char* dev_name_from_list = (char*)ibv_get_device_name(dev_list[i]);
        if (strcmp(dev_name_from_list, dev_name.c_str()) == 0) {
            SLIME_LOG_INFO("found device " << dev_name_from_list);
            ib_dev  = dev_list[i];
            ib_ctx_ = ibv_open_device(ib_dev);
            break;
        }
    }

    if (!ib_ctx_ && num_devices > 0) {
        SLIME_LOG_WARN("Can't find or failed to open the specified device ",
                       dev_name,
                       ", try to open "
                       "the default device ",
                       (char*)ibv_get_device_name(dev_list[0]));
        ib_ctx_ = ibv_open_device(dev_list[0]);
    }

    if (!ib_ctx_) {
        SLIME_ABORT("Failed to open the default device");
    }

    struct ibv_device_attr device_attr;
    if (ibv_query_device(ib_ctx_, &device_attr) != 0)
        SLIME_LOG_ERROR("Failed to query device");

    SLIME_LOG_DEBUG("Max Memory Region:" << device_attr.max_mr);
    SLIME_LOG_DEBUG("Max Memory Region Size:" << device_attr.max_mr_size);
    SLIME_LOG_DEBUG("Max QP:" << device_attr.max_qp);
    SLIME_LOG_DEBUG("Max QP Working Request: " << device_attr.max_qp_wr);
    SLIME_LOG_DEBUG("Max CQ: " << int{device_attr.max_cq});
    SLIME_LOG_DEBUG("Max CQ Element: " << int{device_attr.max_cqe});
    SLIME_LOG_DEBUG("MAX QP RD ATOM: " << int{device_attr.max_qp_init_rd_atom});
    SLIME_LOG_DEBUG("MAX RES RD ATOM: " << int{device_attr.max_res_rd_atom});
    SLIME_LOG_DEBUG("Total ib ports: " << int{device_attr.phys_port_cnt});

    if (SLIME_MAX_RD_ATOMIC > int{device_attr.max_qp_init_rd_atom})
        SLIME_ABORT("MAX_RD_ATOMIC (" << SLIME_MAX_RD_ATOMIC << ") > device max RD ATOMIC ("
                                      << device_attr.max_qp_init_rd_atom << "), please set SLIME_MAX_RD_ATOMIC env "
                                      << "less than device max RD ATOMIC");

    struct ibv_port_attr port_attr;
    ib_port_ = ib_port;

    if (ibv_query_port(ib_ctx_, ib_port, &port_attr)) {
        ibv_close_device(ib_ctx_);
        SLIME_ABORT("Unable to query port " + std::to_string(ib_port_) + "\n");
    }

    if ((port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND && link_type == "RoCE")
        || (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET && link_type == "IB")) {
        SLIME_ABORT("port link layer and config link type don't match");
    }

    if (port_attr.state == IBV_PORT_DOWN) {
        ibv_close_device(ib_ctx_);
        SLIME_ABORT("Device " << dev_name << ", Port " << int{ib_port_} << "is DISABLED.");
    }

    if (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
        gidx = -1;
    }
    else {
        if (SLIME_GID_INDEX > 0)
            gidx = SLIME_GID_INDEX;
        else
            gidx = ibv_find_sgid_type(ib_ctx_, ib_port_, ibv_gid_type_custom::IBV_GID_TYPE_ROCE_V2, AF_INET);
        if (gidx < 0) {
            SLIME_ABORT("Failed to find GID");
        }
    }

    SLIME_LOG_DEBUG("Set GID INDEX to " << gidx);

    lid        = port_attr.lid;
    active_mtu = port_attr.active_mtu;

    /* Alloc Protected Domain (PD) */
    pd_ = ibv_alloc_pd(ib_ctx_);
    if (!pd_) {
        SLIME_LOG_ERROR("Failed to allocate PD");
        return -1;
    }
    memory_pool_ = std::make_unique<RDMAMemoryPool>(pd_);

    /* Alloc Complete Queue (CQ) */
    SLIME_ASSERT(ib_ctx_, "init rdma context first");
    comp_channel_ = ibv_create_comp_channel(ib_ctx_);
    cq_           = ibv_create_cq(ib_ctx_, SLIME_MAX_CQ_DEPTH, NULL, comp_channel_, 0);
    SLIME_ASSERT(cq_, "create CQ failed");

    for (int qpi = 0; qpi < qp_list_len_; ++qpi) {
        /* Create Queue Pair (QP) */
        struct ibv_qp_init_attr qp_init_attr = {};
        qp_init_attr.send_cq                 = cq_;
        qp_init_attr.recv_cq                 = cq_;
        qp_init_attr.qp_type                 = IBV_QPT_RC;  // Reliable Connection
        qp_init_attr.cap.max_send_wr         = SLIME_MAX_SEND_WR;
        qp_init_attr.cap.max_recv_wr         = SLIME_MAX_RECV_WR;
        qp_init_attr.cap.max_send_sge        = 1;
        qp_init_attr.cap.max_recv_sge        = 1;
        qp_init_attr.sq_sig_all              = false;
        qp_management_t* qp_man              = qp_management_[qpi];
        rdma_info_t&     local_rdma_info     = qp_man->local_rdma_info_;
        qp_man->qp_                          = ibv_create_qp(pd_, &qp_init_attr);
        if (!qp_man->qp_) {
            SLIME_LOG_ERROR("Failed to create QP " << qp_man->qp_->qp_num);
            return -1;
        }

        /* Modify QP to INIT state */
        struct ibv_qp_attr attr = {};
        attr.qp_state           = IBV_QPS_INIT;
        attr.port_num           = ib_port_;
        attr.pkey_index         = 0;
        attr.qp_access_flags    = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

        int ret = ibv_modify_qp(qp_man->qp_, &attr, flags);
        if (ret) {
            SLIME_LOG_ERROR("Failed to modify QP to INIT");
        }

        /* Set Packet Sequence Number (PSN) */
        psn = lrand48() & 0xffffff;

        /* Get GID */
        if (gidx != -1 && ibv_query_gid(ib_ctx_, 1, gidx, &gid)) {
            SLIME_LOG_ERROR("Failed to get GID");
        }

        /* Set Local RDMA Info */
        local_rdma_info.gidx = gidx;
        local_rdma_info.qpn  = qp_man->qp_->qp_num;
        local_rdma_info.psn  = psn;
        local_rdma_info.gid  = gid;
        local_rdma_info.lid  = lid;
        local_rdma_info.mtu  = (uint32_t)active_mtu;
    }
    SLIME_LOG_INFO("RDMA context initialized")
    SLIME_LOG_DEBUG("RDMA context local configuration: ", endpoint_info());

    initialized_ = true;

    return 0;
}

int64_t RDMAContext::connect(const json& endpoint_info_json)
{
    SLIME_LOG_INFO("RDMA context remote connecting");
    SLIME_LOG_DEBUG("RDMA context remote configuration: ", endpoint_info_json);
    // Register Remote Memory Region
    for (auto& item : endpoint_info_json["mr_info"].items()) {
        register_remote_memory_region(item.key(), item.value());
    }
    std::cout << "connectconnectconnect" << std::endl;
    SLIME_ASSERT(!connected_, "Already connected!");
    SLIME_ASSERT_EQ(qp_list_len_, endpoint_info_json["rdma_info"].size(), "Peer must have same QP Size.");

    // construct RDMAEndpoint connection
    for (int qpi = 0; qpi < qp_list_len_; qpi++) {
        int                ret;
        struct ibv_qp_attr attr = {};
        int                flags;
        qp_management_t*   qp_man           = qp_management_[qpi];
        struct ibv_qp*     qp               = qp_man->qp_;
        rdma_info_t&       local_rdma_info  = qp_man->local_rdma_info_;
        rdma_info_t&       remote_rdma_info = qp_man->remote_rdma_info_;
        remote_rdma_info                    = rdma_info_t(endpoint_info_json["rdma_info"][qpi]);

        // Modify QP to Ready to Receive (RTR) state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state           = IBV_QPS_RTR;
        attr.path_mtu           = (enum ibv_mtu)std::min((uint32_t)remote_rdma_info.mtu, (uint32_t)local_rdma_info.mtu);
        attr.dest_qp_num        = remote_rdma_info.qpn;
        attr.rq_psn             = remote_rdma_info.psn;
        attr.max_dest_rd_atomic = SLIME_MAX_DEST_RD_ATOMIC;
        attr.min_rnr_timer      = 0x12;
        attr.ah_attr.dlid       = remote_rdma_info.lid;
        attr.ah_attr.sl         = SLIME_SERVICE_LEVEL;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num      = ib_port_;

        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid      = 0;

        if (local_rdma_info.gidx == -1) {
            // IB
            attr.ah_attr.dlid = local_rdma_info.lid;
        }
        else {
            // RoCE v2
            attr.ah_attr.is_global         = 1;
            attr.ah_attr.grh.dgid          = remote_rdma_info.gid;
            attr.ah_attr.grh.sgid_index    = local_rdma_info.gidx;
            attr.ah_attr.grh.hop_limit     = 1;
            attr.ah_attr.grh.flow_label    = 0;
            attr.ah_attr.grh.traffic_class = 0;
        }

        flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC
                | IBV_QP_MIN_RNR_TIMER;

        ret = ibv_modify_qp(qp, &attr, flags);
        if (ret) {
            SLIME_ABORT("Failed to modify QP to RTR: reason: " << strerror(ret));
        }

        // Modify QP to RTS state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state      = IBV_QPS_RTS;
        attr.timeout       = 14;
        attr.retry_cnt     = 7;
        attr.rnr_retry     = 7;
        attr.sq_psn        = local_rdma_info.psn;
        attr.max_rd_atomic = SLIME_MAX_RD_ATOMIC;

        flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN
                | IBV_QP_MAX_QP_RD_ATOMIC;

        ret = ibv_modify_qp(qp, &attr, flags);
        if (ret) {
            SLIME_ABORT("Failed to modify QP to RTS");
        }
        SLIME_LOG_INFO("RDMA exchange done");
        connected_ = true;

        if (ibv_req_notify_cq(cq_, 0)) {
            SLIME_ABORT("Failed to request notify for CQ");
        }
    }
    return 0;
}

void RDMAContext::launch_future()
{
    cq_future_ = std::async(std::launch::async, [this]() -> void { cq_poll_handle(); });
    for (int qpi = 0; qpi < qp_list_len_; qpi++) {
        qp_management_[qpi]->wq_future_ =
            std::async(std::launch::async, [this, qpi]() -> void { wq_dispatch_handle(qpi); });

        std::cout << "launch_futurelaunch_futurelaunch_future" << std::endl;
    }
}

void RDMAContext::stop_future()
{
    // Stop work queue dispatch
    for (int qpi = 0; qpi < qp_list_len_; ++qpi) {
        if (!qp_management_[qpi]->stop_wq_future_ && qp_management_[qpi]->wq_future_.valid()) {
            qp_management_[qpi]->stop_wq_future_ = true;
            qp_management_[qpi]->has_runnable_event_.notify_one();
            qp_management_[qpi]->wq_future_.get();
        }
    }

    if (!stop_cq_future_ && cq_future_.valid()) {
        stop_cq_future_ = true;

        // create fake wr to wake up cq thread
        ibv_req_notify_cq(cq_, 0);
        struct ibv_sge sge;
        memset(&sge, 0, sizeof(sge));
        sge.addr   = (uintptr_t)this;
        sge.length = sizeof(*this);
        sge.lkey   = 0;

        struct ibv_send_wr send_wr;
        memset(&send_wr, 0, sizeof(send_wr));
        // send_wr.wr_id      = (uintptr_t)this;
        send_wr.wr_id      = 0;
        send_wr.sg_list    = &sge;
        send_wr.num_sge    = 1;
        send_wr.opcode     = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED;

        struct ibv_send_wr* bad_send_wr;
        {
            std::unique_lock<std::mutex> lock(qp_management_[0]->rdma_post_send_mutex_);
            ibv_post_send(qp_management_[0]->qp_, &send_wr, &bad_send_wr);
        }
        // wait thread done
        cq_future_.get();
    }
}

RDMAAssignmentSharedPtr
RDMAContext::submit(OpCode opcode, AssignmentBatch& batch, callback_fn_t callback, int qpi, int32_t imm_data)
{
    AssignmentBatch              batch_split_after_max_length;
    std::vector<AssignmentBatch> batch_split_after_cq_depth;

    // auto split_by_max_length = [&]() {
    //     for (size_t i = 0; i < batch.size(); ++i) {
    //         for (size_t j = 0; j < batch[i].length; j += SLIME_MAX_LENGTH_PER_ASSIGNMENT) {
    //             batch_split_after_max_length.push_back(
    //                 Assignment(batch[i].mr_key,
    //                            batch[i].target_offset + j,
    //                            batch[i].source_offset + j,
    //                            std::min(static_cast<size_t>(SLIME_MAX_LENGTH_PER_ASSIGNMENT), batch[i].length - j)));
    //         }
    //     }
    // };

    // auto split_by_max_cq_depth = [&]() {
    //     int split_step = SLIME_MAX_CQ_DEPTH / 2;
    //     for (int i = 0; i < batch_split_after_max_length.size(); i += split_step) {
    //         batch_split_after_cq_depth.push_back(AssignmentBatch(
    //             batch_split_after_max_length.begin() + i,
    //             std::min(batch_split_after_max_length.end(), batch_split_after_max_length.begin() + i +
    //             split_step)));
    //     }
    // };

    // split_by_max_length();
    // split_by_max_cq_depth();
    if (qpi == UNDEFINED_QPI) {
        qpi = select_qpi();
    }
    std::cout << "qpiqpiqpi" << std::endl;
    int split_size = batch.size();
    for (int i = 0; i < split_size; i++) {
        batch_split_after_cq_depth.push_back(AssignmentBatch{batch[i]});
    }

    {
        std::unique_lock<std::mutex> lock(qp_management_[qpi]->assign_queue_mutex_);
        RDMAAssignmentSharedPtr      rdma_assignment;
        std::cout << "split_size" << split_size << std::endl;
        for (int i = 0; i < split_size; ++i) {
            std::cout << "split_size" << split_size << std::endl;
            callback_fn_t split_callback = (i == split_size - 1 ? callback : [](int, int) { return 0; });
            rdma_assignment = std::make_shared<RDMAAssignment>(opcode, batch_split_after_cq_depth[i], split_callback);
            qp_management_[qpi]->assign_queue_.push(rdma_assignment);
            std::cout << "qp_management_" << std::endl;
            rdma_assignment->with_imm_data_ = (i == split_size - 1) ? (imm_data != UNDEFINED_IMM_DATA) : false;
            rdma_assignment->imm_data_      = (i == split_size - 1) ? imm_data : UNDEFINED_IMM_DATA;
        }

        qp_management_[qpi]->has_runnable_event_.notify_one();
        std::cout << "qpiqpiqpi" << qpi << std::endl;
        return rdma_assignment;
    }
}

int64_t RDMAContext::post_send_batch(int qpi, RDMAAssignmentSharedPtr assign)
{
    int                 ret        = 0;
    size_t              batch_size = assign->batch_size();
    struct ibv_send_wr* bad_wr     = nullptr;
    struct ibv_send_wr* wr         = new ibv_send_wr[batch_size];
    struct ibv_sge*     sge        = new ibv_sge[batch_size];
    for (size_t i = 0; i < batch_size; ++i) {

        Assignment&    subassign = assign->batch_[i];
        struct ibv_mr* mr        = memory_pool_->get_mr(subassign.mr_key);
        memset(&sge[i], 0, sizeof(ibv_sge));
        sge[i].addr   = (uintptr_t)mr->addr + subassign.source_offset;
        sge[i].length = subassign.length;
        sge[i].lkey   = mr->lkey;
        memset(&wr[i], 0, sizeof(ibv_send_wr));
        wr[i].wr_id =
            (i == batch_size - 1) ? (uintptr_t)(new callback_info_with_qpi_t{assign->callback_info_, qpi}) : 0;
        wr[i].opcode     = ASSIGN_OP_2_IBV_WR_OP.at(assign->opcode_);
        wr[i].sg_list    = &sge[i];
        wr[i].num_sge    = 1;
        wr[i].imm_data   = (i == batch_size - 1) ? assign->imm_data_ : UNDEFINED_IMM_DATA;
        wr[i].send_flags = (i == batch_size - 1) ? IBV_SEND_SIGNALED : 0;
        wr[i].next       = (i == batch_size - 1) ? nullptr : &wr[i + 1];
    }
    {
        std::unique_lock<std::mutex> lock(qp_management_[qpi]->rdma_post_send_mutex_);
        qp_management_[qpi]->outstanding_rdma_reads_.fetch_add(batch_size, std::memory_order_relaxed);
        ret = ibv_post_send(qp_management_[qpi]->qp_, wr, &bad_wr);
    }
    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret));
        qp_management_[qpi]->outstanding_rdma_reads_.fetch_sub(batch_size, std::memory_order_relaxed);
        return -1;
    }
    delete[] wr;
    delete[] sge;
    return 0;
}

int64_t RDMAContext::post_recv_batch(int qpi, RDMAAssignmentSharedPtr assign)
{
    int64_t             ret        = 0;
    size_t              batch_size = assign->batch_size();
    struct ibv_recv_wr* bad_wr     = nullptr;
    struct ibv_recv_wr* wr         = new ibv_recv_wr[batch_size];
    struct ibv_sge*     sge        = new ibv_sge[batch_size];
    for (size_t i = 0; i < batch_size; ++i) {

        Assignment&    subassign = assign->batch_[i];
        struct ibv_mr* mr        = memory_pool_->get_mr(subassign.mr_key);
        memset(&sge[i], 0, sizeof(ibv_sge));
        sge[i].addr   = (uintptr_t)mr->addr + subassign.source_offset;
        sge[i].length = subassign.length;
        sge[i].lkey   = mr->lkey;
        memset(&wr[i], 0, sizeof(ibv_recv_wr));
        wr[i].wr_id =
            (i == batch_size - 1) ? (uintptr_t)(new callback_info_with_qpi_t{assign->callback_info_, qpi}) : 0;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        wr[i].next    = (i == batch_size - 1) ? nullptr : &wr[i + 1];
    }
    {
        std::unique_lock<std::mutex> lock(qp_management_[qpi]->rdma_post_send_mutex_);
        qp_management_[qpi]->outstanding_rdma_reads_.fetch_add(batch_size, std::memory_order_relaxed);
        ret = ibv_post_recv(qp_management_[qpi]->qp_, wr, &bad_wr);
    }
    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret));
        qp_management_[qpi]->outstanding_rdma_reads_.fetch_sub(batch_size, std::memory_order_relaxed);
        return -1;
    }

    delete[] wr;
    delete[] sge;
    return 0;
}

int64_t RDMAContext::post_rc_oneside_batch(int qpi, RDMAAssignmentSharedPtr assign)
{
    size_t              batch_size = assign->batch_size();
    struct ibv_send_wr* bad_wr     = NULL;
    struct ibv_send_wr* wr         = new ibv_send_wr[batch_size];
    struct ibv_sge*     sge        = new ibv_sge[batch_size];

    for (size_t i = 0; i < batch_size; ++i) {
        Assignment     subassign   = assign->batch_[i];
        struct ibv_mr* mr          = memory_pool_->get_mr(subassign.mr_key);
        remote_mr_t    remote_mr   = memory_pool_->get_remote_mr(subassign.mr_key);
        uint64_t       remote_addr = remote_mr.addr;
        uint32_t       remote_rkey = remote_mr.rkey;
        memset(&sge[i], 0, sizeof(ibv_sge));
        sge[i].addr   = (uint64_t)mr->addr + subassign.source_offset;
        sge[i].length = subassign.length;
        sge[i].lkey   = mr->lkey;
        wr[i].wr_id =
            (i == batch_size - 1) ? (uintptr_t)(new callback_info_with_qpi_t{assign->callback_info_, qpi}) : 0;
        wr[i].opcode              = ASSIGN_OP_2_IBV_WR_OP.at(assign->opcode_);
        wr[i].sg_list             = &sge[i];
        wr[i].num_sge             = 1;
        wr[i].imm_data            = (i == batch_size - 1) ? assign->imm_data_ : UNDEFINED_IMM_DATA;
        wr[i].send_flags          = (i == batch_size - 1) ? IBV_SEND_SIGNALED : 0;
        wr[i].wr.rdma.remote_addr = remote_addr + assign->batch_[i].target_offset;
        wr[i].wr.rdma.rkey        = remote_rkey;
        wr[i].next                = (i == batch_size - 1) ? NULL : &wr[i + 1];
    }
    int ret = 0;
    {
        std::unique_lock<std::mutex> lock(qp_management_[qpi]->rdma_post_send_mutex_);
        qp_management_[qpi]->outstanding_rdma_reads_.fetch_add(assign->batch_size(), std::memory_order_relaxed);
        ret = ibv_post_send(qp_management_[qpi]->qp_, wr, &bad_wr);
    }

    delete[] wr;
    delete[] sge;

    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret));
        return -1;
    }
    return 0;
}

int64_t RDMAContext::cq_poll_handle()
{
    SLIME_LOG_INFO("Polling CQ");

    if (!connected_) {
        SLIME_LOG_ERROR("Start CQ handle before connected, please construct first");
        return -1;
    }
    if (comp_channel_ == NULL)
        SLIME_LOG_ERROR("comp_channel_ should be constructed");
    while (!stop_cq_future_) {
        struct ibv_cq* ev_cq;
        void*          cq_context;
        if (ibv_get_cq_event(comp_channel_, &ev_cq, &cq_context) != 0) {
            SLIME_LOG_ERROR("Failed to get CQ event");
            return -1;
        }
        ibv_ack_cq_events(ev_cq, 1);
        if (ibv_req_notify_cq(ev_cq, 0) != 0) {
            SLIME_LOG_ERROR("Failed to request CQ notification");
            return -1;
        }
        struct ibv_wc wc[SLIME_POLL_COUNT];

        while (size_t nr_poll = ibv_poll_cq(cq_, SLIME_POLL_COUNT, wc)) {
            if (stop_cq_future_)
                return 0;
            if (nr_poll < 0) {
                SLIME_LOG_WARN("Worker: Failed to poll completion queues");
                continue;
            }
            for (size_t i = 0; i < nr_poll; ++i) {
                callback_info_with_qpi_t::CALLBACK_STATUS status_code = callback_info_with_qpi_t::SUCCESS;
                if (wc[i].status != IBV_WC_SUCCESS) {
                    status_code = callback_info_with_qpi_t::FAILED;
                    SLIME_LOG_ERROR("WR failed with status: ",
                                    ibv_wc_status_str(wc[i].status),
                                    ", vi vendor err: ",
                                    wc[i].vendor_err);
                }
                if (wc[i].wr_id != 0) {
                    callback_info_with_qpi_t* callback_with_qpi =
                        reinterpret_cast<callback_info_with_qpi_t*>(wc[i].wr_id);
                    switch (OpCode wr_type = callback_with_qpi->callback_info_->opcode_) {
                        case OpCode::READ:
                            callback_with_qpi->callback_info_->callback_(status_code, wc[i].imm_data);
                            break;
                        case OpCode::WRITE:
                            callback_with_qpi->callback_info_->callback_(status_code, wc[i].imm_data);
                            break;
                        case OpCode::SEND:
                        case OpCode::SEND_WITH_IMM:
                            callback_with_qpi->callback_info_->callback_(status_code, wc[i].imm_data);
                            break;
                        case OpCode::RECV:
                            callback_with_qpi->callback_info_->callback_(status_code, wc[i].imm_data);
                            break;
                        case OpCode::WRITE_WITH_IMM:
                            callback_with_qpi->callback_info_->callback_(status_code, wc[i].imm_data);
                            break;
                        default:
                            SLIME_ABORT("Unimplemented WrType " << int64_t(wr_type));
                    }
                    size_t batch_size = callback_with_qpi->callback_info_->batch_size_;
                    qp_management_[callback_with_qpi->qpi_]->outstanding_rdma_reads_.fetch_sub(
                        batch_size, std::memory_order_relaxed);
                    delete callback_with_qpi;
                }
            }
        }
    }
    return 0;
}

int64_t RDMAContext::wq_dispatch_handle(int qpi)
{
    SLIME_LOG_INFO("Handling WQ");

    if (!connected_) {
        SLIME_LOG_ERROR("Start CQ handle before connected, please construct first");
        return -1;
    }

    if (comp_channel_ == NULL)
        SLIME_LOG_ERROR("comp_channel_ should be constructed");

    while (!qp_management_[qpi]->stop_wq_future_) {
        std::unique_lock<std::mutex> lock(qp_management_[qpi]->assign_queue_mutex_);
        qp_management_[qpi]->has_runnable_event_.wait(lock, [this, &qpi]() {
            return !(qp_management_[qpi]->assign_queue_.empty()) || qp_management_[qpi]->stop_wq_future_;
        });
        if (qp_management_[qpi]->stop_wq_future_)
            return 0;
        while (!(qp_management_[qpi]->assign_queue_.empty())) {
            RDMAAssignmentSharedPtr front_assign = qp_management_[qpi]->assign_queue_.front();
            size_t                  batch_size   = front_assign->batch_size();
            if (batch_size > SLIME_MAX_CQ_DEPTH) {
                SLIME_LOG_ERROR("batch_size(" << batch_size << ") > MAX SLIME_MAX_CQ_DEPTH (" << SLIME_MAX_CQ_DEPTH
                                              << "), this request will be ignored");
                front_assign->callback_info_->callback_(callback_info_with_qpi_t::ASSIGNMENT_BATCH_OVERFLOW, 0);
                qp_management_[qpi]->assign_queue_.pop();
            }
            else if (batch_size + qp_management_[qpi]->outstanding_rdma_reads_ < SLIME_MAX_CQ_DEPTH) {
                SLIME_LOG_DEBUG("Schedule batch, batch size: ",
                                batch_size,
                                ". Outstanding: ",
                                qp_management_[qpi]->outstanding_rdma_reads_);
                switch (front_assign->opcode_) {
                    case OpCode::SEND:
                        post_send_batch(qpi, front_assign);
                        break;
                    case OpCode::RECV:
                        std::cout << "RECVRECVRECVRECVRECV" << std::endl;
                        post_recv_batch(qpi, front_assign);
                        break;
                    case OpCode::READ:
                        post_rc_oneside_batch(qpi, front_assign);
                        break;
                    case OpCode::WRITE:
                        post_rc_oneside_batch(qpi, front_assign);
                        break;
                    case OpCode::SEND_WITH_IMM:
                        post_send_batch(qpi, front_assign);
                        break;
                    case OpCode::WRITE_WITH_IMM:
                        std::cout << "WRITE_WITH_IMMWRITE_WITH_IMMWRITE_WITH_IMM" << std::endl;
                        post_rc_oneside_batch(qpi, front_assign);
                        break;
                    default:
                        SLIME_LOG_ERROR("Unknown OpCode");
                        front_assign->callback_info_->callback_(callback_info_with_qpi_t::UNKNOWN_OPCODE, 0);
                        break;
                }
                qp_management_[qpi]->assign_queue_.pop();
            }
            else {
                std::this_thread::sleep_for(std::chrono::nanoseconds(500000));
                SLIME_LOG_DEBUG(
                    "Assignment Queue is full(", batch_size, ", ", qp_management_[qpi]->outstanding_rdma_reads_, ").");
            }
        }
    }
    return 0;
}

}  // namespace slime
