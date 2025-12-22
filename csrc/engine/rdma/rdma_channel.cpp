#include "rdma_channel.h"

namespace slime {
int32_t RDMAChannel::init(std::shared_ptr<RDMAContext> ctx, size_t num_qp, int32_t max_inline_data)
{
    reset();

    ctx_ = ctx;

    qp_.resize(num_qp);

    send_wr_pool_.resize(num_qp);
    recv_wr_pool_.resize(num_qp);

    send_sge_pool_.resize(num_qp);
    recv_sge_pool_.resize(num_qp);

    local_rdma_info_.resize(num_qp);
    remote_rdma_info_.resize(num_qp);

    for (int qpi = 0; qpi < num_qp; ++qpi) {
        send_wr_pool_[qpi].resize(SLIME_MAX_SEND_WR);
        recv_wr_pool_[qpi].resize(SLIME_MAX_RECV_WR);

        send_sge_pool_[qpi].resize(SLIME_MAX_SEND_WR);
        recv_sge_pool_[qpi].resize(SLIME_MAX_RECV_WR);

        /* Create Queue Pair (QP) */
        struct ibv_qp_init_attr qp_init_attr = {};
        qp_init_attr.send_cq                 = ctx_->cq_;
        qp_init_attr.recv_cq                 = ctx_->cq_;
        qp_init_attr.qp_type                 = IBV_QPT_RC;  // Reliable Connection

        if (max_inline_data == 0) {
            qp_init_attr.cap.max_send_wr = SLIME_MAX_SEND_WR;
        }
        else {
            SLIME_ASSERT(max_inline_data <= 4096, "inline data need to less than or equal to 4096");
            qp_init_attr.cap.max_send_wr     = 4096;
            qp_init_attr.cap.max_inline_data = max_inline_data;
        }

        qp_init_attr.cap.max_recv_wr  = SLIME_MAX_RECV_WR;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.sq_sig_all       = false;

        qp_[qpi] = ibv_create_qp(ctx_->pd_, &qp_init_attr);
        if (!qp_[qpi]) {
            SLIME_LOG_ERROR(
                "[" << ctx_->device_name_ << "] Failed to create QP " << qp_[qpi]->qp_num, ": ", strerror(errno));
            return -1;
        }

        /* Modify QP to INIT state */
        struct ibv_qp_attr attr = {};
        attr.qp_state           = IBV_QPS_INIT;
        attr.port_num           = ctx_->ib_port_;
        attr.pkey_index         = 0;
        attr.qp_access_flags    = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

        int ret = ibv_modify_qp(qp_[qpi], &attr, flags);
        if (ret) {
            SLIME_LOG_ERROR("Failed to modify QP to INIT");
        }

        /* Set Packet Sequence Number (PSN) */
        srand48(time(NULL));
        int32_t psn = lrand48() & 0xffffff;

        /* Get GID */
        if (ctx_->gidx_ != -1 && ibv_query_gid(ctx_->ib_ctx_, 1, ctx_->gidx_, &ctx_->gid_)) {
            SLIME_LOG_ERROR("[" << ctx_->device_name_ << "] Failed to get GID");
        }

        /* Set Local RDMA Info */
        local_rdma_info_[qpi].gidx = ctx_->gidx_;
        local_rdma_info_[qpi].qpn  = qp_[qpi]->qp_num;
        local_rdma_info_[qpi].psn  = psn;
        local_rdma_info_[qpi].gid  = ctx_->gid_;
        local_rdma_info_[qpi].lid  = ctx_->lid_;
        local_rdma_info_[qpi].mtu  = (uint32_t)ctx_->active_mtu_;
    }
    SLIME_LOG_INFO("RDMA context initialized")
    SLIME_LOG_DEBUG("RDMA context local configuration: ", channelInfo());

    state = RDMAChannelState::Initialized;

    return 0;
}

json RDMAChannel::channelInfo() const
{
    json local_info{};
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++)
        local_info[qpi] = local_rdma_info_[qpi].to_json();
    return local_info;
}

int32_t RDMAChannel::connect(json remote_rdma_info_json)
{
    SLIME_ASSERT(state == RDMAChannelState::Initialized, "Not Initialized or already connected");
    SLIME_ASSERT_EQ(local_rdma_info_.size(), remote_rdma_info_json.size(), "Peer must have same QP Size.");

    // construct RDMAEndpoint connection
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++) {
        remote_rdma_info_[qpi] = rdma_info_t(remote_rdma_info_json[qpi]);
    }

    modify_qp_to_r2r();
    modify_qp_to_r2s();

    state = RDMAChannelState::Connected;
    if (ibv_req_notify_cq(ctx_->cq_, 0)) {
        SLIME_ABORT("Failed to request notify for CQ");
    }
    SLIME_LOG_INFO("RDMA exchange done");
    return 0;
}

int32_t RDMAChannel::modify_qp_to_r2r()
{
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++) {
        int                ret;
        struct ibv_qp_attr attr = {};
        int                flags;
        struct ibv_qp*     qp = qp_[qpi];
        // Modify QP to Ready to Receive (RTR) state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu =
            (enum ibv_mtu)std::min((uint32_t)remote_rdma_info_[qpi].mtu, (uint32_t)local_rdma_info_[qpi].mtu);

        attr.dest_qp_num           = remote_rdma_info_[qpi].qpn;
        attr.rq_psn                = remote_rdma_info_[qpi].psn;
        attr.max_dest_rd_atomic    = SLIME_MAX_DEST_RD_ATOMIC;
        attr.min_rnr_timer         = 0x16;
        attr.ah_attr.dlid          = remote_rdma_info_[qpi].lid;
        attr.ah_attr.sl            = SLIME_SERVICE_LEVEL;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num      = ctx_->ib_port_;

        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid      = 0;

        if (local_rdma_info_[qpi].gidx == -1) {
            // IB
            attr.ah_attr.dlid = local_rdma_info_[qpi].lid;
        }
        else {
            // RoCE v2
            attr.ah_attr.is_global         = 1;
            attr.ah_attr.grh.dgid          = remote_rdma_info_[qpi].gid;
            attr.ah_attr.grh.sgid_index    = local_rdma_info_[qpi].gidx;
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
    }
    return 0;
}

int32_t RDMAChannel::modify_qp_to_r2s()
{
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++) {
        int                ret;
        struct ibv_qp_attr attr = {};
        int                flags;
        struct ibv_qp*     qp = qp_[qpi];
        // Modify QP to RTS state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state      = IBV_QPS_RTS;
        attr.timeout       = 14;
        attr.retry_cnt     = 7;
        attr.rnr_retry     = 7;
        attr.sq_psn        = local_rdma_info_[qpi].psn;
        attr.max_rd_atomic = SLIME_MAX_RD_ATOMIC;

        flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN
                | IBV_QP_MAX_QP_RD_ATOMIC;

        ret = ibv_modify_qp(qp, &attr, flags);
        if (ret) {
            SLIME_ABORT("Failed to modify QP to RTS");
        }
    }
    return 0;
}

int32_t RDMAChannel::reset()
{
    for (auto& qp : qp_) {
        if (qp)
            ibv_destroy_qp(qp);
    }
    return 0;
}

int64_t RDMAChannel::post_send_batch(int qpi, RDMAAssign* assign)
{
    int                 ret        = 0;
    size_t              batch_size = assign->batch_size();
    struct ibv_send_wr* bad_wr     = nullptr;
    struct ibv_send_wr* wr         = send_wr_pool_[qpi].data();
    struct ibv_sge*     sge        = send_sge_pool_[qpi].data();
    for (size_t i = 0; i < batch_size; ++i) {

        Assignment&    subassign = assign->batch_[i];
        struct ibv_mr* mr        = ctx_->memory_pool_->get_mr(subassign.mr_key);
        sge[i].addr              = (uintptr_t)mr->addr + subassign.source_offset;
        sge[i].length            = subassign.length;
        sge[i].lkey              = mr->lkey;
        wr[i].wr_id              = (i == batch_size - 1) ? (uintptr_t)(assign) : 0;
        wr[i].opcode             = ASSIGN_OP_2_IBV_WR_OP.at(assign->opcode_);
        wr[i].sg_list            = &sge[i];
        wr[i].num_sge            = 1;
        wr[i].imm_data           = (i == batch_size - 1) ? assign->imm_data_ : UNDEFINED_IMM_DATA;
        wr[i].send_flags         = (i == batch_size - 1) ? IBV_SEND_SIGNALED : 0;
        if (assign->is_inline_)
            wr[i].send_flags |= IBV_SEND_INLINE;
        wr[i].next = (i == batch_size - 1) ? nullptr : &wr[i + 1];
    }
    ret = ibv_post_send(qp_[qpi], wr, &bad_wr);
    if (ret) {
        return -1;
    }
    return 0;
}

int64_t RDMAChannel::post_recv_batch(int qpi, RDMAAssign* assign)
{
    int64_t             ret        = 0;
    size_t              batch_size = assign->batch_size();
    struct ibv_recv_wr* bad_wr     = nullptr;
    struct ibv_recv_wr* wr         = recv_wr_pool_[qpi].data();
    struct ibv_sge*     sge        = recv_sge_pool_[qpi].data();
    for (size_t i = 0; i < batch_size; ++i) {

        Assignment&    subassign = assign->batch_[i];
        struct ibv_mr* mr        = ctx_->memory_pool_->get_mr(subassign.mr_key);
        sge[i].addr              = (uintptr_t)mr->addr + subassign.source_offset;
        sge[i].length            = subassign.length;
        sge[i].lkey              = mr->lkey;
        wr[i].wr_id              = (i == batch_size - 1) ? (uintptr_t)(assign) : 0;
        wr[i].sg_list            = &sge[i];
        wr[i].num_sge            = 1;
        wr[i].next               = (i == batch_size - 1) ? nullptr : &wr[i + 1];
    }
    ret = ibv_post_recv(qp_[qpi], wr, &bad_wr);
    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA recv : " << strerror(ret));
        return -1;
    }

    return 0;
}

int64_t RDMAChannel::post_rc_oneside_batch(int qpi, RDMAAssign* assign)
{
    size_t              batch_size = assign->batch_size();
    struct ibv_send_wr* bad_wr     = NULL;
    struct ibv_send_wr* wr         = send_wr_pool_[qpi].data();
    struct ibv_sge*     sge        = send_sge_pool_[qpi].data();

    for (size_t i = 0; i < batch_size; ++i) {
        Assignment     subassign   = assign->batch_[i];
        struct ibv_mr* mr          = ctx_->memory_pool_->get_mr(subassign.mr_key);
        remote_mr_t    remote_mr   = ctx_->memory_pool_->get_remote_mr(subassign.remote_mr_key);
        uint64_t       remote_addr = remote_mr.addr;
        uint32_t       remote_rkey = remote_mr.rkey;
        sge[i].addr                = (uint64_t)mr->addr + subassign.source_offset;
        sge[i].length              = subassign.length;
        sge[i].lkey                = mr->lkey;
        wr[i].wr_id                = (i == batch_size - 1) ? (uintptr_t)(assign) : 0;

        wr[i].opcode = ASSIGN_OP_2_IBV_WR_OP.at(assign->opcode_);
        if (wr[i].opcode == IBV_WR_RDMA_WRITE_WITH_IMM and (i != batch_size - 1)) {
            wr[i].opcode = IBV_WR_RDMA_WRITE;
        }

        wr[i].sg_list    = &sge[i];
        wr[i].num_sge    = 1;
        wr[i].imm_data   = (i == batch_size - 1) ? assign->imm_data_ : UNDEFINED_IMM_DATA;
        wr[i].send_flags = (i == batch_size - 1) ? IBV_SEND_SIGNALED : 0;
        if (assign->is_inline_)
            wr[i].send_flags |= IBV_SEND_INLINE;
        wr[i].wr.rdma.remote_addr = remote_addr + assign->batch_[i].target_offset;
        wr[i].wr.rdma.rkey        = remote_rkey;
        wr[i].next                = (i == batch_size - 1) ? NULL : &wr[i + 1];
    }
    int ret = 0;
    {
        ret = ibv_post_send(qp_[qpi], wr, &bad_wr);
    }

    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret), ". Error Assignment: ", assign->dump(), ".");
        return -1;
    }
    return 0;
}

}  // namespace slime
