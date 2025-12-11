#include "rdma_endpoint_v0.h"

#include "device/device_api.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_common.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_env.h"
#include "engine/rdma/rdma_utils.h"
#include "logging.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdlib.h>
#include <sys/types.h>
#include <thread>
#include <vector>

namespace slime {

int64_t RDMAEndpointV0::post_send_batch(std::shared_ptr<RDMAContext> ctx, qp_management_t qp_man, RDMAAssign* assign)
{
    int                 ret        = 0;
    size_t              batch_size = assign->batch_size();
    struct ibv_send_wr* bad_wr     = nullptr;
    struct ibv_send_wr* wr         = qp_man.send_wr_pool_.data();
    struct ibv_sge*     sge        = qp_man.send_sge_pool_.data();
    for (size_t i = 0; i < batch_size; ++i) {

        Assignment&    subassign = assign->batch_[i];
        struct ibv_mr* mr        = ctx->memory_pool_->get_mr(subassign.mr_key);
        sge[i].addr              = (uintptr_t)mr->addr + subassign.source_offset;
        sge[i].length            = subassign.length;
        sge[i].lkey              = mr->lkey;
        wr[i].wr_id              = (i == batch_size - 1) ? (uintptr_t)(assign) : 0;
        wr[i].opcode             = ASSIGN_OP_2_IBV_WR_OP.at(assign->opcode_);
        wr[i].sg_list            = &sge[i];
        wr[i].num_sge            = 1;
        wr[i].imm_data           = (i == batch_size - 1) ? assign->imm_data_ : RDMAContext::UNDEFINED_IMM_DATA;
        wr[i].send_flags         = (i == batch_size - 1) ? IBV_SEND_SIGNALED : 0;
        if (assign->is_inline_)
            wr[i].send_flags |= IBV_SEND_INLINE;
        wr[i].next = (i == batch_size - 1) ? nullptr : &wr[i + 1];
    }
    ret = ibv_post_send(qp_man.qp_, wr, &bad_wr);
    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret));
        return -1;
    }
    return 0;
}

int64_t RDMAEndpointV0::post_recv_batch(std::shared_ptr<RDMAContext> ctx, qp_management_t qp_man, RDMAAssign* assign)
{
    int64_t             ret        = 0;
    size_t              batch_size = assign->batch_size();
    struct ibv_recv_wr* bad_wr     = nullptr;
    struct ibv_recv_wr* wr         = qp_man.recv_wr_pool_.data();
    struct ibv_sge*     sge        = qp_man.recv_sge_pool_.data();
    for (size_t i = 0; i < batch_size; ++i) {

        Assignment&    subassign = assign->batch_[i];
        struct ibv_mr* mr        = ctx->memory_pool_->get_mr(subassign.mr_key);
        sge[i].addr              = (uintptr_t)mr->addr + subassign.source_offset;
        sge[i].length            = subassign.length;
        sge[i].lkey              = mr->lkey;
        wr[i].wr_id              = (i == batch_size - 1) ? (uintptr_t)(assign) : 0;
        wr[i].sg_list            = &sge[i];
        wr[i].num_sge            = 1;
        wr[i].next               = (i == batch_size - 1) ? nullptr : &wr[i + 1];
    }
    {
        ret = ibv_post_recv(qp_man.qp_, wr, &bad_wr);
    }
    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret));
        return -1;
    }

    return 0;
}

int64_t
RDMAEndpointV0::post_rc_oneside_batch(std::shared_ptr<RDMAContext> ctx, qp_management_t qp_man, RDMAAssign* assign)
{
    size_t              batch_size = assign->batch_size();
    struct ibv_send_wr* bad_wr     = NULL;
    struct ibv_send_wr* wr         = qp_man.send_wr_pool_.data();
    struct ibv_sge*     sge        = qp_man.send_sge_pool_.data();

    for (size_t i = 0; i < batch_size; ++i) {
        Assignment     subassign   = assign->batch_[i];
        struct ibv_mr* mr          = ctx->memory_pool_->get_mr(subassign.mr_key);
        remote_mr_t    remote_mr   = ctx->memory_pool_->get_remote_mr(subassign.remote_mr_key);
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
        wr[i].imm_data   = (i == batch_size - 1) ? assign->imm_data_ : RDMAContext::UNDEFINED_IMM_DATA;
        wr[i].send_flags = (i == batch_size - 1) ? IBV_SEND_SIGNALED : 0;
        if (assign->is_inline_)
            wr[i].send_flags |= IBV_SEND_INLINE;
        wr[i].wr.rdma.remote_addr = remote_addr + assign->batch_[i].target_offset;
        wr[i].wr.rdma.rkey        = remote_rkey;
        wr[i].next                = (i == batch_size - 1) ? NULL : &wr[i + 1];
    }
    int ret = 0;
    ret     = ibv_post_send(qp_man.qp_, wr, &bad_wr);

    if (ret) {
        SLIME_LOG_ERROR("Failed to post RDMA send : " << strerror(ret));
        return -1;
    }
    return 0;
}

int32_t RDMAEndpointV0::init()
{
    meta_qp_man_ = init_qp(meta_ctx_, 256)[0];
    data_qp_man_ = init_qp(data_ctx_, qp_nums_);
    return 0;
}

void RDMAEndpointV0::connect_qp(std::shared_ptr<RDMAContext> ctx,
                                std::vector<qp_management>   qp_mans,
                                const json&                  endpoint_info_json)
{
    SLIME_LOG_INFO("RDMA context remote connecting");
    SLIME_LOG_DEBUG("RDMA context remote configuration: ", endpoint_info_json);
    // Register Remote Memory Region
    for (auto& item : endpoint_info_json["mr_info"].items()) {
        ctx->registerRemoteMemoryRegion(item.value()["mr_key"].get<uintptr_t>(), item.value());
    }
    SLIME_ASSERT_EQ(qp_mans.size(), endpoint_info_json["rdma_info"].size(), "Peer must have same QP Size.");

    // construct RDMAEndpoint connection
    for (int qpi = 0; qpi < qp_mans.size(); qpi++) {
        int                ret;
        struct ibv_qp_attr attr = {};
        int                flags;
        qp_management_t    qp_man           = qp_mans[qpi];
        struct ibv_qp*     qp               = qp_man.qp_;
        rdma_info_t&       local_rdma_info  = qp_man.local_rdma_info_;
        rdma_info_t&       remote_rdma_info = qp_man.remote_rdma_info_;
        remote_rdma_info                    = rdma_info_t(endpoint_info_json["rdma_info"][qpi]);

        // Modify QP to Ready to Receive (RTR) state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state           = IBV_QPS_RTR;
        attr.path_mtu           = (enum ibv_mtu)std::min((uint32_t)remote_rdma_info.mtu, (uint32_t)local_rdma_info.mtu);
        attr.dest_qp_num        = remote_rdma_info.qpn;
        attr.rq_psn             = remote_rdma_info.psn;
        attr.max_dest_rd_atomic = SLIME_MAX_DEST_RD_ATOMIC;
        attr.min_rnr_timer      = 0x16;
        attr.ah_attr.dlid       = remote_rdma_info.lid;
        attr.ah_attr.sl         = SLIME_SERVICE_LEVEL;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num      = ctx->ib_port_;

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

        if (ibv_req_notify_cq(ctx->cq_, 0)) {
            SLIME_ABORT("Failed to request notify for CQ");
        }
    }
}

std::vector<RDMAEndpointV0::qp_management>
RDMAEndpointV0::init_qp(std::shared_ptr<RDMAContext> ctx, size_t num_qp, size_t max_num_inline_data)
{
    std::vector<qp_management> qp_mans;
    qp_mans.resize(num_qp);
    for (int qpi = 0; qpi < num_qp; ++qpi) {
        qp_management_t qp_man = qp_mans[qpi];
        qp_man.send_wr_pool_.resize(SLIME_MAX_SEND_WR);
        qp_man.send_sge_pool_.resize(SLIME_MAX_SEND_WR);
        qp_man.recv_wr_pool_.resize(SLIME_MAX_RECV_WR);
        qp_man.recv_sge_pool_.resize(SLIME_MAX_RECV_WR);

        /* Create Queue Pair (QP) */
        struct ibv_qp_init_attr qp_init_attr = {};
        qp_init_attr.send_cq                 = ctx->get_cq();
        qp_init_attr.recv_cq                 = ctx->get_cq();
        qp_init_attr.qp_type                 = IBV_QPT_RC;  // Reliable Connection

        if (max_num_inline_data == 0) {
            qp_init_attr.cap.max_send_wr = SLIME_MAX_SEND_WR;
        }
        else {
            SLIME_ASSERT(max_num_inline_data <= 4096, "inline data need to less than or equal to 4096");
            qp_init_attr.cap.max_send_wr     = 4096;
            qp_init_attr.cap.max_inline_data = max_num_inline_data;
        }

        qp_init_attr.cap.max_recv_wr  = SLIME_MAX_RECV_WR;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.sq_sig_all       = false;
        rdma_info_t& local_rdma_info  = qp_man.local_rdma_info_;
        qp_man.qp_                    = ibv_create_qp(ctx->pd_, &qp_init_attr);
        if (!qp_man.qp_) {
            SLIME_LOG_ERROR("Failed to create QP " << qp_man.qp_->qp_num, ": ", strerror(errno));
            return {};
        }

        /* Modify QP to INIT state */
        struct ibv_qp_attr attr = {};
        attr.qp_state           = IBV_QPS_INIT;
        attr.port_num           = ctx->ib_port_;
        attr.pkey_index         = 0;
        attr.qp_access_flags    = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

        int ret = ibv_modify_qp(qp_man.qp_, &attr, flags);
        if (ret) {
            SLIME_LOG_ERROR("Failed to modify QP to INIT");
        }

        /* Set Packet Sequence Number (PSN) */
        uint32_t psn;
        psn = lrand48() & 0xffffff;

        /* Get GID */
        if (ctx->gidx_ != -1 && ibv_query_gid(ctx->ib_ctx_, 1, ctx->gidx_, &ctx->gid_)) {
            SLIME_LOG_ERROR("Failed to get GID");
        }

        /* Set Local RDMA Info */
        local_rdma_info.gidx = ctx->gidx_;
        local_rdma_info.qpn  = qp_man.qp_->qp_num;
        local_rdma_info.psn  = psn;
        local_rdma_info.gid  = ctx->gid_;
        local_rdma_info.lid  = ctx->lid_;
        local_rdma_info.mtu  = (uint32_t)(ctx->active_mtu_);
    }
    return qp_mans;
}

jring_t* RDMAEndpointV0::createRing(const char* name, size_t count)
{
    size_t ring_sz = jring_get_buf_ring_size(sizeof(void*), count);

    void* mem = nullptr;
    if (posix_memalign(&mem, 64, ring_sz) != 0) {
        throw std::runtime_error(std::string("Failed to allocate ring memory: ") + name);
    }

    jring_t* r = (jring_t*)mem;

    // Initialize ring: MP=1 (Multi-Producer safe), MC=1 (Multi-Consumer safe)
    if (jring_init(r, count, sizeof(void*), 1, 1) < 0) {
        free(mem);
        throw std::runtime_error(std::string("Failed to init ring: ") + name);
    }
    return r;
}

void RDMAEndpointV0::freeRing(jring_t* ring)
{
    if (ring) {
        free(ring);
    }
}

RDMAEndpointV0::RDMAEndpointV0(std::shared_ptr<RDMAContext> data_ctx,
                               std::shared_ptr<RDMAContext> meta_ctx,
                               size_t                       qp_nums)
{
    SLIME_LOG_INFO("Init RDMAEndpointV0 Contexts and Devices...");
    SLIME_LOG_INFO("bypass Signal: ", SLIME_BYPASS_DEVICE_SIGNAL);
    if (SLIME_BYPASS_DEVICE_SIGNAL)
        bypass_signal_ = true;
    qp_nums_ = qp_nums;

    SLIME_ASSERT(1 == SLIME_AGG_QP_NUM, "cannot aggqp when sendrecv");
    SLIME_ASSERT(64 > SLIME_QP_NUM, "QP NUM must less than 64");

    size_t meta_buffer_size = sizeof(meta_info_t) * MAX_FIFO_DEPTH;

    void* dummy_mem = nullptr;
    if (posix_memalign(&dummy_mem, 64, sizeof(int64_t)) != 0)
        throw std::runtime_error("dummy alloc fail");
    dummy_ = (int64_t*)dummy_mem;

    void* remote_raw_mem = nullptr;
    if (posix_memalign(&remote_raw_mem, 64, meta_buffer_size) != 0)
        throw std::runtime_error("remote meta alloc fail");
    remote_meta_info_ = static_cast<meta_info_t*>(remote_raw_mem);

    void* local_raw_mem = nullptr;
    if (posix_memalign(&local_raw_mem, 64, meta_buffer_size) != 0)
        throw std::runtime_error("local meta alloc fail");
    local_meta_info_ = static_cast<meta_info_t*>(local_raw_mem);

    send_ctx_pool_.resize(MAX_FIFO_DEPTH);
    recv_ctx_pool_.resize(MAX_FIFO_DEPTH);

    size_t assign_buffer_size = sizeof(RDMAAssign) * MAX_FIFO_DEPTH;

    void* data_send_assign_raw = nullptr;
    if (posix_memalign(&data_send_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    data_send_assign_ = static_cast<RDMAAssign*>(data_send_assign_raw);

    void* data_recv_assign_raw = nullptr;
    if (posix_memalign(&data_recv_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    data_recv_assign_ = static_cast<RDMAAssign*>(data_recv_assign_raw);

    void* meta_send_assign_raw = nullptr;
    if (posix_memalign(&meta_send_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    meta_send_assign_ = static_cast<RDMAAssign*>(meta_send_assign_raw);

    void* meta_recv_assign_raw = nullptr;
    if (posix_memalign(&meta_recv_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    meta_recv_assign_ = static_cast<RDMAAssign*>(meta_recv_assign_raw);

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        send_ctx_pool_[i].signal = slime::device::createSignal(bypass_signal_);
        recv_ctx_pool_[i].signal = slime::device::createSignal(bypass_signal_);
    }

    // Register Memory Regions (MR)
    // Registering these upfront prevents expensive registration calls during runtime.
    meta_ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));
    meta_ctx_->registerMemoryRegion(reinterpret_cast<uintptr_t>(remote_meta_info_),
                                    reinterpret_cast<uintptr_t>(remote_meta_info_),
                                    meta_buffer_size);
    meta_ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(local_meta_info_), reinterpret_cast<uintptr_t>(local_meta_info_), meta_buffer_size);
    data_ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));

    SLIME_LOG_INFO("Memory Regions Registered.");

    // Initialize Scoreboards
    // These atomic flags serve as signaling mechanism between RDMA callback thread and Proxy threads.
    SLIME_LOG_DEBUG("Initializing scoreboards...");
    meta_arrived_scoreboard_ = new PaddedAtomicUint64[MAX_FIFO_DEPTH];
    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        meta_arrived_scoreboard_[i].val.store(0);
    }

    // Initialize Rings
    // Size is double the depth to handle potential overflow gracefully.
    size_t ring_size  = MAX_FIFO_DEPTH * 2;
    send_buffer_ring_ = createRing("send_buf", ring_size);
    recv_buffer_ring_ = createRing("recv_buf", ring_size);

    meta_qp_man_ = init_qp(meta_ctx_, 256)[0];
    data_qp_man_ = init_qp(data_ctx_, qp_nums_);
    SLIME_LOG_INFO("RDMA Endpoint Initialization Completed.");
}

RDMAEndpointV0::~RDMAEndpointV0()
{
    try {
        data_ctx_->stop_future();
        meta_ctx_->stop_future();

        free(dummy_);
        free(local_meta_info_);
        free(remote_meta_info_);
        delete[] meta_arrived_scoreboard_;

        freeRing(send_buffer_ring_);
        freeRing(recv_buffer_ring_);

        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully.");
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Exception in RDMAEndpoint destructor: ", e.what());
    }
}

void RDMAEndpointV0::connect(const json& data_ctx_info, const json& meta_ctx_info)
{
    SLIME_LOG_INFO("Establishing RDMA Connection...");

    connect_qp(meta_ctx_, {meta_qp_man_}, meta_ctx_info);
    connect_qp(data_ctx_, data_qp_man_, data_ctx_info);

    remote_meta_key_ = meta_ctx_info["remote_meta_key"];

    SLIME_LOG_INFO("Connection Established. Pre-posting RECV requests...");

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
        meta_recv_assign_[i].reset(OpCode::RECV, 0, batch, [this, i](int32_t status, int32_t imm) {
            meta_arrived_scoreboard_[i].val.store(1, std::memory_order_release);
            if (status != 0)
                SLIME_LOG_ERROR("Meta Recv Failed: ", status);
        });
        auto assign = post_recv_batch(meta_ctx_, meta_qp_man_, &(meta_recv_assign_[i]));
    }

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        auto signal = recv_ctx_pool_[i].signal;
        for (size_t qpi = 0; qpi < data_ctx_qp_num_; ++qpi) {
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

            data_recv_assign_[i].reset(OpCode::RECV, qpi, batch, [signal, qpi](int32_t status, int32_t imm) {
                if (status == 0) {
                    signal->set_comm_done(qpi);
                }
                else {
                    SLIME_LOG_ERROR("Data Recv Failed during pre-post");
                }
            });
            post_recv_batch(data_ctx_, data_qp_man_[qpi], &(data_recv_assign_[i]));
        }
    }

    SLIME_LOG_INFO("RDMA Contexts Launched.");
}

int32_t RDMAEndpointV0::addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer, void* stream_handle)
{
    auto buffer_mr = data_ctx_->get_mr(buffer->ptr_);
    if (not(buffer_mr and buffer_mr->length == buffer->data_size_)) {
        SLIME_LOG_DEBUG("Registering new MR for buffer: ", buffer->ptr_);
        data_ctx_->registerMemoryRegion(buffer->ptr_, buffer->ptr_, buffer->data_size_);
    }

    uint32_t target_mask = (1 << qp_nums_) - 1;
    buffer->num_pack_    = qp_nums_;

    if (OpCode::SEND == opcode) {
        uint64_t slot    = send_slot_id_.fetch_add(1, std::memory_order_release) % MAX_FIFO_DEPTH;
        buffer->slot_id_ = slot;

        SendContext* ctx = &send_ctx_pool_[slot];

        ctx->slot_id       = slot;
        ctx->buffer        = buffer;
        ctx->expected_mask = target_mask;

        ctx->signal->reset_all();
        ctx->signal->bind_stream(stream_handle);
        ctx->signal->record_gpu_ready();
        buffer->signal_ = ctx->signal;

        while (jring_enqueue_burst(send_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            cpu_relax();
        }
    }
    else if (OpCode::RECV == opcode) {
        uint64_t slot    = recv_slot_id_.fetch_add(1, std::memory_order_release) % MAX_FIFO_DEPTH;
        buffer->slot_id_ = slot;

        RecvContext* ctx = &recv_ctx_pool_[slot];

        ctx->slot_id       = slot;
        ctx->buffer        = buffer;
        ctx->expected_mask = target_mask;

        ctx->signal->reset_all();
        ctx->signal->bind_stream(stream_handle);
        ctx->signal->record_gpu_ready();
        buffer->signal_ = ctx->signal;

        local_meta_info_[slot].r_key_ = data_ctx_->get_mr(buffer->ptr_)->rkey;
        local_meta_info_[slot].view_  = buffer->view_;

        while (jring_enqueue_burst(recv_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::sendProcess()
{
    int free_space = MAX_PENDING_SIZE - send_count_;

    int batch_size = std::min((int)BURST_SIZE, free_space);

    if (batch_size > 0) {
        void* buf_ptrs[BURST_SIZE];
        int   n = jring_dequeue_burst(send_buffer_ring_, buf_ptrs, batch_size, nullptr);

        for (int i = 0; i < n; ++i) {
            pending_send_buf_[send_tail_] = (SendContext*)buf_ptrs[i];
            send_tail_++;
            if (send_tail_ == MAX_PENDING_SIZE)
                send_tail_ = 0;
        }
        send_count_ += n;
    }

    int processed_count = 0;

    while (send_count_ > 0) {
        auto* s_ctx = pending_send_buf_[send_head_];
        int   slot  = s_ctx->slot_id;

        if (!s_ctx->signal->is_gpu_ready()) {
            return processed_count;
        }

        if (!meta_arrived_scoreboard_[slot].val.load(std::memory_order_acquire)) {
            return processed_count;
        }

        meta_arrived_scoreboard_[slot].val.store(false, std::memory_order_relaxed);

        std::vector<Assignment> meta_batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

        meta_recv_assign_[slot].reset(OpCode::RECV, 0, meta_batch, [this, slot](int32_t status, int32_t imm) {
            meta_arrived_scoreboard_[slot].val.store(1, std::memory_order_release);
        });

        post_recv_batch(meta_ctx_, meta_qp_man_, &(meta_recv_assign_[slot]));

        auto meta = remote_meta_info_[slot];

        data_ctx_->registerRemoteMemoryRegion(meta.view_.data_ptr, meta.view_.data_ptr, meta.view_.length, meta.r_key_);

        size_t total_len  = s_ctx->buffer->data_size_;
        size_t chunk_size = (total_len + qp_nums_ - 1) / qp_nums_;

        for (size_t qpi = 0; qpi < qp_nums_; ++qpi) {
            size_t offset = qpi * chunk_size;

            if (offset >= total_len)
                break;

            size_t current_len = std::min(chunk_size, total_len - offset);

            Assignment assign(s_ctx->buffer->ptr_,
                              meta.view_.data_ptr,
                              offset,  // target offset (相对于 MR base)
                              offset,  // source offset (相对于 MR base)
                              current_len);

            AssignmentBatch batch{assign};

            data_send_assign_[slot].reset(
                OpCode::WRITE_WITH_IMM,
                qpi,
                batch,
                [s_ctx, qpi](int32_t stat, int32_t imm_data) { s_ctx->signal->set_comm_done(qpi); },
                false);

            post_rc_oneside_batch(data_ctx_, data_qp_man_[qpi], &(data_send_assign_[slot]));
        }

        // --- 成功处理，移动 Head 指针 ---
        send_head_++;
        if (send_head_ == MAX_PENDING_SIZE)
            send_head_ = 0;  // Wrap around

        send_count_--;
        processed_count++;
    }

    return processed_count;
}

int32_t RDMAEndpointV0::recvProcess()
{
    int free_space = MAX_PENDING_SIZE - recv_count_;
    int batch_size = std::min((int)BURST_SIZE, free_space);

    if (batch_size > 0) {
        void* buf_ptrs[BURST_SIZE];
        int   n = jring_dequeue_burst(recv_buffer_ring_, buf_ptrs, batch_size, nullptr);
        for (int i = 0; i < n; ++i) {
            pending_recv_buf_[recv_tail_] = (RecvContext*)buf_ptrs[i];
            recv_tail_++;
            if (recv_tail_ == MAX_PENDING_SIZE)
                recv_tail_ = 0;
        }
        recv_count_ += n;
    }

    int processed_count = 0;

    while (recv_count_ > 0) {
        auto* r_ctx = pending_recv_buf_[recv_head_];
        int   slot  = r_ctx->slot_id;

        if (!r_ctx->signal->is_gpu_ready()) {
            return processed_count;
        }

        for (size_t qpi = 0; qpi < data_ctx_qp_num_; ++qpi) {
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

            data_recv_assign_[slot].reset(OpCode::RECV, qpi, batch, [r_ctx, qpi](int32_t status, int32_t imm) {
                if (status == 0) {
                    r_ctx->signal->set_comm_done(qpi);
                }
                else {
                    SLIME_LOG_ERROR("Data Recv Failed, status: ", status);
                }
            });

            post_recv_batch(data_ctx_, data_qp_man_[qpi], &(data_recv_assign_[slot]));
        }

        Assignment assign(reinterpret_cast<uintptr_t>(local_meta_info_),
                          remote_meta_key_,
                          slot * sizeof(meta_info_t),
                          slot * sizeof(meta_info_t),
                          sizeof(meta_info_t));

        AssignmentBatch assign_batch{assign};

        meta_send_assign_[slot].reset(OpCode::WRITE_WITH_IMM, 0, assign_batch, nullptr, true);

        post_rc_oneside_batch(meta_ctx_, meta_qp_man_, &(meta_send_assign_[slot]));

        recv_head_++;
        if (recv_head_ == MAX_PENDING_SIZE)
            recv_head_ = 0;

        recv_count_--;
        processed_count++;
    }

    return processed_count;
}

}  // namespace slime