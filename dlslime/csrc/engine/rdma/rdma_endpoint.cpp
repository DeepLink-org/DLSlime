#include "rdma_endpoint.h"

#include <stdlib.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include "dlslime/csrc/common/pause.h"
#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/logging.h"
#include "dlslime/csrc/utils.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_channel.h"
#include "rdma_assignment.h"
#include "rdma_common.h"
#include "rdma_context.h"
#include "rdma_context_pool.h"
#include "rdma_env.h"
#include "rdma_future.h"
#include "rdma_utils.h"
#include "rdma_worker.h"
#include "rdma_worker_pool.h"

namespace dlslime {

namespace {

std::shared_ptr<EndpointOpState>
make_op_state(uint64_t op_id, bool bypass_signal, void* stream_handle, uint32_t expected_mask, bool record_gpu_ready)
{
    auto op_state           = std::make_shared<EndpointOpState>();
    op_state->signal        = dlslime::device::createSignal(bypass_signal);
    op_state->op_id         = op_id;
    op_state->expected_mask = expected_mask;
    op_state->completion_mask.store(0, std::memory_order_release);
    op_state->completion_status.store(RDMAAssign::SUCCESS, std::memory_order_release);
    op_state->imm_data.store(0, std::memory_order_release);
    if (op_state->signal) {
        op_state->signal->reset_all();
        op_state->signal->bind_stream(stream_handle);
        if (record_gpu_ready) {
            op_state->signal->record_gpu_ready();
        }
    }
    return op_state;
}

template<typename ContextT>
ContextT* acquire_slot(std::atomic<uint64_t>& counter, ContextT* pool, size_t depth)
{
    while (true) {
        uint64_t start = counter.fetch_add(1, std::memory_order_relaxed);
        for (size_t i = 0; i < depth; ++i) {
            ContextT* ctx = &pool[(start + i) % depth];
            bool      expected{false};
            if (ctx->in_use.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
                return ctx;
            }
        }
        machnet_pause();
    }
}

template<typename ContextT>
bool slot_is_active(const ContextT* ctx)
{
    return ctx->in_use.load(std::memory_order_acquire) && static_cast<bool>(ctx->op_state)
           && ctx->generation.load(std::memory_order_acquire) != 0;
}

template<typename ContextT>
bool slot_generation_matches(const ContextT* ctx, uint64_t generation)
{
    return ctx->generation.load(std::memory_order_acquire) == generation;
}

void release_slot(ReadWriteContext* ctx)
{
    ctx->op_state.reset();
    ctx->expected_mask = 0;
    ctx->finished_qp_mask.store(0, std::memory_order_release);
    ctx->generation.store(0, std::memory_order_release);
    ctx->state_ = IOContextState::FREE;
    ctx->in_use.store(false, std::memory_order_release);
}

void release_slot(ImmRecvContext* ctx)
{
    ctx->op_state.reset();
    ctx->expected_mask = 0;
    ctx->finished_qp_mask.store(0, std::memory_order_release);
    ctx->generation.store(0, std::memory_order_release);
    ctx->state_ = IOContextState::FREE;
    ctx->in_use.store(false, std::memory_order_release);
}

void release_slot(SendContext* ctx)
{
    ctx->op_state.reset();
    ctx->expected_mask = 0;
    ctx->finished_mask.store(0, std::memory_order_release);
    ctx->meta_arrived_flag_.val.store(0, std::memory_order_release);
    ctx->generation.store(0, std::memory_order_release);
    ctx->state_ = SendContextState::WAIT_GPU_READY;
    ctx->in_use.store(false, std::memory_order_release);
}

void release_slot(RecvContext* ctx)
{
    ctx->op_state.reset();
    ctx->expected_mask = 0;
    ctx->finished_mask.store(0, std::memory_order_release);
    ctx->generation.store(0, std::memory_order_release);
    ctx->state_ = RecvContextState::INIT_SEND_META;
    ctx->in_use.store(false, std::memory_order_release);
}

void drain_ring(jring_t* ring, void** scratch, uint32_t scratch_len)
{
    while (jring_dequeue_burst(ring, scratch, scratch_len, nullptr) > 0) {}
}

void update_op_completion(const std::shared_ptr<EndpointOpState>& op_state, int32_t status)
{
    if (!op_state) {
        return;
    }
    if (status != RDMAAssign::SUCCESS) {
        int32_t expected = RDMAAssign::SUCCESS;
        op_state->completion_status.compare_exchange_strong(
            expected, status, std::memory_order_release, std::memory_order_relaxed);
    }
}

}  // namespace

// ============================================================
// Constructor & Setup
// ============================================================

RDMAEndpoint::RDMAEndpoint(std::shared_ptr<RDMAMemoryPool> pool, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    local_pool_(pool), num_qp_(num_qp)
{
    if (!local_pool_) {
        SLIME_ABORT("Memory Pool cannot be null");
    }
    ctx_ = local_pool_->context();
    init(worker);
}

RDMAEndpoint::RDMAEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    ctx_(ctx), num_qp_(num_qp)
{
    if (!ctx_) {
        SLIME_ABORT("RDMA Context cannot be null");
    }
    local_pool_ = std::make_shared<RDMAMemoryPool>(ctx);
    init(worker);
}

void RDMAEndpoint::init(std::shared_ptr<RDMAWorker> worker)
{
    if (not ctx_)
        SLIME_ABORT("No NIC Resources");

    // Always create a new Remote Pool (Independent)
    remote_pool_ = std::make_shared<RDMARemoteMemoryPool>();

    worker_ = worker ? worker : GlobalWorkerManager::instance().get_default_worker(socketId(ctx_->device_name_));

    SLIME_LOG_INFO("Initializing RDMA Endpoint. QPs: ", num_qp_, ", Token Bucket: ", SLIME_MAX_SEND_WR);
    SLIME_LOG_INFO("bypass Signal: ", SLIME_BYPASS_DEVICE_SIGNAL);
    if (SLIME_BYPASS_DEVICE_SIGNAL)
        bypass_signal_ = true;

    // Aggregation logic is not supported in V0 Send/Recv mode.
    SLIME_ASSERT(1 == SLIME_AGG_QP_NUM, "cannot aggqp when sendrecv");
    SLIME_ASSERT(64 > SLIME_QP_NUM, "QP NUM must less than 64");

    // ============================================================
    // Initialize meta_pool (per-endpoint, sys buffers, borrows PD from local_pool_)
    // ============================================================
    meta_pool_ = std::make_shared<RDMAMemoryPool>(local_pool_);

    // ============================================================
    // Initialize IO Endpoint Components
    // ============================================================

    io_data_channel_ = std::make_shared<RDMAChannel>(local_pool_, remote_pool_);
    io_data_channel_->init(ctx_, num_qp_, 0);

    for (int i = 0; i < num_qp_; ++i) {
        token_bucket_[i].store(SLIME_MAX_SEND_WR);
    }

    size_t ring_size        = SLIME_MAX_IO_FIFO_DEPTH;
    read_write_buffer_ring_ = createRing("io_rw_ring", ring_size);
    imm_recv_buffer_ring_   = createRing("io_recv_ring", ring_size);

    void* raw_rw_ctx = nullptr;
    if (posix_memalign(&raw_rw_ctx, 64, sizeof(ReadWriteContext) * SLIME_MAX_IO_FIFO_DEPTH) != 0) {
        throw std::runtime_error("Failed to allocate memory for ReadWriteContext pool");
    }
    read_write_ctx_pool_ = static_cast<ReadWriteContext*>(raw_rw_ctx);

    void* raw_recv_ctx = nullptr;
    if (posix_memalign(&raw_recv_ctx, 64, sizeof(ImmRecvContext) * SLIME_MAX_IO_FIFO_DEPTH) != 0) {
        free(raw_rw_ctx);
        throw std::runtime_error("Failed to allocate memory for ImmRecvContext pool");
    }
    imm_recv_ctx_pool_ = static_cast<ImmRecvContext*>(raw_recv_ctx);

    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        new (&read_write_ctx_pool_[i]) ReadWriteContext();
        read_write_ctx_pool_[i].assigns_.resize(num_qp_);
        read_write_ctx_pool_[i].finished_qp_mask.store(0);

        new (&imm_recv_ctx_pool_[i]) ImmRecvContext();
        imm_recv_ctx_pool_[i].assigns_.resize(num_qp_);
    }

    void* io_dummy_mem = nullptr;
    if (posix_memalign(&io_dummy_mem, 64, sizeof(int64_t)) != 0) {
        throw std::runtime_error("Failed to allocate io_dummy memory");
    }
    io_dummy_ = (int64_t*)io_dummy_mem;

    io_dummy_handle_ =
        meta_pool_->registerMemoryRegion(reinterpret_cast<uintptr_t>(io_dummy_), sizeof(int64_t), "sys.io_dummy");

    // ============================================================
    // Initialize Msg Endpoint Components
    // ============================================================

    // Allocate dummy buffer for Immediate Data payload or signaling.
    void* msg_dummy_mem = nullptr;
    if (posix_memalign(&msg_dummy_mem, 64, sizeof(int64_t)) != 0)
        throw std::runtime_error("msg_dummy alloc fail");
    msg_dummy_ = (int64_t*)msg_dummy_mem;

    // Allocate context pools aligned to cache lines.
    // Coalesced allocation for Send and Recv Contexts
    size_t send_pool_size = sizeof(SendContext) * SLIME_MAX_MSG_FIFO_DEPTH;
    size_t recv_pool_size = sizeof(RecvContext) * SLIME_MAX_MSG_FIFO_DEPTH;
    size_t total_size     = send_pool_size + recv_pool_size;

    void* raw_pool_ptr = nullptr;
    if (posix_memalign(&raw_pool_ptr, 64, total_size) != 0)
        throw std::runtime_error("context pool alloc fail");

    // Assign pointers
    send_ctx_pool_ = static_cast<SendContext*>(raw_pool_ptr);
    // Recv pool follows Send pool immediately
    recv_ctx_pool_ = reinterpret_cast<RecvContext*>(static_cast<char*>(raw_pool_ptr) + send_pool_size);

    // Initialize Signals
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        new (&send_ctx_pool_[i]) SendContext();

        new (&recv_ctx_pool_[i]) RecvContext();
    }

    // Register Memory Regions (MR) upfront on meta_pool_
    msg_dummy_handle_ =
        meta_pool_->registerMemoryRegion(reinterpret_cast<uintptr_t>(msg_dummy_), sizeof(int64_t), "sys.msg_dummy");

    // Register the single large block
    send_ctx_handle_ =
        meta_pool_->registerMemoryRegion(reinterpret_cast<uintptr_t>(send_ctx_pool_), total_size, "sys.send_ctx");

    // Calculate offset of remote_meta_info_ at runtime
    send_ctx_meta_offset_ = reinterpret_cast<uintptr_t>(&(send_ctx_pool_[0].remote_meta_info_))
                            - reinterpret_cast<uintptr_t>(send_ctx_pool_);

    SLIME_LOG_INFO("Endpoint initialized. Send/Recv Pool Coalesced. Meta Offset: ", send_ctx_meta_offset_);

    meta_channel_     = std::make_unique<RDMAChannel>(local_pool_, remote_pool_);
    msg_data_channel_ = std::make_unique<RDMAChannel>(local_pool_, remote_pool_);

    // Meta channel uses 1 QP (latency sensitive), Data channel uses num_qp_
    meta_channel_->init(ctx_, 1, 256);
    msg_data_channel_->init(ctx_, num_qp_, 0);

    // Initialize Rings. Size is double the depth to handle potential overflow gracefully.
    size_t msg_ring_size = SLIME_MAX_MSG_FIFO_DEPTH * 2;
    send_buffer_ring_    = createRing("send_buf", msg_ring_size);
    recv_buffer_ring_    = createRing("recv_buf", msg_ring_size);

    SLIME_LOG_INFO("RDMA Endpoint Initialization Completed.");
}

RDMAEndpoint::RDMAEndpoint(
    std::string dev_name, int32_t ib_port, std::string link_type, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    RDMAEndpoint(GlobalContextManager::instance().get_context(dev_name, ib_port, link_type), num_qp, worker)
{
}

RDMAEndpoint::~RDMAEndpoint()
{
    try {
        // 1. Stop worker from calling process() on this endpoint
        connected_.store(false, std::memory_order_release);

        // 2. Destroy QPs FIRST — flushes pending WRs back to CQ while
        //    context pools are still valid for the CQ thread to dereference wr_id
        io_data_channel_.reset();
        meta_channel_.reset();
        msg_data_channel_.reset();

        // 3. Now safe to free context pools and rings
        freeRing(read_write_buffer_ring_);
        freeRing(imm_recv_buffer_ring_);

        for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
            read_write_ctx_pool_[i].~ReadWriteContext();
            imm_recv_ctx_pool_[i].~ImmRecvContext();
        }
        free(read_write_ctx_pool_);
        free(imm_recv_ctx_pool_);
        free(io_dummy_);

        free(msg_dummy_);

        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            send_ctx_pool_[i].~SendContext();
            recv_ctx_pool_[i].~RecvContext();
        }
        free(send_ctx_pool_);

        freeRing(send_buffer_ring_);
        freeRing(recv_buffer_ring_);

        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully.");
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Exception in RDMAEndpoint destructor: ", e.what());
    }
}

void RDMAEndpoint::connect(const json& remote_endpoint_info)
{
    // Auto-register remote user MRs from exchanged endpoint info.
    // Sort by original handle to guarantee remote handles match source-side handles.
    if (remote_endpoint_info.contains("mr_info") && !remote_endpoint_info["mr_info"].empty()) {
        std::vector<std::pair<std::string, json>> sorted_mrs;
        for (auto& [name, mr_data] : remote_endpoint_info["mr_info"].items()) {
            sorted_mrs.emplace_back(name, mr_data);
        }
        std::sort(sorted_mrs.begin(), sorted_mrs.end(), [](const auto& a, const auto& b) {
            return a.second.value("handle", 0) < b.second.value("handle", 0);
        });
        for (auto& [name, mr_data] : sorted_mrs) {
            registerOrAccessRemoteMemoryRegion(name, mr_data);
        }
    }

    // Connect IO Endpoint
    if (remote_endpoint_info.contains("io_info")) {
        io_data_channel_->connect(remote_endpoint_info["io_info"]["data_channel_info"]);
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'io_info' in remote info");
    }

    // Connect Msg Endpoint
    if (remote_endpoint_info.contains("msg_info")) {
        SLIME_LOG_INFO("Establishing RDMA Connection...");
        meta_channel_->connect(remote_endpoint_info["msg_info"]["meta_channel_info"]);
        msg_data_channel_->connect(remote_endpoint_info["msg_info"]["data_channel_info"]);

        SLIME_LOG_INFO("Connection Established. Pre-posting RECV requests...");

        // Register the single Remote MR for Meta
        auto      meta_info   = remote_endpoint_info["msg_info"]["remote_meta_base"];
        uintptr_t remote_base = meta_info["addr"].get<uintptr_t>();
        size_t    length      = meta_info["length"].get<size_t>();
        uint32_t  rkey        = meta_info["rkey"].get<uint32_t>();

        int32_t remote_meta_handle = remote_pool_->registerRemoteMemoryRegion(remote_base, length, rkey);

        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            recv_ctx_pool_[i].remote_meta_key_ = remote_meta_handle;
        }

        // Pre-post RECV requests for Meta Channel to avoid RNR on the message path.
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            SendContext*            send_ctx = &(send_ctx_pool_[i]);
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};
            send_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, batch, [send_ctx](int32_t status, int32_t imm) {
                send_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
            });
            meta_channel_->post_recv_batch(0, &(send_ctx->meta_recv_assign_), meta_pool_);
        }

        // Pre-post RECV requests for Data Channel to avoid RNR on the message path.
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            RecvContext* recv_ctx = &(recv_ctx_pool_[i]);
            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                std::vector<Assignment> batch{
                    Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};

                recv_ctx->data_recv_assigns_[qpi].reset(
                    OpCode::RECV, qpi, batch, [recv_ctx, qpi](int32_t status, int32_t imm) {
                        auto op_state = recv_ctx->op_state;
                        if (!op_state) {
                            if (status != 0) {
                                SLIME_LOG_DEBUG("Data Recv flushed during pre-post (likely teardown)");
                            }
                            return;
                        }
                        update_op_completion(op_state, status);
                        if (op_state->signal) {
                            op_state->signal->set_comm_done(qpi);
                        }
                        if (status != 0) {
                            SLIME_LOG_DEBUG("Data Recv flushed during pre-post (likely teardown)");
                        }
                    });
                msg_data_channel_->post_recv_batch(qpi, &(recv_ctx->data_recv_assigns_[qpi]), meta_pool_);
            }
        }

        SLIME_LOG_INFO("RDMA Contexts Launched.");
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'msg_info' in remote info");
    }

    connected_.store(true, std::memory_order_release);
    worker_->addEndpoint(shared_from_this());
}

json RDMAEndpoint::endpointInfo() const
{
    // IO endpoint info
    json io_info = json{{"data_channel_info", io_data_channel_->channelInfo()}};

    // Msg endpoint info (send_ctx is in meta_pool_)
    auto           base_ptr = reinterpret_cast<uintptr_t>(send_ctx_pool_);
    int32_t        handle   = meta_pool_->get_mr_handle(base_ptr);
    struct ibv_mr* mr       = meta_pool_->get_mr_fast(handle);
    SLIME_ASSERT(mr, "Send Context Pool MR not found");

    json msg_info = json{{"meta_channel_info", meta_channel_->channelInfo()},
                         {"data_channel_info", msg_data_channel_->channelInfo()},
                         {"remote_meta_base", {{"addr", base_ptr}, {"rkey", mr->rkey}, {"length", mr->length}}}};

    return json{{"mr_info", local_pool_->mrInfo()}, {"io_info", io_info}, {"msg_info", msg_info}};
}

json RDMAEndpoint::mrInfo() const
{
    return local_pool_->mrInfo();
}

void RDMAEndpoint::shutdown()
{
    connected_.store(false, std::memory_order_release);

    // Manually cancel all pending futures
    cancelAll();

    if (worker_) {
        worker_->removeEndpoint(shared_from_this());
    }
}

// ============================================================
// Memory Management
// ============================================================

int32_t RDMAEndpoint::registerOrAccessMemoryRegion(uintptr_t mr_key, uintptr_t ptr, uintptr_t offset, size_t length)
{
    std::string auto_name = "mr_" + std::to_string(mr_key);
    return local_pool_->registerMemoryRegion(ptr + offset, length, auto_name);
}

int32_t
RDMAEndpoint::registerOrAccessMemoryRegion(const std::string& name, uintptr_t ptr, uintptr_t offset, size_t length)
{
    return local_pool_->registerMemoryRegion(ptr + offset, length, name);
}

int32_t RDMAEndpoint::registerOrAccessRemoteMemoryRegion(const std::string& name, json mr_info)
{
    return remote_pool_->registerRemoteMemoryRegion(name, mr_info);
}

// ============================================================
// IO Endpoint Helper Methods
// ============================================================

void RDMAEndpoint::dummyReset(ImmRecvContext* ctx)
{
    auto     op_state        = ctx->op_state;
    uint64_t slot_generation = ctx->generation.load(std::memory_order_acquire);
    for (int qpi = 0; qpi < num_qp_; ++qpi) {
        ctx->assigns_[qpi].opcode_ = OpCode::RECV;

        ctx->assigns_[qpi].batch_.resize(1);

        ctx->assigns_[qpi].batch_[0].length        = sizeof(*io_dummy_);
        ctx->assigns_[qpi].batch_[0].mr_key        = (uintptr_t)io_dummy_;
        ctx->assigns_[qpi].batch_[0].remote_mr_key = (uintptr_t)io_dummy_;
        ctx->assigns_[qpi].batch_[0].target_offset = 0;
        ctx->assigns_[qpi].batch_[0].source_offset = 0;

        ctx->assigns_[qpi].callback_ = [ctx, op_state, slot_generation, qpi](int32_t status, int32_t imm) {
            if (!slot_generation_matches(ctx, slot_generation)) {
                return;
            }
            update_op_completion(op_state, status);
            if (op_state) {
                op_state->imm_data.store((status == RDMAAssign::SUCCESS) ? imm : 0, std::memory_order_release);
                if (op_state->signal) {
                    op_state->signal->set_comm_done(qpi);
                }
            }
            uint64_t old_mask =
                op_state ? op_state->completion_mask.fetch_or(1ULL << qpi, std::memory_order_acq_rel) : 0;
            uint64_t new_mask = old_mask | (1ULL << qpi);
            if (op_state && new_mask == op_state->expected_mask) {
                release_slot(ctx);
            }
        };

        ctx->assigns_[qpi].is_inline_ = false;

        ctx->assigns_[qpi].imm_data_ = 0;
    }
}

int32_t RDMAEndpoint::dispatchTask(OpCode                             op_code,
                                   const std::vector<assign_tuple_t>& assign,
                                   std::shared_ptr<EndpointOpState>   op_state,
                                   int32_t                            imm_data,
                                   void*                              stream)
{
    size_t req_idx    = 0;
    size_t req_offset = 0;
    size_t total_reqs = assign.size();

    int32_t last_slot = -1;

    while (req_idx < total_reqs) {
        ReadWriteContext* ctx = acquire_slot(rw_slot_id_, read_write_ctx_pool_, SLIME_MAX_IO_FIFO_DEPTH);
        ctx->slot_id          = static_cast<int32_t>(ctx - read_write_ctx_pool_);
        ctx->expected_mask    = (1 << num_qp_) - 1;
        ctx->finished_qp_mask.store(0);
        ctx->op_state = op_state;
        ctx->generation.store(op_state ? op_state->op_id : 0, std::memory_order_release);

        last_slot = ctx->slot_id;

        OpCode slot_op_code = op_code;

        for (size_t i = 0; i < num_qp_; ++i) {
            ctx->assigns_[i].batch_.clear();
            ctx->assigns_[i].batch_.reserve(SLIME_MAX_SEND_WR);
        }

        size_t current_batch_size = 0;

        while (req_idx < total_reqs && current_batch_size < SLIME_MAX_SEND_WR) {

            size_t total_len  = std::get<4>(assign[req_idx]);
            size_t remain_len = total_len - req_offset;
            size_t chunk_len  = std::min(remain_len, SLIME_MAX_LENGTH_PER_ASSIGNMENT);

            bool is_request_tail = (req_offset + chunk_len >= total_len);
            bool is_overall_tail = (req_idx == total_reqs - 1) && is_request_tail;

            if (op_code == OpCode::WRITE_WITH_IMM && !is_overall_tail) {
                slot_op_code = OpCode::WRITE;
            }

            uintptr_t l_base     = std::get<0>(assign[req_idx]);
            uintptr_t r_base     = std::get<1>(assign[req_idx]);
            uintptr_t t_off_base = std::get<2>(assign[req_idx]) + req_offset;
            uintptr_t s_off_base = std::get<3>(assign[req_idx]) + req_offset;

            size_t qp_chunk_size = (chunk_len + num_qp_ - 1) / num_qp_;

            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                size_t qp_rel_off = qpi * qp_chunk_size;
                ctx->assigns_[qpi].batch_.emplace_back();
                auto& assign_elem = ctx->assigns_[qpi].batch_.back();

                if (qp_rel_off >= chunk_len) {
                    assign_elem.length = 0;
                }
                else {
                    assign_elem.length = std::min(qp_chunk_size, chunk_len - qp_rel_off);
                }

                assign_elem.mr_key        = l_base;
                assign_elem.remote_mr_key = r_base;
                assign_elem.target_offset = t_off_base + qp_rel_off;
                assign_elem.source_offset = s_off_base + qp_rel_off;
            }

            current_batch_size++;
            req_offset += chunk_len;

            if (req_offset >= total_len) {
                req_idx++;
                req_offset = 0;
            }
        }
        bool is_final_slot = (req_idx >= total_reqs);

        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            auto& assign      = ctx->assigns_[qpi];
            assign.opcode_    = slot_op_code;
            assign.is_inline_ = false;

            uint64_t slot_generation = ctx->generation.load(std::memory_order_acquire);
            assign.callback_ = [this, ctx, op_state, slot_generation, qpi, is_final_slot](int32_t status, int32_t imm) {
                if (!slot_generation_matches(ctx, slot_generation)) {
                    return;
                }
                update_op_completion(op_state, status);
                uint32_t old_mask = ctx->finished_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                uint32_t new_mask = old_mask | (1u << qpi);

                token_bucket_[qpi].fetch_add(ctx->assigns_[qpi].batch_.size());

                if (is_final_slot && op_state && op_state->signal) {
                    op_state->signal->set_comm_done(qpi);
                }
                if (new_mask == ctx->expected_mask) {
                    release_slot(ctx);
                }
            };

            if (slot_op_code == OpCode::WRITE_WITH_IMM) {
                assign.imm_data_ = imm_data;
            }
        }

        while (jring_enqueue_burst(read_write_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            machnet_pause();
        }
    }

    return last_slot;
}

// ============================================================
// Two-Sided Primitives (Message Passing)
// ============================================================

std::shared_ptr<SendFuture> RDMAEndpoint::send(const chunk_tuple_t& chunk, void* stream_handle)
{
    auto data_ptr = std::get<0>(chunk);
    auto offset   = std::get<1>(chunk);
    auto length   = std::get<2>(chunk);

    storage_view_t view{data_ptr, offset, length};
    int32_t        handle = local_pool_->registerMemoryRegion(data_ptr, length);

    uint32_t     target_mask = (1 << num_qp_) - 1;
    uint64_t     op_id       = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    auto         op_state    = make_op_state(op_id, bypass_signal_, stream_handle, target_mask, true);
    SendContext* s_ctx       = acquire_slot(send_slot_id_, send_ctx_pool_, SLIME_MAX_MSG_FIFO_DEPTH);

    s_ctx->reset();
    s_ctx->slot_id                = static_cast<int64_t>(s_ctx - send_ctx_pool_);
    s_ctx->local_meta_info_.view_ = {data_ptr, offset, length};
    s_ctx->expected_mask          = target_mask;
    s_ctx->op_state               = op_state;
    s_ctx->generation.store(op_id, std::memory_order_release);

    while (jring_enqueue_burst(send_buffer_ring_, (void**)&s_ctx, 1, nullptr) == 0) {
        cpu_relax();
    }

    return std::make_shared<SendFuture>(op_state);
}

std::shared_ptr<RecvFuture> RDMAEndpoint::recv(const chunk_tuple_t& chunk, void* stream_handle)
{
    auto data_ptr = std::get<0>(chunk);
    auto offset   = std::get<1>(chunk);
    auto length   = std::get<2>(chunk);

    storage_view_t view{data_ptr, offset, length};
    int32_t        handle = local_pool_->registerMemoryRegion(data_ptr, length);

    uint32_t     target_mask = (1 << num_qp_) - 1;
    uint64_t     op_id       = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    auto         op_state    = make_op_state(op_id, bypass_signal_, stream_handle, target_mask, true);
    RecvContext* r_ctx       = acquire_slot(msg_recv_slot_id_, recv_ctx_pool_, SLIME_MAX_MSG_FIFO_DEPTH);

    r_ctx->reset();
    r_ctx->slot_id       = static_cast<int64_t>(r_ctx - recv_ctx_pool_);
    r_ctx->view_         = {data_ptr, offset, length};
    r_ctx->expected_mask = target_mask;
    r_ctx->op_state      = op_state;
    r_ctx->generation.store(op_id, std::memory_order_release);

    struct ibv_mr* mr              = local_pool_->get_mr_fast(handle);
    r_ctx->local_meta_info_.r_key_ = mr->rkey;
    r_ctx->local_meta_info_.view_  = {data_ptr, offset, length};

    while (jring_enqueue_burst(recv_buffer_ring_, (void**)&r_ctx, 1, nullptr) == 0) {
        cpu_relax();
    }

    return std::make_shared<RecvFuture>(op_state);
}

// ============================================================
// One-Sided Primitives (RDMA IO)
// ============================================================

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::read(const std::vector<assign_tuple_t>& assign, void* stream)
{
    uint64_t op_id    = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    auto     op_state = make_op_state(op_id, false, stream, (1 << num_qp_) - 1, true);
    if (assign.empty()) {
        if (op_state->signal) {
            op_state->signal->force_complete();
        }
        return std::make_shared<ReadWriteFuture>(op_state);
    }
    dispatchTask(OpCode::READ, assign, op_state, 0, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::write(const std::vector<assign_tuple_t>& assign, void* stream)
{
    uint64_t op_id    = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    auto     op_state = make_op_state(op_id, false, stream, (1 << num_qp_) - 1, true);
    if (assign.empty()) {
        if (op_state->signal) {
            op_state->signal->force_complete();
        }
        return std::make_shared<ReadWriteFuture>(op_state);
    }
    dispatchTask(OpCode::WRITE, assign, op_state, 0, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ReadWriteFuture>
RDMAEndpoint::writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    uint64_t op_id    = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    auto     op_state = make_op_state(op_id, false, stream, (1 << num_qp_) - 1, true);
    if (assign.empty()) {
        if (op_state->signal) {
            op_state->signal->force_complete();
        }
        return std::make_shared<ReadWriteFuture>(op_state);
    }
    dispatchTask(OpCode::WRITE_WITH_IMM, assign, op_state, imm_data, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ImmRecvFuture> RDMAEndpoint::immRecv(void* stream)
{
    uint64_t        op_id    = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    auto            op_state = make_op_state(op_id, false, stream, (1 << num_qp_) - 1, false);
    ImmRecvContext* ctx      = acquire_slot(io_recv_slot_id_, imm_recv_ctx_pool_, SLIME_MAX_IO_FIFO_DEPTH);

    ctx->slot_id       = static_cast<int32_t>(ctx - imm_recv_ctx_pool_);
    ctx->expected_mask = (1 << num_qp_) - 1;
    ctx->finished_qp_mask.store(0, std::memory_order_release);
    ctx->op_state = op_state;
    ctx->generation.store(op_id, std::memory_order_release);
    dummyReset(ctx);

    for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
        io_data_channel_->post_recv_batch(qpi, &(ctx->assigns_[qpi]), meta_pool_);
    }
    ctx->state_ = IOContextState::POSTED;

    return std::make_shared<ImmRecvFuture>(op_state);
}

// ============================================================
// Progress Engine - IO Operations
// ============================================================

int32_t RDMAEndpoint::readWriteProcess()
{
    int32_t work_done = 0;

    int n_rw = jring_dequeue_burst(read_write_buffer_ring_, io_burst_buf_, IO_BURST_SIZE, nullptr);
    if (n_rw > 0) {
        for (int i = 0; i < n_rw; ++i) {
            auto* ctx = (ReadWriteContext*)io_burst_buf_[i];
            pending_rw_queue_.push_back(ctx);
        }
    }

    while (!pending_rw_queue_.empty()) {
        auto* ctx = pending_rw_queue_.front();

        if (!slot_is_active(ctx)) {
            pending_rw_queue_.pop_front();
            continue;
        }

        if (!ctx->op_state || !ctx->op_state->signal || !ctx->op_state->signal->is_gpu_ready()) {
            break;
        }

        bool has_token = true;
        for (int qpi = 0; qpi < num_qp_; ++qpi) {
            if (token_bucket_[qpi].load(std::memory_order_acquire) < ctx->assigns_[qpi].batch_.size()) {
                has_token = false;
                break;
            }
        }
        if (not has_token)
            break;

        pending_rw_queue_.pop_front();

        work_done++;

        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            token_bucket_[qpi].fetch_sub(ctx->assigns_[qpi].batch_.size(), std::memory_order_release);
            io_data_channel_->post_rc_oneside_batch(qpi, &(ctx->assigns_[qpi]), local_pool_);
        }

        ctx->state_ = IOContextState::POSTED;
    }
    return work_done;
}

int32_t RDMAEndpoint::immRecvProcess()
{
    return 0;
}

// ============================================================
// Progress Engine - Message Operations
// ============================================================

int32_t RDMAEndpoint::sendProcess()
{
    int work_done = 0;

    int n = jring_dequeue_burst(send_buffer_ring_, send_new_burst_buf_, BURST_SIZE, nullptr);
    if (n > 0) {
        work_done += n;
        for (int i = 0; i < n; ++i) {
            auto* s_ctx = (SendContext*)send_new_burst_buf_[i];
            pending_send_queue_.push_back(s_ctx);
        }
    }

    while (!pending_send_queue_.empty() && !slot_is_active(pending_send_queue_.front())) {
        pending_send_queue_.pop_front();
    }

    auto it = pending_send_queue_.begin();

    if (it != pending_send_queue_.end()) {
        SendContext* s_ctx          = *it;
        bool         task_completed = false;

        switch (s_ctx->state_) {
            case SendContextState::WAIT_GPU_READY: {
                if (s_ctx->op_state && s_ctx->op_state->signal && s_ctx->op_state->signal->is_gpu_ready()) {
                    s_ctx->state_ = SendContextState::WAIT_META;
                    goto CHECK_META_READY;
                }
                break;
            }

            CHECK_META_READY:
            case SendContextState::WAIT_META: {
                if (s_ctx->meta_arrived_flag_.val.load(std::memory_order_acquire)) {
                    s_ctx->meta_arrived_flag_.val.store(false, std::memory_order_release);

                    std::vector<Assignment> meta_batch{
                        Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};
                    s_ctx->meta_recv_assign_.reset(
                        OpCode::RECV, 0, meta_batch, [this, s_ctx](int32_t status, int32_t imm) {
                            s_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
                        });
                    meta_channel_->post_recv_batch(0, &(s_ctx->meta_recv_assign_), meta_pool_);

                    s_ctx->state_ = SendContextState::POST_DATA_SEND;

                    s_ctx->state_ = SendContextState::POST_DATA_SEND;

                    int32_t remote_data_handle =
                        remote_pool_->registerRemoteMemoryRegion(s_ctx->remote_meta_info_.view_.data_ptr,
                                                                 s_ctx->remote_meta_info_.view_.length,
                                                                 s_ctx->remote_meta_info_.r_key_);

                    int32_t local_data_handle = local_pool_->get_mr_handle(s_ctx->local_meta_info_.view_.data_ptr);

                    size_t   total_len       = s_ctx->remote_meta_info_.view_.length;
                    size_t   chunk_size      = (total_len + num_qp_ - 1) / num_qp_;
                    auto     op_state        = s_ctx->op_state;
                    uint64_t slot_generation = s_ctx->generation.load(std::memory_order_acquire);

                    for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                        size_t offset      = qpi * chunk_size;
                        size_t current_len = 0;

                        if (offset < total_len) {
                            current_len = std::min(chunk_size, total_len - offset);
                        }
                        else {
                            current_len = 0;
                            offset      = 0;
                        }

                        // Use handles (not raw ptrs) for post_rc_oneside_batch fast-path lookup
                        Assignment      assign(local_data_handle, remote_data_handle, offset, offset, current_len);
                        AssignmentBatch batch{assign};

                        s_ctx->data_send_assigns_[qpi].reset(
                            OpCode::WRITE_WITH_IMM,
                            qpi,
                            batch,
                            [s_ctx, op_state, slot_generation, qpi](int32_t stat, int32_t imm_data) {
                                if (!slot_generation_matches(s_ctx, slot_generation)) {
                                    return;
                                }
                                update_op_completion(op_state, stat);
                                if (op_state && op_state->signal) {
                                    op_state->signal->set_comm_done(qpi);
                                }
                                uint64_t old_mask =
                                    op_state ?
                                        op_state->completion_mask.fetch_or(1ULL << qpi, std::memory_order_acq_rel) :
                                        0;
                                uint64_t new_mask = old_mask | (1ULL << qpi);
                                if (op_state && new_mask == op_state->expected_mask) {
                                    release_slot(s_ctx);
                                }
                            },
                            false);

                        msg_data_channel_->post_rc_oneside_batch(qpi, &(s_ctx->data_send_assigns_[qpi]), local_pool_);
                    }

                    task_completed = true;
                }
                break;
            }

            default:
                break;
        }

        if (task_completed) {
            pending_send_queue_.pop_front();
            work_done++;
        }
    }
    return work_done;
}

int32_t RDMAEndpoint::recvProcess()
{
    int work_done = 0;

    int n = jring_dequeue_burst(recv_buffer_ring_, recv_new_burst_buf_, BURST_SIZE, nullptr);
    if (n > 0) {
        work_done += n;
        for (int i = 0; i < n; ++i) {
            auto* r_ctx   = (RecvContext*)recv_new_burst_buf_[i];
            r_ctx->state_ = RecvContextState::WAIT_GPU_BUF;
            pending_recv_queue_.push_back(r_ctx);
        }
    }

    while (!pending_recv_queue_.empty() && !slot_is_active(pending_recv_queue_.front())) {
        pending_recv_queue_.pop_front();
    }

    auto it = pending_recv_queue_.begin();
    if (it != pending_recv_queue_.end()) {
        RecvContext* r_ctx          = *it;
        bool         task_completed = false;

        switch (r_ctx->state_) {
            case RecvContextState::WAIT_GPU_BUF: {
                if (r_ctx->op_state && r_ctx->op_state->signal && r_ctx->op_state->signal->is_gpu_ready()) {
                    r_ctx->state_ = RecvContextState::INIT_SEND_META;
                    goto SEND_META;
                }
                break;
            }

            SEND_META:
            case RecvContextState::INIT_SEND_META: {
                auto     op_state        = r_ctx->op_state;
                uint64_t slot_generation = r_ctx->generation.load(std::memory_order_acquire);
                for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                    std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, 8)};
                    r_ctx->data_recv_assigns_[qpi].reset(
                        OpCode::RECV, qpi, batch, [r_ctx, op_state, slot_generation, qpi](int32_t status, int32_t imm) {
                            if (!slot_generation_matches(r_ctx, slot_generation)) {
                                return;
                            }
                            update_op_completion(op_state, status);
                            if (op_state && op_state->signal) {
                                op_state->signal->set_comm_done(qpi);
                            }
                            if (status != 0) {
                                SLIME_LOG_DEBUG("Data Recv flushed during completion (likely teardown)");
                            }
                            uint64_t old_mask =
                                op_state ? op_state->completion_mask.fetch_or(1ULL << qpi, std::memory_order_acq_rel) :
                                           0;
                            uint64_t new_mask = old_mask | (1ULL << qpi);
                            if (op_state && new_mask == op_state->expected_mask) {
                                release_slot(r_ctx);
                            }
                        });
                    msg_data_channel_->post_recv_batch(qpi, &(r_ctx->data_recv_assigns_[qpi]), meta_pool_);
                }

                int slot = r_ctx->slot_id;

                uintptr_t local_meta_addr = reinterpret_cast<uintptr_t>(&(r_ctx->local_meta_info_));
                uintptr_t send_pool_base  = reinterpret_cast<uintptr_t>(send_ctx_pool_);
                uint64_t  local_offset    = local_meta_addr - send_pool_base;

                uint64_t remote_offset = slot * sizeof(SendContext) + send_ctx_meta_offset_;

                // Use handles (not raw ptrs): send_ctx_handle_ for local, remote_meta_key_ already a handle
                Assignment assign(
                    send_ctx_handle_, r_ctx->remote_meta_key_, remote_offset, local_offset, sizeof(meta_info_t));
                AssignmentBatch assign_batch{assign};

                r_ctx->meta_send_assign_.reset(OpCode::WRITE_WITH_IMM, 0, assign_batch, nullptr, true);
                meta_channel_->post_rc_oneside_batch(0, &(r_ctx->meta_send_assign_), meta_pool_);

                r_ctx->state_  = RecvContextState::WAIT_GPU_BUF;
                task_completed = true;
                break;
            }

            default:
                break;
        }

        if (task_completed) {
            pending_recv_queue_.pop_front();
            work_done++;
        }
    }

    return work_done;
}

// ============================================================
// Main Process Loop
// ============================================================

int32_t RDMAEndpoint::process()
{
    if (connected_.load(std::memory_order_acquire)) {
        return readWriteProcess() + immRecvProcess() + sendProcess() + recvProcess();
    }
    return 0;
}

// ============================================================
// Cleanup
// ============================================================

void RDMAEndpoint::cancelAll()
{
    pending_rw_queue_.clear();
    pending_send_queue_.clear();
    pending_recv_queue_.clear();
    drain_ring(read_write_buffer_ring_, io_burst_buf_, IO_BURST_SIZE);
    drain_ring(imm_recv_buffer_ring_, io_burst_buf_, IO_BURST_SIZE);
    drain_ring(send_buffer_ring_, send_new_burst_buf_, BURST_SIZE);
    drain_ring(recv_buffer_ring_, recv_new_burst_buf_, BURST_SIZE);

    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        if (read_write_ctx_pool_ && read_write_ctx_pool_[i].op_state && read_write_ctx_pool_[i].op_state->signal) {
            read_write_ctx_pool_[i].op_state->completion_status.store(RDMAAssign::FAILED, std::memory_order_release);
            read_write_ctx_pool_[i].op_state->signal->force_complete();
            release_slot(&read_write_ctx_pool_[i]);
        }
    }
    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        if (imm_recv_ctx_pool_ && imm_recv_ctx_pool_[i].op_state && imm_recv_ctx_pool_[i].op_state->signal) {
            imm_recv_ctx_pool_[i].op_state->completion_status.store(RDMAAssign::FAILED, std::memory_order_release);
            imm_recv_ctx_pool_[i].op_state->signal->force_complete();
            release_slot(&imm_recv_ctx_pool_[i]);
        }
    }
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        if (send_ctx_pool_ && send_ctx_pool_[i].op_state && send_ctx_pool_[i].op_state->signal) {
            send_ctx_pool_[i].op_state->completion_status.store(RDMAAssign::FAILED, std::memory_order_release);
            send_ctx_pool_[i].op_state->signal->force_complete();
            release_slot(&send_ctx_pool_[i]);
        }
    }
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        if (recv_ctx_pool_ && recv_ctx_pool_[i].op_state && recv_ctx_pool_[i].op_state->signal) {
            recv_ctx_pool_[i].op_state->completion_status.store(RDMAAssign::FAILED, std::memory_order_release);
            recv_ctx_pool_[i].op_state->signal->force_complete();
            release_slot(&recv_ctx_pool_[i]);
        }
    }
}

}  // namespace dlslime
