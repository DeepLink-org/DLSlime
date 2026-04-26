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
#include <optional>
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
        read_write_ctx_pool_[i].signal = dlslime::device::createSignal(false);
        read_write_ctx_pool_[i].assigns_.resize(num_qp_);
        read_write_ctx_pool_[i].finished_qp_mask.store(0);

        new (&imm_recv_ctx_pool_[i]) ImmRecvContext();
        imm_recv_ctx_pool_[i].signal = dlslime::device::createSignal(false);
        imm_recv_ctx_pool_[i].assigns_.resize(num_qp_);
    }

    for (size_t i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        read_write_future_pool_.push_back(std::make_shared<ReadWriteFuture>(&(read_write_ctx_pool_[i])));
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
        send_ctx_pool_[i].signal = dlslime::device::createSignal(bypass_signal_);

        new (&recv_ctx_pool_[i]) RecvContext();
        recv_ctx_pool_[i].signal = dlslime::device::createSignal(bypass_signal_);
    }

    for (size_t i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        send_future_pool_.push_back(std::make_shared<SendFuture>(&(send_ctx_pool_[i])));
        recv_future_pool_.push_back(std::make_shared<RecvFuture>(&(recv_ctx_pool_[i])));
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
        postImmRecvWindow();
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

        // Pre-post RECV requests for Meta Channel
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            SendContext*            send_ctx = &(send_ctx_pool_[i]);
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};
            send_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, batch, [send_ctx](int32_t status, int32_t imm) {
                send_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
            });
            meta_channel_->post_recv_batch(0, &(send_ctx->meta_recv_assign_), meta_pool_);
        }

        // Pre-post RECV requests for Data Channel
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            RecvContext* recv_ctx = &(recv_ctx_pool_[i]);
            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                std::vector<Assignment> batch{
                    Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};

                recv_ctx->data_recv_assigns_[qpi].reset(
                    OpCode::RECV, qpi, batch, [recv_ctx, qpi](int32_t status, int32_t imm) {
                        if (status == 0) {
                            recv_ctx->signal->set_comm_done(qpi);
                        }
                        else {
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
    for (int qpi = 0; qpi < num_qp_; ++qpi) {
        ctx->assigns_[qpi].opcode_ = OpCode::RECV;

        ctx->assigns_[qpi].batch_.resize(1);

        ctx->assigns_[qpi].batch_[0].length        = sizeof(*io_dummy_);
        ctx->assigns_[qpi].batch_[0].mr_key        = (uintptr_t)io_dummy_;
        ctx->assigns_[qpi].batch_[0].remote_mr_key = (uintptr_t)io_dummy_;
        ctx->assigns_[qpi].batch_[0].target_offset = 0;
        ctx->assigns_[qpi].batch_[0].source_offset = 0;

        ctx->assigns_[qpi].callback_ = [this, ctx, qpi](int32_t status, int32_t imm) {
            if (status != RDMAAssign::SUCCESS) {
                int32_t expected = RDMAAssign::SUCCESS;
                ctx->completion_status.compare_exchange_strong(
                    expected, status, std::memory_order_release, std::memory_order_relaxed);
            }
            else if (qpi == 0) {
                ctx->imm_data.store(imm, std::memory_order_release);
            }

            uint32_t old_mask = ctx->finished_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
            uint32_t new_mask = old_mask | (1u << qpi);
            if (new_mask == ctx->expected_mask) {
                enqueueImmRecvCompletion(ctx);
            }
        };

        ctx->assigns_[qpi].is_inline_ = false;

        ctx->assigns_[qpi].imm_data_ = 0;
    }
}

void RDMAEndpoint::postImmRecvSlot(ImmRecvContext* ctx)
{
    ctx->expected_mask = (1u << num_qp_) - 1;
    ctx->finished_qp_mask.store(0, std::memory_order_release);
    ctx->completion_status.store(RDMAAssign::SUCCESS, std::memory_order_release);
    ctx->imm_data.store(0, std::memory_order_release);

    dummyReset(ctx);

    for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
        io_data_channel_->post_recv_batch(qpi, &(ctx->assigns_[qpi]), meta_pool_);
    }
    ctx->state_ = IOContextState::POSTED;
}

void RDMAEndpoint::postImmRecvWindow()
{
    // Fill the IO receive queues independently of user immRecv() calls. This
    // is the RNR-avoidance window: WRITE_WITH_IMM should find a posted receive
    // even if the application has not yet asked for the next message.
    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        ImmRecvContext* ctx = &imm_recv_ctx_pool_[i];
        ctx->slot_id        = i;
        postImmRecvSlot(ctx);
    }
}

void RDMAEndpoint::completeImmRecvOp(const std::shared_ptr<ImmRecvOpState>& op_state, const ImmRecvEvent& event)
{
    if (!op_state) {
        return;
    }
    op_state->completion_status.store(event.status, std::memory_order_release);
    op_state->imm_data.store(event.imm_data, std::memory_order_release);
    if (SLIME_WITH_TIME_TRACE) {
        op_state->trace_end_ns.store(event.trace_end_ns, std::memory_order_release);
    }
    if (op_state->signal) {
        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            op_state->signal->set_comm_done(qpi);
        }
    }
}

void RDMAEndpoint::enqueueImmRecvCompletion(ImmRecvContext* ctx)
{
    // CQ callbacks run on the polling thread. Copy the reusable transport
    // slot's result into a small event, match it to a user op if possible, and
    // let the endpoint progress loop repost the slot outside the callback.
    ImmRecvEvent event{
        ctx->completion_status.load(std::memory_order_acquire),
        ctx->imm_data.load(std::memory_order_acquire),
        SLIME_WITH_TIME_TRACE ? monotonic_time_ns() : 0,
    };

    std::shared_ptr<ImmRecvOpState> op_state;
    {
        std::lock_guard<std::mutex> guard(imm_recv_mutex_);
        if (!pending_imm_recv_ops_.empty()) {
            op_state = std::move(pending_imm_recv_ops_.front());
            pending_imm_recv_ops_.pop_front();
        }
        else {
            // Queue all events (SUCCESS, FAILED, FLUSH) so that a future
            // immRecv() caller observes the actual completion status rather
            // than blocking indefinitely.
            completed_imm_recv_events_.push_back(event);
        }
        pending_imm_recv_refill_.push_back(ctx);
    }

    if (op_state) {
        completeImmRecvOp(op_state, event);
    }
}

int32_t
RDMAEndpoint::dispatchTask(OpCode op_code, const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    size_t req_idx    = 0;
    size_t req_offset = 0;
    size_t total_reqs = assign.size();

    int32_t last_slot = -1;

    while (req_idx < total_reqs) {

        uint64_t          slot = rw_slot_id_.fetch_add(1, std::memory_order_relaxed) % SLIME_MAX_IO_FIFO_DEPTH;
        ReadWriteContext* ctx  = &read_write_ctx_pool_[slot];
        ctx->slot_id           = slot;
        ctx->expected_mask     = (1 << num_qp_) - 1;
        ctx->finished_qp_mask.store(0);
        ctx->completion_status.store(RDMAAssign::SUCCESS, std::memory_order_release);

        last_slot = (int32_t)slot;

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

            assign.callback_ = [this, ctx, qpi, is_final_slot](int32_t status, int32_t imm) {
                if (status != RDMAAssign::SUCCESS) {
                    int32_t expected = RDMAAssign::SUCCESS;
                    ctx->completion_status.compare_exchange_strong(
                        expected, status, std::memory_order_release, std::memory_order_relaxed);
                }
                uint32_t old_mask = ctx->finished_qp_mask.fetch_or(1 << qpi, std::memory_order_acq_rel);
                uint32_t new_mask = old_mask | (1 << qpi);

                token_bucket_[qpi].fetch_add(ctx->assigns_[qpi].batch_.size());

                if (is_final_slot) {
                    ctx->signal->set_comm_done(qpi);
                }
            };

            if (slot_op_code == OpCode::WRITE_WITH_IMM) {
                assign.imm_data_ = imm_data;
            }
        }

        ctx->signal->reset_all();
        ctx->signal->bind_stream(stream);
        ctx->signal->record_gpu_ready();

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

    uint32_t target_mask = (1 << num_qp_) - 1;
    uint64_t slot        = send_slot_id_.fetch_add(1, std::memory_order_release) % SLIME_MAX_MSG_FIFO_DEPTH;

    SendContext* s_ctx = &(send_ctx_pool_[slot]);

    s_ctx->reset();
    s_ctx->slot_id                = slot;
    s_ctx->local_meta_info_.view_ = {data_ptr, offset, length};
    s_ctx->expected_mask          = target_mask;

    s_ctx->signal->bind_stream(stream_handle);
    s_ctx->signal->record_gpu_ready();

    while (jring_enqueue_burst(send_buffer_ring_, (void**)&s_ctx, 1, nullptr) == 0) {
        cpu_relax();
    }

    return send_future_pool_[slot];
}

std::shared_ptr<RecvFuture> RDMAEndpoint::recv(const chunk_tuple_t& chunk, void* stream_handle)
{
    auto data_ptr = std::get<0>(chunk);
    auto offset   = std::get<1>(chunk);
    auto length   = std::get<2>(chunk);

    storage_view_t view{data_ptr, offset, length};
    int32_t        handle = local_pool_->registerMemoryRegion(data_ptr, length);

    uint32_t target_mask = (1 << num_qp_) - 1;
    uint64_t slot        = msg_recv_slot_id_.fetch_add(1, std::memory_order_release) % SLIME_MAX_MSG_FIFO_DEPTH;

    RecvContext* r_ctx = &(recv_ctx_pool_[slot]);

    r_ctx->reset();
    r_ctx->slot_id       = slot;
    r_ctx->view_         = {data_ptr, offset, length};
    r_ctx->expected_mask = target_mask;

    r_ctx->signal->bind_stream(stream_handle);
    r_ctx->signal->record_gpu_ready();

    struct ibv_mr* mr              = local_pool_->get_mr_fast(handle);
    r_ctx->local_meta_info_.r_key_ = mr->rkey;
    r_ctx->local_meta_info_.view_  = {data_ptr, offset, length};

    while (jring_enqueue_burst(recv_buffer_ring_, (void**)&r_ctx, 1, nullptr) == 0) {
        cpu_relax();
    }

    return recv_future_pool_[slot];
}

// ============================================================
// One-Sided Primitives (RDMA IO)
// ============================================================

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::read(const std::vector<assign_tuple_t>& assign, void* stream)
{
    int32_t slot_id = dispatchTask(OpCode::READ, assign, 0, stream);
    return read_write_future_pool_[slot_id];
}

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::write(const std::vector<assign_tuple_t>& assign, void* stream)
{
    int32_t slot_id = dispatchTask(OpCode::WRITE, assign, 0, stream);
    return read_write_future_pool_[slot_id];
}

std::shared_ptr<ReadWriteFuture>
RDMAEndpoint::writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    int32_t slot_id = dispatchTask(OpCode::WRITE_WITH_IMM, assign, imm_data, stream);
    return read_write_future_pool_[slot_id];
}

std::shared_ptr<ImmRecvFuture> RDMAEndpoint::immRecv(void* stream)
{
    io_recv_slot_id_.fetch_add(1, std::memory_order_relaxed);

    // One user-level receive operation. It may complete immediately from a
    // queued event, or wait in pending_imm_recv_ops_ until a future CQ event
    // arrives. It does not own a hardware receive slot.
    auto op_state               = std::make_shared<ImmRecvOpState>();
    op_state->signal            = dlslime::device::createSignal(false);
    op_state->expected_mask     = (1u << num_qp_) - 1;
    op_state->completion_status = RDMAAssign::SUCCESS;
    op_state->imm_data          = 0;
    if (SLIME_WITH_TIME_TRACE) {
        op_state->trace_start_ns.store(monotonic_time_ns(), std::memory_order_release);
        op_state->trace_end_ns.store(0, std::memory_order_release);
    }
    op_state->signal->reset_all();
    op_state->signal->bind_stream(stream);

    std::optional<ImmRecvEvent> ready_event;
    {
        std::lock_guard<std::mutex> guard(imm_recv_mutex_);
        if (!completed_imm_recv_events_.empty()) {
            ready_event = completed_imm_recv_events_.front();
            completed_imm_recv_events_.pop_front();
        }
        else {
            pending_imm_recv_ops_.push_back(op_state);
        }
    }

    if (ready_event) {
        completeImmRecvOp(op_state, *ready_event);
    }

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

        if (!ctx->signal->is_gpu_ready()) {
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
    int32_t work_done = 0;

    // Repost completed transport slots. Keeping refill out of the CQ callback
    // avoids mutating RDMAAssign while its callback is still executing.
    while (true) {
        ImmRecvContext* ctx = nullptr;
        {
            std::lock_guard<std::mutex> guard(imm_recv_mutex_);
            if (pending_imm_recv_refill_.empty()) {
                break;
            }
            ctx = pending_imm_recv_refill_.front();
            pending_imm_recv_refill_.pop_front();
        }
        postImmRecvSlot(ctx);
        work_done++;
    }

    return work_done;
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

    auto it = pending_send_queue_.begin();

    if (it != pending_send_queue_.end()) {
        SendContext* s_ctx          = *it;
        bool         task_completed = false;

        switch (s_ctx->state_) {
            case SendContextState::WAIT_GPU_READY: {
                if (s_ctx->signal->is_gpu_ready()) {
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

                    size_t total_len  = s_ctx->remote_meta_info_.view_.length;
                    size_t chunk_size = (total_len + num_qp_ - 1) / num_qp_;

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
                            [s_ctx, qpi](int32_t stat, int32_t imm_data) { s_ctx->signal->set_comm_done(qpi); },
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

    auto it = pending_recv_queue_.begin();
    if (it != pending_recv_queue_.end()) {
        RecvContext* r_ctx          = *it;
        bool         task_completed = false;

        switch (r_ctx->state_) {
            case RecvContextState::WAIT_GPU_BUF: {
                if (r_ctx->signal->is_gpu_ready()) {
                    r_ctx->state_ = RecvContextState::INIT_SEND_META;
                    goto SEND_META;
                }
                break;
            }

            SEND_META:
            case RecvContextState::INIT_SEND_META: {
                for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                    std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, 8)};
                    r_ctx->data_recv_assigns_[qpi].reset(
                        OpCode::RECV, qpi, batch, [r_ctx, qpi](int32_t status, int32_t imm) {
                            if (status == 0) {
                                r_ctx->signal->set_comm_done(qpi);
                            }
                            else {
                                SLIME_LOG_DEBUG("Data Recv flushed during completion (likely teardown)");
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
    {
        std::lock_guard<std::mutex> guard(imm_recv_mutex_);
        for (auto& op_state : pending_imm_recv_ops_) {
            if (op_state) {
                op_state->completion_status.store(RDMAAssign::FAILED, std::memory_order_release);
                if (op_state->signal) {
                    op_state->signal->force_complete();
                }
            }
        }
        pending_imm_recv_ops_.clear();
        completed_imm_recv_events_.clear();
        pending_imm_recv_refill_.clear();
    }

    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        if (read_write_ctx_pool_) {
            read_write_ctx_pool_[i].signal->force_complete();
        }
    }
    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        if (imm_recv_ctx_pool_) {
            imm_recv_ctx_pool_[i].signal->force_complete();
        }
    }
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        if (send_ctx_pool_) {
            send_ctx_pool_[i].signal->force_complete();
        }
    }
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        if (recv_ctx_pool_) {
            recv_ctx_pool_[i].signal->force_complete();
        }
    }
}

}  // namespace dlslime
