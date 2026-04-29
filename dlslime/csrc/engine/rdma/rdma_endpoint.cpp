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
#include "rdma_op_state.h"
#include "rdma_utils.h"
#include "rdma_worker.h"
#include "rdma_worker_pool.h"

namespace dlslime {

namespace {

// Build a fresh EndpointOpState for one user submission. The signal is
// unique to this op so the returned future is never coupled to a reused
// slot's signal state.
std::shared_ptr<EndpointOpState>
makeOpState(uint32_t expected_mask, bool bypass_signal, void* stream, bool trace_start = false)
{
    auto op_state           = std::make_shared<EndpointOpState>();
    op_state->signal        = dlslime::device::createSignal(bypass_signal);
    op_state->expected_mask = expected_mask;
    op_state->completion_mask.store(0, std::memory_order_relaxed);
    op_state->completion_status.store(RDMAAssign::SUCCESS, std::memory_order_relaxed);
    op_state->imm_data.store(0, std::memory_order_relaxed);
    if (trace_start) {
        op_state->trace_start_ns.store(monotonic_time_ns(), std::memory_order_relaxed);
    }
    op_state->trace_end_ns.store(0, std::memory_order_relaxed);

    if (op_state->signal) {
        op_state->signal->reset_all();
        op_state->signal->bind_stream(stream);
    }
    return op_state;
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
    // jring capacity is size-1 (one slot reserved for the head/tail gap).
    // Double the nominal depth so the free ring can actually hold every
    // slot at once.
    rw_free_ring_ = createRing("io_rw_free_ring", ring_size * 2);

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
        read_write_ctx_pool_[i].slot_id = i;
        ReadWriteContext* slot_ptr      = &read_write_ctx_pool_[i];
        while (jring_enqueue_burst(rw_free_ring_, (void**)&slot_ptr, 1, nullptr) == 0) {
            // Unreachable on a freshly-created ring sized to DEPTH, but guard
            // anyway so a future resize does not silently drop slots.
            machnet_pause();
        }

        new (&imm_recv_ctx_pool_[i]) ImmRecvContext();
        imm_recv_ctx_pool_[i].signal = dlslime::device::createSignal(false);
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

    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        new (&send_ctx_pool_[i]) SendContext();
        send_ctx_pool_[i].slot_id = i;

        new (&recv_ctx_pool_[i]) RecvContext();
        recv_ctx_pool_[i].slot_id = i;
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

    // Free-slot rings. jring capacity is size-1, so 2*DEPTH is required
    // to actually hold DEPTH slot pointers.
    send_free_ring_ = createRing("send_free", SLIME_MAX_MSG_FIFO_DEPTH * 2);
    recv_free_ring_ = createRing("recv_free", SLIME_MAX_MSG_FIFO_DEPTH * 2);
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        SendContext* s_slot = &send_ctx_pool_[i];
        RecvContext* r_slot = &recv_ctx_pool_[i];
        while (jring_enqueue_burst(send_free_ring_, (void**)&s_slot, 1, nullptr) == 0) {
            machnet_pause();
        }
        while (jring_enqueue_burst(recv_free_ring_, (void**)&r_slot, 1, nullptr) == 0) {
            machnet_pause();
        }
    }

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
        freeRing(rw_free_ring_);

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
        freeRing(send_free_ring_);
        freeRing(recv_free_ring_);

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
        // RNR avoidance: keep the HW RQ primed with transport-owned RECVs
        // so WRITE_WITH_IMM from the peer always finds a posted receive.
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

        // RNR avoidance: keep the message RQ primed so peer WRITE_WITH_IMMs
        // and meta SENDs always find a posted receive. These pre-posted WRs
        // get their callbacks re-bound to the live op_state when a user
        // send() / recv() runs.
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            SendContext*            send_ctx = &(send_ctx_pool_[i]);
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};
            send_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, batch, [send_ctx](int32_t status, int32_t imm) {
                send_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
            });
            meta_channel_->post_recv_batch(0, &(send_ctx->meta_recv_assign_), meta_pool_);
        }

        // Pre-post RECV requests for Data Channel. The callback is a
        // placeholder that fires off slot-local state only — it is replaced
        // when a user recv() binds an op_state during recvProcess().
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            RecvContext* recv_ctx = &(recv_ctx_pool_[i]);
            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                std::vector<Assignment> batch{
                    Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};

                recv_ctx->data_recv_assigns_[qpi].reset(OpCode::RECV, qpi, batch, [](int32_t status, int32_t imm) {
                    // Placeholder. recvProcess() overwrites this callback
                    // with one that closes over the user's op_state before
                    // the peer is signaled to write.
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

    // Force-complete any still-waited futures.
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
// In-flight tracking (for cancelAll)
// ============================================================

void RDMAEndpoint::registerInFlight(const std::shared_ptr<EndpointOpState>& op_state)
{
    std::lock_guard<SpinLock> guard(in_flight_lock_);
    // Opportunistic compaction: drop expired weak_ptrs so the vector does not
    // grow unboundedly with long-lived endpoints.
    if ((in_flight_ops_.size() & 0x3F) == 0) {
        in_flight_ops_.erase(std::remove_if(in_flight_ops_.begin(),
                                            in_flight_ops_.end(),
                                            [](const std::weak_ptr<EndpointOpState>& w) { return w.expired(); }),
                             in_flight_ops_.end());
    }
    in_flight_ops_.emplace_back(op_state);
}

// ============================================================
// Slot acquire / release
// ============================================================
//
// A slot is owned by exactly one in-flight op at a time. Acquisition
// blocks on the free ring if the pool is exhausted — this is explicit
// back-pressure at the API boundary, NOT the modulo stomping that the
// pre-refactor code used. Release is only called once every per-qp
// callback for the owning op has fired, so the HW can never deliver a
// CQE whose wr_id points into a slot that has been reassigned.

ReadWriteContext* RDMAEndpoint::acquireReadWriteSlot()
{
    // jring may batch up to 2 void*s per memcpy; use a 2-wide landing area
    // so static overflow analysis does not flag the unused upper half.
    void* landing[2] = {nullptr, nullptr};
    while (jring_dequeue_burst(rw_free_ring_, landing, 1, nullptr) == 0) {
        machnet_pause();
    }
    ReadWriteContext* ctx = static_cast<ReadWriteContext*>(landing[0]);
    ctx->slot_qp_mask.store(0, std::memory_order_relaxed);
    ctx->slot_qp_expected = (1u << num_qp_) - 1;
    return ctx;
}

SendContext* RDMAEndpoint::acquireSendSlot()
{
    void* landing[2] = {nullptr, nullptr};
    while (jring_dequeue_burst(send_free_ring_, landing, 1, nullptr) == 0) {
        machnet_pause();
    }
    SendContext* ctx = static_cast<SendContext*>(landing[0]);
    ctx->slot_qp_mask.store(0, std::memory_order_relaxed);
    ctx->slot_qp_expected = (1u << num_qp_) - 1;
    ctx->state_           = SendContextState::WAIT_GPU_READY;
    ctx->meta_arrived_flag_.val.store(0, std::memory_order_release);
    return ctx;
}

RecvContext* RDMAEndpoint::acquireRecvSlot()
{
    void* landing[2] = {nullptr, nullptr};
    while (jring_dequeue_burst(recv_free_ring_, landing, 1, nullptr) == 0) {
        machnet_pause();
    }
    RecvContext* ctx = static_cast<RecvContext*>(landing[0]);
    ctx->slot_qp_mask.store(0, std::memory_order_relaxed);
    ctx->slot_qp_expected = (1u << num_qp_) - 1;
    ctx->state_           = RecvContextState::WAIT_GPU_BUF;
    return ctx;
}

void RDMAEndpoint::releaseReadWriteSlot(ReadWriteContext* ctx)
{
    ctx->op_state.reset();
    while (jring_enqueue_burst(rw_free_ring_, (void**)&ctx, 1, nullptr) == 0) {
        machnet_pause();
    }
}

void RDMAEndpoint::releaseSendSlot(SendContext* ctx)
{
    ctx->op_state.reset();
    while (jring_enqueue_burst(send_free_ring_, (void**)&ctx, 1, nullptr) == 0) {
        machnet_pause();
    }
}

void RDMAEndpoint::releaseRecvSlot(RecvContext* ctx)
{
    ctx->op_state.reset();
    while (jring_enqueue_burst(recv_free_ring_, (void**)&ctx, 1, nullptr) == 0) {
        machnet_pause();
    }
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

void RDMAEndpoint::pushRefill(ImmRecvContext* ctx)
{
    // Lock-free MPSC push: CQ callback threads push completed slots.
    ImmRecvContext* old_head = refill_head_.load(std::memory_order_relaxed);
    do {
        ctx->next_refill_.store(old_head, std::memory_order_relaxed);
    } while (!refill_head_.compare_exchange_weak(old_head, ctx, std::memory_order_release, std::memory_order_relaxed));
}

ImmRecvContext* RDMAEndpoint::popAllRefill()
{
    // Atomically grab the entire refill chain. Only the progress thread calls
    // this, so a single exchange is sufficient (no ABA concern).
    return refill_head_.exchange(nullptr, std::memory_order_acquire);
}

void RDMAEndpoint::completeImmRecvOp(const std::shared_ptr<EndpointOpState>& op_state, const ImmRecvEvent& event)
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

    std::shared_ptr<EndpointOpState> op_state;
    {
        std::lock_guard<SpinLock> guard(imm_recv_match_lock_);
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
    }

    // Lock-free push to refill stack — outside any lock.
    pushRefill(ctx);

    if (op_state) {
        completeImmRecvOp(op_state, event);
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

    // One EndpointOpState covers the whole user-level request even when the
    // request is sharded across multiple transport slots.
    op_state->expected_mask = (1u << num_qp_) - 1;
    if (op_state->signal) {
        op_state->signal->reset_all();
        op_state->signal->bind_stream(stream);
        op_state->signal->record_gpu_ready();
    }

    while (req_idx < total_reqs) {

        // Acquire a free slot. Blocks (spins) if the pool is exhausted —
        // back-pressure replaces modulo stomping so we never reassign a
        // slot whose previous op still has WRs in flight.
        ReadWriteContext* ctx = acquireReadWriteSlot();
        ctx->op_state         = op_state;

        last_slot = (int32_t)ctx->slot_id;

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

            // Capture the exact batch size we charged to the token bucket so
            // the refund on completion is correct. With the free-slot ring
            // this is now just defensive — the slot cannot be reused until
            // this callback fires — but keeping the capture costs nothing.
            size_t batch_size = assign.batch_.size();
            // Close over op_state by shared_ptr so the callback owns the
            // op's lifetime until it has fired (rdma_context.cpp moves the
            // callback to its own stack before invoking, breaking the
            // shared_ptr cycle on return).
            assign.callback_ = [this, ctx, qpi, is_final_slot, batch_size, op_state](int32_t status, int32_t imm) {
                if (status != RDMAAssign::SUCCESS) {
                    int32_t expected = RDMAAssign::SUCCESS;
                    op_state->completion_status.compare_exchange_strong(
                        expected, status, std::memory_order_release, std::memory_order_relaxed);
                }

                token_bucket_[qpi].fetch_add(batch_size, std::memory_order_release);

                op_state->completion_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);

                if (is_final_slot && op_state->signal) {
                    op_state->signal->set_comm_done(qpi);
                }

                // Return this slot to the free pool once every qp-level
                // callback for this slot's owning op has fired. Must be
                // last: after this, the slot may be re-acquired by an
                // unrelated op.
                uint32_t mask_before = ctx->slot_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                uint32_t mask_after  = mask_before | (1u << qpi);
                if (mask_after == ctx->slot_qp_expected) {
                    releaseReadWriteSlot(ctx);
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
    (void)handle;

    uint32_t target_mask = (1u << num_qp_) - 1;

    // Blocking slot acquire — the free ring guarantees that ctx is not
    // owned by any in-flight op.
    SendContext* s_ctx = acquireSendSlot();

    // Build a fresh op_state for this send — unique signal + completion
    // fields. The future the caller receives can never observe another
    // send's completion.
    auto op_state = makeOpState(target_mask, bypass_signal_, stream_handle, /*trace_start=*/false);

    s_ctx->op_state               = op_state;
    s_ctx->local_meta_info_.view_ = {data_ptr, offset, length};

    if (op_state->signal) {
        op_state->signal->record_gpu_ready();
    }

    registerInFlight(op_state);

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

    uint32_t target_mask = (1u << num_qp_) - 1;

    RecvContext* r_ctx = acquireRecvSlot();

    auto op_state = makeOpState(target_mask, bypass_signal_, stream_handle, /*trace_start=*/false);

    r_ctx->op_state = op_state;
    r_ctx->view_    = {data_ptr, offset, length};

    struct ibv_mr* mr              = local_pool_->get_mr_fast(handle);
    r_ctx->local_meta_info_.r_key_ = mr->rkey;
    r_ctx->local_meta_info_.view_  = {data_ptr, offset, length};

    if (op_state->signal) {
        op_state->signal->record_gpu_ready();
    }

    registerInFlight(op_state);

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
    auto op_state = makeOpState((1u << num_qp_) - 1, false, stream, /*trace_start=*/false);
    registerInFlight(op_state);
    dispatchTask(OpCode::READ, assign, op_state, 0, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::write(const std::vector<assign_tuple_t>& assign, void* stream)
{
    auto op_state = makeOpState((1u << num_qp_) - 1, false, stream, /*trace_start=*/false);
    registerInFlight(op_state);
    dispatchTask(OpCode::WRITE, assign, op_state, 0, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ReadWriteFuture>
RDMAEndpoint::writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    auto op_state = makeOpState((1u << num_qp_) - 1, false, stream, /*trace_start=*/false);
    registerInFlight(op_state);
    dispatchTask(OpCode::WRITE_WITH_IMM, assign, op_state, imm_data, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ImmRecvFuture> RDMAEndpoint::immRecv(void* stream)
{
    io_recv_slot_id_.fetch_add(1, std::memory_order_relaxed);

    // One user-level receive operation. It may complete immediately from a
    // queued event, or wait in pending_imm_recv_ops_ until a future CQ event
    // arrives. It does not own a hardware receive slot; the transport-owned
    // slots stay permanently posted (RNR-avoidance window).
    auto op_state = makeOpState((1u << num_qp_) - 1, false, stream, /*trace_start=*/SLIME_WITH_TIME_TRACE != 0);

    std::optional<ImmRecvEvent> ready_event;
    {
        std::lock_guard<SpinLock> guard(imm_recv_match_lock_);
        if (!completed_imm_recv_events_.empty()) {
            ready_event = completed_imm_recv_events_.front();
            completed_imm_recv_events_.pop_front();
        }
        else {
            pending_imm_recv_ops_.push_back(op_state);
        }
    }

    registerInFlight(op_state);

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

        auto op_state = ctx->op_state;
        if (!op_state || !op_state->signal) {
            pending_rw_queue_.pop_front();
            continue;
        }
        if (!op_state->signal->is_gpu_ready()) {
            break;
        }

        bool has_token = true;
        for (int qpi = 0; qpi < num_qp_; ++qpi) {
            if (token_bucket_[qpi].load(std::memory_order_acquire) < (int32_t)ctx->assigns_[qpi].batch_.size()) {
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
    }
    return work_done;
}

int32_t RDMAEndpoint::immRecvProcess()
{
    int32_t work_done = 0;

    // Atomically drain the entire lock-free refill stack in one exchange,
    // then batch-repost all completed transport slots without any locking.
    ImmRecvContext* batch = popAllRefill();
    while (batch) {
        ImmRecvContext* next = batch->next_refill_.load(std::memory_order_relaxed);
        batch->next_refill_.store(nullptr, std::memory_order_relaxed);
        postImmRecvSlot(batch);
        batch = next;
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

        auto op_state = s_ctx->op_state;
        if (!op_state) {
            pending_send_queue_.pop_front();
            return work_done;
        }

        switch (s_ctx->state_) {
            case SendContextState::WAIT_GPU_READY: {
                if (op_state->signal && op_state->signal->is_gpu_ready()) {
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
                    s_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, meta_batch, [s_ctx](int32_t status, int32_t imm) {
                        s_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
                    });
                    meta_channel_->post_recv_batch(0, &(s_ctx->meta_recv_assign_), meta_pool_);

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
                            // Close over op_state (not s_ctx) so the future
                            // bound to THIS send sees completion even if the
                            // slot is later reused for a different send. The
                            // s_ctx capture is used only to release the slot
                            // back to the free pool once every qp has fired.
                            [this, s_ctx, op_state, qpi](int32_t stat, int32_t imm_data) {
                                if (stat != RDMAAssign::SUCCESS) {
                                    int32_t expected = RDMAAssign::SUCCESS;
                                    op_state->completion_status.compare_exchange_strong(
                                        expected, stat, std::memory_order_release, std::memory_order_relaxed);
                                }
                                op_state->completion_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                                if (op_state->signal) {
                                    op_state->signal->set_comm_done(qpi);
                                }

                                uint32_t mask_before =
                                    s_ctx->slot_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                                uint32_t mask_after = mask_before | (1u << qpi);
                                if (mask_after == s_ctx->slot_qp_expected) {
                                    releaseSendSlot(s_ctx);
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

    auto it = pending_recv_queue_.begin();
    if (it != pending_recv_queue_.end()) {
        RecvContext* r_ctx          = *it;
        bool         task_completed = false;

        auto op_state = r_ctx->op_state;
        if (!op_state) {
            pending_recv_queue_.pop_front();
            return work_done;
        }

        switch (r_ctx->state_) {
            case RecvContextState::WAIT_GPU_BUF: {
                if (op_state->signal && op_state->signal->is_gpu_ready()) {
                    r_ctx->state_ = RecvContextState::INIT_SEND_META;
                    goto SEND_META;
                }
                break;
            }

            SEND_META:
            case RecvContextState::INIT_SEND_META: {
                // Re-bind the data-recv callbacks to THIS recv's op_state
                // BEFORE we signal the sender (by writing our meta). Since
                // the RECVs were already pre-posted in connect(), the
                // callback mutation here takes effect for the CQE that
                // arrives once the peer writes. RNR-avoidance is preserved:
                // the RQ is never empty.
                for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                    std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, 8)};
                    r_ctx->data_recv_assigns_[qpi].reset(
                        OpCode::RECV, qpi, batch, [this, r_ctx, op_state, qpi](int32_t status, int32_t imm) {
                            if (status == 0) {
                                op_state->completion_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                                if (op_state->signal) {
                                    op_state->signal->set_comm_done(qpi);
                                }
                            }
                            else {
                                int32_t expected = RDMAAssign::SUCCESS;
                                op_state->completion_status.compare_exchange_strong(
                                    expected, status, std::memory_order_release, std::memory_order_relaxed);
                                SLIME_LOG_DEBUG("Data Recv flushed during completion (likely teardown)");
                            }

                            uint32_t mask_before = r_ctx->slot_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                            uint32_t mask_after  = mask_before | (1u << qpi);
                            if (mask_after == r_ctx->slot_qp_expected) {
                                releaseRecvSlot(r_ctx);
                            }
                        });
                    // Refill the RQ so the next peer write still has a
                    // posted receive waiting — RNR guard stays up.
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
    // Drain pending imm-recv user ops: mark failed + wake their signals.
    {
        std::lock_guard<SpinLock> guard(imm_recv_match_lock_);
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
    }

    // Drain the lock-free refill stack.
    popAllRefill();

    // Force-complete every in-flight user op. This includes send / recv /
    // read / write / writeWithImm, so futures waiting on shutdown do not
    // block forever. Stale weak_ptrs are skipped automatically.
    std::vector<std::shared_ptr<EndpointOpState>> live_ops;
    {
        std::lock_guard<SpinLock> guard(in_flight_lock_);
        live_ops.reserve(in_flight_ops_.size());
        for (auto& w : in_flight_ops_) {
            if (auto sp = w.lock()) {
                live_ops.push_back(std::move(sp));
            }
        }
        in_flight_ops_.clear();
    }
    for (auto& op_state : live_ops) {
        int32_t expected = RDMAAssign::SUCCESS;
        op_state->completion_status.compare_exchange_strong(
            expected, RDMAAssign::FAILED, std::memory_order_release, std::memory_order_relaxed);
        if (op_state->signal) {
            op_state->signal->force_complete();
        }
    }

    // Also force-complete transport-owned imm-recv slot signals so any
    // still-pending CQ wakeups on shutdown do not spin forever.
    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        if (imm_recv_ctx_pool_ && imm_recv_ctx_pool_[i].signal) {
            imm_recv_ctx_pool_[i].signal->force_complete();
        }
    }
}

}  // namespace dlslime
