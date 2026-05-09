#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "dlslime/csrc/common/jring.h"
#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/device/signal.h"
#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/utils.h"
#include "memory_pool.h"
#include "rdma_assignment.h"
#include "rdma_channel.h"
#include "rdma_common.h"
#include "rdma_context.h"
#include "rdma_op_state.h"
#include "remote_memory_pool.h"

namespace dlslime {

using json = nlohmann::json;

class SendFuture;
class RecvFuture;
class ReadWriteFuture;
class ImmRecvFuture;

class RDMAWorker;

// ============================================================
// Constants
// ============================================================

constexpr int IO_BURST_SIZE = 32;
constexpr int BURST_SIZE    = 128;

// ============================================================
// Context Structures for IO Operations
// ============================================================

// --- Read/Write Context (Initiator) ---
//
// Transport dispatch slot for the one-sided (read / write / writeWithImm)
// path. Strictly a lease-scoped coordinator: a single in-flight op owns
// the slot from acquire until every per-qp callback has fired.
//
// Responsibilities (slot-level only):
//   - hold the per-qp RDMAAssign storage that wr_id points into
//   - track per-qp callback firings (slot_qp_mask)
//   - carry a back-reference to the current op_state so callbacks can
//     update it
// NON-responsibilities (belong to EndpointOpState):
//   - user-visible completion signal, status, imm_data
//   - op identity / lifetime
struct ReadWriteContext {
    int32_t slot_id;

    std::vector<RDMAAssign> assigns_;

    // The op currently leasing this slot. Reset to nullptr on release.
    std::shared_ptr<EndpointOpState> op_state;

    // Per-qp callback-fired mask. The slot is returned to rw_free_ring_
    // only when this reaches slot_qp_expected, so no wr_id can point
    // into assigns_ after re-lease.
    std::atomic<uint32_t> slot_qp_mask{0};
    uint32_t              slot_qp_expected{0};
};

struct ImmRecvContext {
    // Transport-owned receive slot. These contexts stay posted on the
    // hardware RQ and recycle through a refill stack; user futures do
    // NOT reference them (they observe EndpointOpState instead, matched
    // via pending_imm_recv_ops_ / completed_imm_recv_events_).
    int32_t                                        slot_id;
    std::shared_ptr<dlslime::device::DeviceSignal> signal;
    std::vector<RDMAAssign>                        assigns_;

    uint32_t              expected_mask;
    std::atomic<uint32_t> finished_qp_mask{0};
    std::atomic<int32_t>  completion_status{0};
    std::atomic<int32_t>  imm_data{0};

    // Intrusive pointer for the lock-free MPSC refill stack.
    std::atomic<ImmRecvContext*> next_refill_{nullptr};
};

struct ImmRecvEvent {
    // Small value object that bridges transport completions and user receives.
    // It may be queued briefly if a WRITE_WITH_IMM arrives before immRecv().
    int32_t  status{RDMAAssign::SUCCESS};
    int32_t  imm_data{0};
    uint64_t trace_end_ns{0};
};

// ============================================================
// Context Structures for Message Operations
// ============================================================

enum class SendContextState : uint8_t {
    WAIT_GPU_READY,
    WAIT_META,
    POST_DATA_SEND,
    DONE
};

enum class RecvContextState : uint8_t {
    INIT_SEND_META,
    WAIT_GPU_BUF,
    POST_DATA_RECV,
    DONE
};

/**
 * @brief Meta information exchanged between nodes.
 * Aligned to 64 bytes to match Cache Line size, preventing False Sharing.
 */
typedef struct alignas(64) MetaInfo {
    uint32_t       r_key_;
    storage_view_t view_;

    MetaInfo(): r_key_(0), view_() {}  // Default constructor
    MetaInfo(uint32_t r_key, storage_view_t view): r_key_(r_key), view_(view) {}

    std::string dump()
    {
        // JSON dumping is heavy, strictly use for debugging
        return json{{"r_key_", r_key_},
                    {"view_", {{"ptr", view_.data_ptr}, {"length", view_.length}, {"offset", view_.storage_offset}}}}
            .dump();
    }
} meta_info_t;

struct alignas(64) PaddedAtomicUint64 {
    std::atomic<uint64_t> val{0};
};

// Context for Send Operations
//
// As with ReadWriteContext, a slot is leased from send_free_ring_ at
// submission time and returned only after all per-qp data_send callbacks
// for the owning op have fired. This eliminates modulo-reuse races on
// slot-local state (meta_arrived_flag_, remote_meta_info_, op_state).
struct alignas(64) SendContext {
    int64_t slot_id;

    PaddedAtomicUint64 meta_arrived_flag_;

    meta_info_t local_meta_info_;
    meta_info_t remote_meta_info_;

    RDMAAssign meta_recv_assign_;
    RDMAAssign data_send_assigns_[64];

    // Per-lease state machine (WAIT_GPU_READY → WAIT_META → …).
    SendContextState state_;

    std::shared_ptr<EndpointOpState> op_state;

    std::atomic<uint32_t> slot_qp_mask{0};
    uint32_t              slot_qp_expected{0};
};

// Context for Recv Operations
//
// Leased from recv_free_ring_ at submission; released after all per-qp
// data_recv callbacks fire. See SendContext for the rationale.
struct alignas(64) RecvContext {
    int64_t        slot_id;
    storage_view_t view_;

    meta_info_t local_meta_info_;

    RDMAAssign meta_send_assign_;
    RDMAAssign data_recv_assigns_[64];

    uintptr_t remote_meta_key_;

    // Per-lease state machine (WAIT_GPU_BUF → INIT_SEND_META → …).
    RecvContextState state_;

    std::shared_ptr<EndpointOpState> op_state;

    std::atomic<uint32_t> slot_qp_mask{0};
    uint32_t              slot_qp_expected{0};
};

// ============================================================
// RDMAEndpoint - Unified Endpoint Class
// ============================================================

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {
    friend class RDMAWorker;

public:
    RDMAEndpoint(std::shared_ptr<RDMAMemoryPool> pool, size_t num_qp, std::shared_ptr<RDMAWorker> worker = nullptr);

    RDMAEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp, std::shared_ptr<RDMAWorker> worker = nullptr);

    RDMAEndpoint(std::string                 dev_name  = "",
                 int32_t                     ib_port   = 1,
                 std::string                 link_type = "RoCE",
                 size_t                      num_qp    = 1,
                 std::shared_ptr<RDMAWorker> worker    = nullptr);

    ~RDMAEndpoint();

    void connect(const json& remote_endpoint_info);

    json endpointInfo() const;
    json mrInfo() const;
    void shutdown();

    int32_t registerOrAccessMemoryRegion(uintptr_t mr_key, uintptr_t ptr, uintptr_t, size_t length);
    int32_t registerOrAccessMemoryRegion(const std::string& name, uintptr_t ptr, uintptr_t, size_t length);

    int32_t registerOrAccessRemoteMemoryRegion(const std::string& name, json mr_info);

    // TwoSide Primitive
    std::shared_ptr<SendFuture> send(const chunk_tuple_t& chunk, void* stream_handler);
    std::shared_ptr<RecvFuture> recv(const chunk_tuple_t& chunk, void* stream_handler);

    // OneSide Primitive
    std::shared_ptr<ReadWriteFuture> read(const std::vector<assign_tuple_t>& assign, void* stream);
    std::shared_ptr<ReadWriteFuture> write(const std::vector<assign_tuple_t>& assign, void* stream);
    std::shared_ptr<ReadWriteFuture>
    writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream);

    std::shared_ptr<ImmRecvFuture> immRecv(void* stream = nullptr);

    int32_t process();

    void setId(int64_t id)
    {
        id_.store(id, std::memory_order_relaxed);
    }
    int64_t getId() const
    {
        return id_.load(std::memory_order_relaxed);
    }

    void cancelAll();

    // Expose pools for power users / binding access
    std::shared_ptr<RDMAMemoryPool> get_local_pool()
    {
        return local_pool_;
    }
    std::shared_ptr<RDMARemoteMemoryPool> get_remote_pool()
    {
        return remote_pool_;
    }

private:
    // ============================================================
    // Private Methods - Initialization
    // ============================================================
    void init(std::shared_ptr<RDMAWorker> worker);

    // ============================================================
    // Private Methods - IO Operations
    // ============================================================
    void dummyReset(ImmRecvContext* ctx);
    void postImmRecvSlot(ImmRecvContext* ctx);
    void postImmRecvWindow();
    void completeImmRecvOp(const std::shared_ptr<EndpointOpState>& op_state, const ImmRecvEvent& event);
    void enqueueImmRecvCompletion(ImmRecvContext* ctx);

    /// Lock-free MPSC helpers for the refill stack.
    void            pushRefill(ImmRecvContext* ctx);
    ImmRecvContext* popAllRefill();

    int32_t dispatchTask(OpCode                             op_code,
                         const std::vector<assign_tuple_t>& assign,
                         std::shared_ptr<EndpointOpState>   op_state,
                         int32_t                            imm_data = 0,
                         void*                              stream   = nullptr);

    int32_t readWriteProcess();
    int32_t immRecvProcess();

    // ============================================================
    // Private Methods - Message Operations
    // ============================================================
    int32_t sendProcess();
    int32_t recvProcess();

    // Tracks every EndpointOpState this endpoint still considers in-flight,
    // so cancelAll() can force-complete exactly those operations instead of
    // blasting every pooled slot signal. Weak references so lingering
    // callbacks do not pin completed ops indefinitely.
    void registerInFlight(const std::shared_ptr<EndpointOpState>& op_state);

    // Slot acquire / release helpers. Acquire blocks (spins) if the pool
    // is exhausted, applying back-pressure at the API boundary instead of
    // silently stomping a still-in-flight slot. Release pushes the slot
    // back onto its free ring; it is only called once every per-qp
    // callback for the owning op has fired.
    ReadWriteContext* acquireReadWriteSlot();
    SendContext*      acquireSendSlot();
    RecvContext*      acquireRecvSlot();
    void              releaseReadWriteSlot(ReadWriteContext* ctx);
    void              releaseSendSlot(SendContext* ctx);
    void              releaseRecvSlot(RecvContext* ctx);

    // ============================================================
    // Member Variables - Common
    // ============================================================
    std::atomic<int64_t> id_{-1};
    std::atomic<bool>    connected_{false};

    int obs_nic_id_{-1};  // Observability: registered NIC slot index

    std::shared_ptr<RDMAContext>          ctx_;
    std::shared_ptr<RDMAMemoryPool>       local_pool_;  // user_pool (shared when from PeerAgent)
    std::shared_ptr<RDMAMemoryPool>       meta_pool_;   // per-endpoint, sys buffers (borrows PD from local_pool_)
    std::shared_ptr<RDMARemoteMemoryPool> remote_pool_;

    std::shared_ptr<RDMAWorker> worker_;

    size_t num_qp_;

    // ============================================================
    // Member Variables - IO Operations
    // ============================================================
    std::shared_ptr<RDMAChannel> io_data_channel_;

    ReadWriteContext* read_write_ctx_pool_;
    ImmRecvContext*   imm_recv_ctx_pool_;

    jring_t* read_write_buffer_ring_;
    jring_t* imm_recv_buffer_ring_;

    // Free-slot ring: holds every ReadWriteContext that is idle and safe
    // to lease. Populated at init. A slot leaves on acquire() and
    // re-enters only after every per-qp callback for its current op has
    // fired — never by modulo reuse.
    jring_t* rw_free_ring_;

    std::deque<ReadWriteContext*> pending_rw_queue_;

    std::atomic<uint64_t> rw_slot_id_{0};
    std::atomic<uint64_t> io_recv_slot_id_{0};

    std::atomic<int32_t> token_bucket_[64];

    // Matching queues for the decoupled imm-recv path.
    // Protected by imm_recv_match_lock_ (spinlock, short critical sections).
    SpinLock                                     imm_recv_match_lock_;
    std::deque<std::shared_ptr<EndpointOpState>> pending_imm_recv_ops_;
    std::deque<ImmRecvEvent>                     completed_imm_recv_events_;

    // Lock-free MPSC refill stack. CQ callback threads push via pushRefill(),
    // the progress thread drains via popAllRefill(). No lock needed.
    std::atomic<ImmRecvContext*> refill_head_{nullptr};

    // Scratchpad buffers
    void*    io_burst_buf_[IO_BURST_SIZE];
    int64_t* io_dummy_;

    // ============================================================
    // Member Variables - Message Operations
    // ============================================================
    bool bypass_signal_{false};

    std::unique_ptr<RDMAChannel> meta_channel_;
    std::unique_ptr<RDMAChannel> msg_data_channel_;

    // --- jring_t* Lock-free Queues ---
    jring_t* send_buffer_ring_;
    jring_t* recv_buffer_ring_;

    // Free-slot rings for the two-sided message path. Same invariant as
    // rw_free_ring_: slots are only released back here after every per-qp
    // data-path callback for the owning op has fired.
    jring_t* send_free_ring_;
    jring_t* recv_free_ring_;

    // Context Pools to avoid dynamic allocation
    SendContext* send_ctx_pool_;
    RecvContext* recv_ctx_pool_;

    std::deque<SendContext*> pending_send_queue_;
    std::deque<RecvContext*> pending_recv_queue_;

    std::atomic<uint64_t> send_slot_id_{0};
    std::atomic<uint64_t> msg_recv_slot_id_{0};

    void* send_new_burst_buf_[BURST_SIZE];
    void* recv_new_burst_buf_[BURST_SIZE];

    size_t send_ctx_meta_offset_{0};

    int32_t io_dummy_handle_{-1};
    int32_t msg_dummy_handle_{-1};
    int32_t send_ctx_handle_{-1};

    int64_t* msg_dummy_;

    // ============================================================
    // In-flight op registry (for cancelAll())
    // ============================================================
    SpinLock                                    in_flight_lock_;
    std::vector<std::weak_ptr<EndpointOpState>> in_flight_ops_;
};

}  // namespace dlslime
