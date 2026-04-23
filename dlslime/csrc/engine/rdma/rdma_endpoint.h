#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "dlslime/csrc/common/jring.h"
#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/device/signal.h"
#include "dlslime/csrc/engine/assignment.h"
#include "memory_pool.h"
#include "rdma_assignment.h"
#include "rdma_channel.h"
#include "rdma_common.h"
#include "rdma_context.h"
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

struct EndpointOpState {
    std::shared_ptr<dlslime::device::DeviceSignal> signal;
    uint64_t                                       op_id{0};
    uint32_t                                       expected_mask{0};
    std::atomic<uint64_t>                          completion_mask{0};
    std::atomic<int32_t>                           completion_status{RDMAAssign::SUCCESS};
    std::atomic<int32_t>                           imm_data{0};
};

// ============================================================
// Context Structures for IO Operations
// ============================================================

enum class IOContextState {
    FREE,
    PENDING,
    WAIT_TOKEN,
    POSTED,
    DONE
};

// --- Read/Write Context (Initiator) ---
struct ReadWriteContext {
    int32_t slot_id;

    std::shared_ptr<EndpointOpState> op_state;

    std::vector<RDMAAssign> assigns_;

    uintptr_t local_ptr;
    uintptr_t remote_ptr;
    size_t    length;
    uint32_t  rkey;
    int32_t   imm_data;
    OpCode    op_code;
    uint32_t  expected_mask;

    std::atomic<uint32_t> finished_qp_mask{0};
    std::atomic<uint64_t> generation{0};
    std::atomic<bool>     in_use{false};

    IOContextState state_ = IOContextState::FREE;
};

struct ImmRecvContext {
    int32_t                          slot_id;
    std::shared_ptr<EndpointOpState> op_state;
    std::vector<RDMAAssign>          assigns_;

    uint32_t              expected_mask;
    std::atomic<uint32_t> finished_qp_mask{0};
    std::atomic<uint64_t> generation{0};
    std::atomic<bool>     in_use{false};
    IOContextState        state_ = IOContextState::FREE;
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
struct alignas(64) SendContext {
    int64_t slot_id;

    PaddedAtomicUint64 meta_arrived_flag_;

    meta_info_t local_meta_info_;
    meta_info_t remote_meta_info_;

    RDMAAssign meta_recv_assign_;
    RDMAAssign data_send_assigns_[64];

    SendContextState state_;

    std::shared_ptr<EndpointOpState> op_state;

    uint64_t              expected_mask = 0;
    std::atomic<uint64_t> finished_mask{0};
    std::atomic<uint64_t> generation{0};
    std::atomic<bool>     in_use{false};

    void reset()
    {
        expected_mask = 0;
        finished_mask.store(0, std::memory_order_release);
        op_state.reset();
        meta_arrived_flag_.val.store(0, std::memory_order_release);
        state_ = SendContextState::WAIT_GPU_READY;
    }
};

// Context for Recv Operations
struct alignas(64) RecvContext {
    int64_t        slot_id;
    storage_view_t view_;

    meta_info_t local_meta_info_;

    RDMAAssign meta_send_assign_;
    RDMAAssign data_recv_assigns_[64];

    uintptr_t remote_meta_key_;

    RecvContextState state_;

    std::shared_ptr<EndpointOpState> op_state;

    uint64_t              expected_mask = 0;
    std::atomic<uint64_t> finished_mask{0};
    std::atomic<uint64_t> generation{0};
    std::atomic<bool>     in_use{false};

    void reset()
    {
        expected_mask = 0;
        finished_mask.store(0, std::memory_order_release);
        op_state.reset();
        state_ = RecvContextState::INIT_SEND_META;
    }
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

    int32_t dispatchTask(OpCode op_code,
                         const std::vector<assign_tuple_t>&,
                         std::shared_ptr<EndpointOpState> op_state,
                         int32_t                          imm_data = 0,
                         void*                            stream   = nullptr);

    int32_t readWriteProcess();
    int32_t immRecvProcess();

    // ============================================================
    // Private Methods - Message Operations
    // ============================================================
    int32_t sendProcess();
    int32_t recvProcess();

    // ============================================================
    // Member Variables - Common
    // ============================================================
    std::atomic<int64_t> id_{-1};
    std::atomic<bool>    connected_{false};

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

    std::deque<ReadWriteContext*> pending_rw_queue_;

    std::atomic<uint64_t> rw_slot_id_{0};
    std::atomic<uint64_t> io_recv_slot_id_{0};
    std::atomic<uint64_t> next_op_id_{1};

    std::atomic<int32_t> token_bucket_[64];

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
};

}  // namespace dlslime
