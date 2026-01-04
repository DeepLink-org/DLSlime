#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "dlslime/device/host/host_signal.h"
#include "dlslime/device/signal.h"
#include "dlslime/engine/assignment.h"
#include "dlslime/jring.h"
#include "dlslime/json.hpp"
#include "memory_pool.h"
#include "rdma_assignment.h"
#include "rdma_channel.h"
#include "rdma_common.h"
#include "rdma_context.h"

namespace dlslime {

using json = nlohmann::json;

class SendFuture;
class RecvFuture;

static const int BURST_SIZE = 128;

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

    std::shared_ptr<dlslime::device::DeviceSignal> signal;

    uint64_t expected_mask = 0;

    void reset()
    {
        expected_mask = 0;
        state_        = SendContextState::WAIT_GPU_READY;
        signal->reset_all();
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

    std::shared_ptr<dlslime::device::DeviceSignal> signal;

    uint64_t expected_mask = 0;

    void reset()
    {
        expected_mask = 0;
        state_        = RecvContextState::INIT_SEND_META;
        signal->reset_all();
    }
};

class RDMAMsgEndpoint: public std::enable_shared_from_this<RDMAMsgEndpoint> {
    friend class RDMAWorker;

public:
    explicit RDMAMsgEndpoint(std::shared_ptr<RDMAContext>    ctx,
                             std::shared_ptr<RDMAMemoryPool> memory_pool,
                             size_t                          qp_nums);

    ~RDMAMsgEndpoint();

    void connect(const json& remote_endpoint_info);

    json endpointInfo() const;

    std::shared_ptr<SendFuture> send(const chunk_tuple_t& chunk, void* stream_handler);

    std::shared_ptr<RecvFuture> recv(const chunk_tuple_t& chunk, void* stream_handler);

    void cancelAll();

    int32_t process();

private:
    bool bypass_signal_{false};

    int64_t num_qp_;

    std::shared_ptr<RDMAContext>    ctx_;
    std::shared_ptr<RDMAMemoryPool> memory_pool_;

    std::unique_ptr<RDMAChannel> meta_channel_;
    std::unique_ptr<RDMAChannel> data_channel_;

    // --- jring_t* Lock-free Queues ---
    jring_t* send_buffer_ring_;
    jring_t* recv_buffer_ring_;

    // Context Pools to avoid dynamic allocation
    SendContext* send_ctx_pool_;
    RecvContext* recv_ctx_pool_;

    std::vector<std::shared_ptr<SendFuture>> send_future_pool_;
    std::vector<std::shared_ptr<RecvFuture>> recv_future_pool_;

    std::deque<SendContext*> pending_send_queue_;
    std::deque<RecvContext*> pending_recv_queue_;

    std::atomic<uint64_t> send_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    void* send_new_burst_buf_[BURST_SIZE];
    void* recv_new_burst_buf_[BURST_SIZE];

    int32_t sendProcess();
    int32_t recvProcess();

    size_t send_ctx_meta_offset_{0};

    int64_t* dummy_;
};

}  // namespace dlslime
