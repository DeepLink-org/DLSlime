#pragma once

#include "device/host/host_signal.h"
#include "device/signal.h"

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"

#include "engine/rdma/rdma_channel.h"
#include "jring.h"
#include "json.hpp"
#include "rdma_common.h"
#include "rdma_context.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <immintrin.h>

namespace slime {

using json = nlohmann::json;

class RDMABuffer;

static const size_t MAX_FIFO_DEPTH = 4096;
static const int    BURST_SIZE     = 128;

static inline void cpu_relax()
{
    _mm_pause();
}

/**
 * @brief Meta information exchanged between nodes.
 * Aligned to 64 bytes to match Cache Line size, preventing False Sharing.
 */
typedef struct alignas(64) ViewInfo {
    uint32_t       r_key_;
    storage_view_t view_;

    ViewInfo(): r_key_(0), view_() {}  // Default constructor
    ViewInfo(uint32_t r_key, storage_view_t view): r_key_(r_key), view_(view) {}

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
    int64_t                     slot_id;
    std::shared_ptr<RDMABuffer> buffer;

    // PaddedAtomicUint64 meta_arrived_;

    std::shared_ptr<slime::device::DeviceSignal> signal;

    uint64_t expected_mask = 0;

    void reset()
    {
        buffer        = nullptr;
        expected_mask = 0;
    }
};

// Context for Recv Operations
struct alignas(64) RecvContext {
    int64_t                     slot_id;
    std::shared_ptr<RDMABuffer> buffer;

    std::shared_ptr<slime::device::DeviceSignal> signal;

    uint64_t expected_mask = 0;

    void reset()
    {
        buffer        = nullptr;
        expected_mask = 0;
    }
};

// ==========================================
// Class Definition
// ==========================================

class RDMAEndpointV0: public std::enable_shared_from_this<RDMAEndpointV0> {
    friend class RDMABuffer;

public:
    explicit RDMAEndpointV0(std::shared_ptr<RDMAContext> ctx, size_t qp_nums);
    ~RDMAEndpointV0();

    /**
     * @brief Submit a buffer for Send or Recv operation.
     * This method is thread-safe and non-blocking (uses lock-free ring).
     */
    int32_t addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer, void* stream_handle = nullptr);

    /**
     * @brief Establish connection and start proxy threads.
     */
    void connect(const json& remote_endpoint_info);

    inline json endpointInfo() const
    {
        json endpoint_info = json{{"mr_info", ctx_->memory_pool_->mr_info()},
                                  {"meta_channel_info", meta_channel_->channelInfo()},
                                  {"data_channel_info", data_channel_->channelInfo()},
                                  {"remote_meta_key", uintptr_t(remote_meta_info_)}};
        return endpoint_info;
    }

    inline int32_t registerMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length)
    {
        return ctx_->registerMemoryRegion(mr_key, ptr, length);
    }

private:
    bool bypass_signal_{false};

    int64_t num_qp_;

    std::shared_ptr<RDMAContext> ctx_;

    std::unique_ptr<RDMAChannel> meta_channel_;
    std::unique_ptr<RDMAChannel> data_channel_;

    // DMA-able memory regions for metadata exchange
    meta_info_t* remote_meta_info_;
    meta_info_t* local_meta_info_;

    // --- jring_t* Lock-free Queues ---
    jring_t* send_buffer_ring_;
    jring_t* recv_buffer_ring_;

    // Context Pools to avoid dynamic allocation
    std::vector<SendContext> send_ctx_pool_;
    std::vector<RecvContext> recv_ctx_pool_;

    RDMAAssign* meta_send_assign_;
    RDMAAssign* meta_recv_assign_;
    RDMAAssign* data_send_assign_;
    RDMAAssign* data_recv_assign_;

    uintptr_t remote_meta_key_;

    // Scoreboards for signaling completion between RDMA callbacks and Proxy threads
    PaddedAtomicUint64* meta_arrived_scoreboard_;

    std::atomic<uint64_t> send_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    std::atomic<bool> stop_send_proxy_signal_{false};
    std::atomic<bool> stop_recv_proxy_signal_{false};

    std::thread send_proxy_thread_;
    std::thread recv_proxy_thread_;

    void proxyInit();
    void proxyDestroy();

    int32_t sendProxy();
    int32_t recvProxy();

    jring_t* createRing(const char* name, size_t count);
    void     freeRing(jring_t* ring);

    int64_t* dummy_;
};

}  // namespace slime
