#pragma once

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "jring.h"
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

class RDMABuffer;

/**
 * @brief Configuration constants for V0 Optimization.
 * MAX_FIFO_DEPTH must be a power of 2 for jring compatibility.
 * BURST_SIZE defines the batch size for ring operations to maximize throughput.
 */
static const size_t MAX_FIFO_DEPTH = 1024;
static const int    BURST_SIZE     = 128;

/**
 * @brief Efficient CPU pause instruction (pause/yield).
 * Used in spin-loops to reduce power consumption and hint the CPU pipeline.
 */
static inline void cpu_relax()
{
    _mm_pause();
}

// ==========================================
// V2 Data Structures: Context Pooling
// ==========================================

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

/**
 * @brief Context object for Send operations.
 * Pre-allocated in a pool to avoid malloc overhead during runtime.
 */
struct alignas(64) SendContext {
    int32_t                            slot_id;
    std::shared_ptr<RDMABuffer>        buffer;
    std::shared_ptr<RDMAAssignHandler> assign_handler;

    void reset()
    {
        buffer         = nullptr;
        assign_handler = nullptr;
    }
};

/**
 * @brief Context object for Recv operations.
 */
struct alignas(64) RecvContext {
    int32_t                            slot_id;
    std::shared_ptr<RDMABuffer>        buffer;
    std::shared_ptr<RDMAAssignHandler> assign_handler;

    void reset()
    {
        buffer         = nullptr;
        assign_handler = nullptr;
    }
};

/**
 * @brief Atomic boolean with padding to ensure it occupies a full cache line.
 * This is critical for scoreboards to prevent False Sharing between threads/cores.
 */
struct alignas(64) PaddedAtomicBool {
    std::atomic<bool> val{false};
};

// ==========================================
// Class Definition
// ==========================================

class RDMAEndpointV0: public std::enable_shared_from_this<RDMAEndpointV0> {
    friend class RDMABuffer;

public:
    explicit RDMAEndpointV0(const std::string& dev_name, size_t ib_port, const std::string& link_type, size_t qp_nums);
    ~RDMAEndpointV0();

    /**
     * @brief Submit a buffer for Send or Recv operation.
     * This method is thread-safe and non-blocking (uses lock-free ring).
     */
    int32_t addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer);

    /**
     * @brief Establish connection and start proxy threads.
     */
    void connect(const json& data_ctx_info, const json& meta_ctx_info);

    inline json dataCtxInfo() const
    {
        return data_ctx_->endpoint_info();
    }

    inline json metaCtxInfo() const
    {
        auto endpoint_info               = meta_ctx_->endpoint_info();
        endpoint_info["remote_meta_key"] = uintptr_t(remote_meta_info_);
        return endpoint_info;
    }

    inline int32_t registerMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length)
    {
        return data_ctx_->registerMemoryRegion(mr_key, ptr, length);
    }

private:
    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;
    size_t                       data_ctx_qp_num_;
    size_t                       meta_ctx_qp_num_;

    uintptr_t remote_meta_key_;

    // DMA-able memory regions for metadata exchange
    meta_info_t* remote_meta_info_;
    meta_info_t* local_meta_info_;

    // --- jring_t* Lock-free Queues ---
    // Using raw pointers to avoid overhead, managed by jring
    jring_t* send_buffer_ring_;
    jring_t* recv_buffer_ring_;

    // Context Pools to avoid dynamic allocation
    std::vector<SendContext> send_ctx_pool_;
    std::vector<RecvContext> recv_ctx_pool_;

    // Scoreboards for signaling completion between RDMA callbacks and Proxy threads
    PaddedAtomicBool* meta_arrived_scoreboard_;
    PaddedAtomicBool* data_arrived_scoreboard_;

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

    // Helper to allocate aligned memory for jring
    jring_t* createRing(const char* name, size_t count);
    void     freeRing(jring_t* ring);

    // Dummy buffer for RDMA operations that require a payload but carry no data
    int64_t* dummy_;
};

}  // namespace slime
