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

static const size_t MAX_FIFO_DEPTH = 1024;
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

struct alignas(64) PaddedAtomicUint64 {
    std::atomic<uint64_t> val{0};
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
    int64_t qp_nums_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;
    size_t                       data_ctx_qp_num_;
    size_t                       meta_ctx_qp_num_;

    uintptr_t remote_meta_key_;

    // DMA-able memory regions for metadata exchange
    meta_info_t* remote_meta_info_;
    meta_info_t* local_meta_info_;

    // --- jring_t* Lock-free Queues ---
    jring_t* send_buffer_ring_;
    jring_t* recv_buffer_ring_;

    // Context Pools to avoid dynamic allocation
    std::vector<SendContext> send_ctx_pool_;
    std::vector<RecvContext> recv_ctx_pool_;

    // Scoreboards for signaling completion between RDMA callbacks and Proxy threads
    PaddedAtomicUint64* meta_arrived_scoreboard_;
    PaddedAtomicUint64* data_arrived_scoreboard_;

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
