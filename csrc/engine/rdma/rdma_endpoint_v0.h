#pragma once

#include "device/host/host_signal.h"
#include "device/signal.h"

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"

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
    friend class RDMAWorker;
    static constexpr size_t MAX_PENDING_SIZE = MAX_FIFO_DEPTH;

public:
    RDMAEndpointV0(std::shared_ptr<RDMAContext> data_ctx,
                   std::shared_ptr<RDMAContext> meta_ctx,
                   size_t                       qp_nums);

    ~RDMAEndpointV0();

    void alloc_qps();
    json local_info() const;

    std::shared_ptr<RDMAAssignHandler> submit(
        OpCode opcode, AssignmentBatch& batch, callback_fn_t callback, int32_t imm_data = -1, bool is_inline = false);

    /**
     * @brief Submit a buffer for Send or Recv operation.
     * This method is thread-safe and non-blocking (uses lock-free ring).
     */
    int32_t addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer, void* stream_handle = nullptr);

    /**
     * @brief Establish connection and start proxy threads.
     */
    void connect(const json& data_ctx_info, const json& meta_ctx_info);

    inline json dataCtxInfo() const
    {
        json local_info{};
        for (int i = 0; i < data_qp_man_.size(); ++i)
            local_info[i] = data_qp_man_[i].local_rdma_info_.to_json();
        return json{{"rdma_info", local_info}, {"mr_info", data_ctx_->memory_pool_->mr_info()}};
    }

    inline json metaCtxInfo() const
    {
        auto endpoint_info = json{{"rdma_info", meta_qp_man_.local_rdma_info_.to_json()},
                                  {"mr_info", data_ctx_->memory_pool_->mr_info()}};

        endpoint_info["remote_meta_key"] = uintptr_t(remote_meta_info_);
        return endpoint_info;
    }

    inline int32_t registerMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length)
    {
        return data_ctx_->registerMemoryRegion(mr_key, ptr, length);
    }

private:
    typedef struct qp_management {
        /* queue peer list */
        struct ibv_qp* qp_{nullptr};

        /* RDMA Exchange Information */
        rdma_info_t remote_rdma_info_;
        rdma_info_t local_rdma_info_;

        RDMAAssign* assign_pool_;
        /* polling pool */
        std::vector<ibv_send_wr> send_wr_pool_;
        std::vector<ibv_recv_wr> recv_wr_pool_;
        std::vector<ibv_sge>     send_sge_pool_;
        std::vector<ibv_sge>     recv_sge_pool_;

        ~qp_management()
        {
            if (qp_)
                ibv_destroy_qp(qp_);
            free(assign_pool_);
        }
    } qp_management_t;

    qp_management                meta_qp_man_;
    std::vector<qp_management_t> data_qp_man_;

    int32_t                    init();
    std::vector<qp_management> init_qp(std::shared_ptr<RDMAContext> ctx, size_t num_qp, size_t max_num_inline_data = 0);
    void                       connect_qp(std::shared_ptr<RDMAContext>               ctx,
                                          std::vector<RDMAEndpointV0::qp_management> qp_mans,
                                          const json&                                endpoint_info_json);

    bool bypass_signal_{false};

    int64_t qp_nums_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    /* Async RDMA SendRecv */
    int64_t post_send_batch(std::shared_ptr<RDMAContext> ctx, qp_management_t qp_man, RDMAAssign* assign);
    int64_t post_recv_batch(std::shared_ptr<RDMAContext> ctx, qp_management_t qp_man, RDMAAssign* assign);

    /* Async RDMA Read */
    int64_t post_rc_oneside_batch(std::shared_ptr<RDMAContext> ctx, qp_management_t qp_man, RDMAAssign* assign);

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

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

    SendContext* pending_send_buf_[MAX_PENDING_SIZE];
    uint32_t     send_head_  = 0;
    uint32_t     send_tail_  = 0;
    uint32_t     send_count_ = 0;

    RecvContext* pending_recv_buf_[MAX_PENDING_SIZE];
    uint32_t     recv_head_  = 0;
    uint32_t     recv_tail_  = 0;
    uint32_t     recv_count_ = 0;

    inline bool is_send_full() const
    {
        return send_count_ == MAX_PENDING_SIZE;
    }
    inline bool is_send_empty() const
    {
        return send_count_ == 0;
    }

    uintptr_t remote_meta_key_;

    // Scoreboards for signaling completion between RDMA callbacks and Proxy threads
    PaddedAtomicUint64* meta_arrived_scoreboard_;

    std::atomic<uint64_t> send_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    int32_t sendProcess();
    int32_t recvProcess();

    jring_t* createRing(const char* name, size_t count);
    void     freeRing(jring_t* ring);

    int64_t* dummy_;
};

}  // namespace slime
