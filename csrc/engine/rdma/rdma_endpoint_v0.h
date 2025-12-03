#pragma once

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_endpoint.h"
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

// V2 Optimization: Parameters
static const size_t MAX_FIFO_DEPTH = 512;
static const int    BURST_SIZE     = 16;

// V2 Optimization: Efficient CPU Pause
static inline void cpu_relax()
{
    _mm_pause();
}

// ==========================================
// V2 Data Structures: Context Pooling
// ==========================================

// Meta info aligned to Cache Line
typedef struct alignas(64) ViewInfo {
    uint32_t       r_key_;
    storage_view_t view_;

    ViewInfo(): r_key_(0), view_() {}  // Default constructor
    ViewInfo(uint32_t r_key, storage_view_t view): r_key_(r_key), view_(view) {}

    std::string dump()
    {
        return json{{"r_key_", r_key_},
                    {"view_", {{"ptr", view_.data_ptr}, {"length", view_.length}, {"offset", view_.storage_offset}}}}
            .dump();
    }
} meta_info_t;

// Context for Send Operations
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

// Context for Recv Operations
struct alignas(64) RecvContext {
    int32_t                            slot_id;
    std::shared_ptr<RDMABuffer>        buffer;
    std::shared_ptr<RDMAAssignHandler> assign_handler;  // Points to the internal recv completion

    void reset()
    {
        buffer         = nullptr;
        assign_handler = nullptr;
    }
};

// Context for Meta/Data Token Recv
struct alignas(64) TokenRecvContext {
    int32_t                            idx;
    std::shared_ptr<RDMAAssignHandler> assign;
};

// ==========================================
// Class Definition
// ==========================================

class RDMAEndpointV0: public std::enable_shared_from_this<RDMAEndpointV0> {
    friend class RDMABuffer;

public:
    explicit RDMAEndpointV0(const std::string& dev_name, size_t ib_port, const std::string& link_type, size_t qp_nums);
    ~RDMAEndpointV0();

    int32_t addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer);
    void    connect(const json& data_ctx_info, const json& meta_ctx_info);

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

    meta_info_t* remote_meta_info_;
    meta_info_t* local_meta_info_;

    // --- jring_t* queue ---
    jring_t* send_buffer_ring_;
    jring_t* recv_buffer_ring_;

    jring_t* meta_recv_assign_ring_;
    jring_t* data_recv_assign_ring_;

    jring_t* send_complete_ring_;
    jring_t* recv_complete_ring_;

    std::vector<SendContext>      send_ctx_pool_;
    std::vector<RecvContext>      recv_ctx_pool_;
    std::vector<TokenRecvContext> meta_recv_ctx_pool_;
    std::vector<TokenRecvContext> data_recv_ctx_pool_;

    int64_t* dummy_;

    std::atomic<uint64_t> send_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    std::atomic<bool> stop_send_proxy_signal_{false};
    std::atomic<bool> stop_recv_proxy_signal_{false};

    std::atomic<bool> stop_send_finish_signal_{false};
    std::atomic<bool> stop_recv_finish_signal_{false};

    std::thread send_proxy_thread_;
    std::thread recv_proxy_thread_;

    std::thread send_finish_thread_;
    std::thread recv_finish_thread_;

    void proxyInit();
    void proxyDestroy();

    int32_t sendProxy();
    int32_t recvProxy();

    int32_t sendFinish();
    int32_t recvFinish();

    // jring helper function
    jring_t* createRing(const char* name, size_t count);
    void     freeRing(jring_t* ring);
};

}  // namespace slime