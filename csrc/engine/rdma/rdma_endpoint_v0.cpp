#include "rdma_endpoint_v0.h"

#include "engine/assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_context.h"
#include "../../utils.h"
#include "logging.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdlib.h>  // for posix_memalign
#include <sys/types.h>
#include <thread>
#include <vector>

namespace slime {

// ==========================================
// 辅助函数实现
// ==========================================

jring_t* RDMAEndpointV0::createRing(const char* name, size_t count)
{
    // jring 要求 element_size 必须是 4 的倍数，这里 sizeof(void*) 是 8
    // count 必须是 2 的幂次
    size_t ring_sz = jring_get_buf_ring_size(sizeof(void*), count);

    void* mem = nullptr;
    // 64字节对齐分配
    if (posix_memalign(&mem, 64, ring_sz) != 0) {
        throw std::runtime_error(std::string("Failed to allocate ring memory: ") + name);
    }

    jring_t* r = (jring_t*)mem;

    // 初始化: MP=1(多产), MC=1(多消) -> 线程安全模式
    if (jring_init(r, count, sizeof(void*), 1, 1) < 0) {
        free(mem);
        throw std::runtime_error(std::string("Failed to init ring: ") + name);
    }
    return r;
}

void RDMAEndpointV0::freeRing(jring_t* ring)
{
    if (ring) {
        free(ring);
    }
}

// ==========================================
// 构造与析构
// ==========================================

RDMAEndpointV0::RDMAEndpointV0(const std::string& dev_name,
                               size_t             ib_port,
                               const std::string& link_type,
                               size_t             qp_nums)
{
    SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
    data_ctx_ = std::make_shared<RDMAContext>(qp_nums, 0);
    meta_ctx_ = std::make_shared<RDMAContext>(1, 0);

    data_ctx_->init(dev_name, ib_port, link_type);
    meta_ctx_->init(dev_name, ib_port, link_type);

    data_ctx_qp_num_ = data_ctx_->qp_list_len_;
    meta_ctx_qp_num_ = meta_ctx_->qp_list_len_;

    SLIME_LOG_INFO("The QP number of data plane is: ", data_ctx_qp_num_);
    SLIME_LOG_INFO("The QP number of control plane is: ", meta_ctx_qp_num_);
    SLIME_LOG_INFO("RDMA Endpoint Init Success...");

    size_t meta_buffer_size = sizeof(meta_info_t) * MAX_FIFO_DEPTH;

    // --- 内存对齐分配 ---
    void* dummy_mem = nullptr;
    if (posix_memalign(&dummy_mem, 64, sizeof(int64_t)) != 0)
        throw std::runtime_error("dummy alloc fail");
    dummy_ = (int64_t*)dummy_mem;

    void* remote_raw_mem = nullptr;
    if (posix_memalign(&remote_raw_mem, 64, meta_buffer_size) != 0)
        throw std::runtime_error("remote meta alloc fail");
    remote_meta_info_ = static_cast<meta_info_t*>(remote_raw_mem);

    void* local_raw_mem = nullptr;
    if (posix_memalign(&local_raw_mem, 64, meta_buffer_size) != 0)
        throw std::runtime_error("local meta alloc fail");
    local_meta_info_ = static_cast<meta_info_t*>(local_raw_mem);

    // V2: Pre-allocate Context Pools
    send_ctx_pool_.resize(MAX_FIFO_DEPTH);
    recv_ctx_pool_.resize(MAX_FIFO_DEPTH);
    meta_recv_ctx_pool_.resize(MAX_FIFO_DEPTH);
    data_recv_ctx_pool_.resize(MAX_FIFO_DEPTH);

    SLIME_LOG_INFO("Memory Pre-allocated...");

    // 注册 MR
    meta_ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));
    meta_ctx_->registerMemoryRegion(reinterpret_cast<uintptr_t>(remote_meta_info_),
                                      reinterpret_cast<uintptr_t>(remote_meta_info_),
                                      meta_buffer_size);
    meta_ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(local_meta_info_), reinterpret_cast<uintptr_t>(local_meta_info_), meta_buffer_size);
    data_ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));

    SLIME_LOG_INFO("MR Registered...");
    
    // --- 初始化 jrings ---
    // 为了防止溢出，Ring 大小通常设为队列深度的 2-4 倍，且需为 2 的幂次
    // 512 * 2 = 1024
    size_t ring_size       = MAX_FIFO_DEPTH * 2;
    send_buffer_ring_      = createRing("send_buf", ring_size);
    recv_buffer_ring_      = createRing("recv_buf", ring_size);
    meta_recv_assign_ring_ = createRing("meta_recv", ring_size);
    data_recv_assign_ring_ = createRing("data_recv", ring_size);
    send_complete_ring_    = createRing("send_comp", ring_size);
    recv_complete_ring_    = createRing("recv_comp", ring_size);
}

RDMAEndpointV0::~RDMAEndpointV0()
{
    try {
        proxyDestroy();
        data_ctx_->stop_future();
        meta_ctx_->stop_future();
        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully");

        free(dummy_);
        free(local_meta_info_);
        free(remote_meta_info_);

        freeRing(send_buffer_ring_);
        freeRing(recv_buffer_ring_);
        freeRing(meta_recv_assign_ring_);
        freeRing(data_recv_assign_ring_);
        freeRing(send_complete_ring_);
        freeRing(recv_complete_ring_);
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Exception in RDMAEndpoint destructor: ", e.what());
    }
}

// ==========================================
// 线程管理
// ==========================================

void RDMAEndpointV0::proxyInit()
{
    send_proxy_thread_  = std::thread([this]() { this->sendProxy(); });
    recv_proxy_thread_  = std::thread([this]() { this->recvProxy(); });
    send_finish_thread_ = std::thread([this]() { this->sendFinish(); });
    recv_finish_thread_ = std::thread([this]() { this->recvFinish(); });

    SLIME_LOG_INFO("All Proxy Threads Started Successfully");
}

void RDMAEndpointV0::proxyDestroy()
{
    SLIME_LOG_INFO("Stopping all RDMA endpoint threads...");

    stop_send_proxy_signal_.store(true, std::memory_order_release);
    stop_recv_proxy_signal_.store(true, std::memory_order_release);
    stop_send_finish_signal_.store(true, std::memory_order_release);
    stop_recv_finish_signal_.store(true, std::memory_order_release);

    if (send_proxy_thread_.joinable())
        send_proxy_thread_.join();
    if (recv_proxy_thread_.joinable())
        recv_proxy_thread_.join();
    if (send_finish_thread_.joinable())
        send_finish_thread_.join();
    if (recv_finish_thread_.joinable())
        recv_finish_thread_.join();

    SLIME_LOG_INFO("All Proxy Threads Stopped Successfully");
}

void RDMAEndpointV0::connect(const json& data_ctx_info, const json& meta_ctx_info)
{
    SLIME_LOG_INFO("Lauch the RDMAConstex for DATA and META")
    data_ctx_->connect(data_ctx_info);
    meta_ctx_->connect(meta_ctx_info);

    remote_meta_key_ = meta_ctx_info["remote_meta_key"];

    // V2: Zero-Malloc Initialization
    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
        auto                    assign = meta_ctx_->submit(OpCode::RECV, batch);

        // Get from pool
        TokenRecvContext* ctx = &meta_recv_ctx_pool_[i];
        ctx->idx = i;
        ctx->assign = assign;

        if (jring_enqueue_burst(meta_recv_assign_ring_, (void**)&ctx, 1, nullptr) != 1) {
            SLIME_LOG_ERROR("Meta recv ring full during init");
        }
    }

    // Pre Recv Data
    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
        auto                    assign = data_ctx_->submit(OpCode::RECV, batch);

        // Get from pool
        TokenRecvContext* ctx = &data_recv_ctx_pool_[i];
        ctx->idx = i;
        ctx->assign = assign;

        if (jring_enqueue_burst(data_recv_assign_ring_, (void**)&ctx, 1, nullptr) != 1) {
            SLIME_LOG_ERROR("Data recv ring full during init");
        }
    }

    proxyInit();

    data_ctx_->launch_future();
    meta_ctx_->launch_future();
}

// ==========================================
// 核心逻辑 (V2: Batching, Pooling, Zero-Malloc)
// ==========================================

int32_t RDMAEndpointV0::addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer)
{
    // High-performance hot path: removed logs
    // MR Check / Registration can be optimized further, keeping functional logic here
    auto buffer_mr = data_ctx_->get_mr(buffer->ptr_ + buffer->offset_);
    if (not(buffer_mr and buffer_mr->length == buffer->data_size_)) {
        data_ctx_->registerMemoryRegion(buffer->ptr_, buffer->ptr_ + buffer->offset_, buffer->data_size_);
    }

    if (OpCode::SEND == opcode) {
        // 1. Atomically get slot
        uint64_t slot = send_slot_id_.fetch_add(1, std::memory_order_relaxed) % MAX_FIFO_DEPTH;
        buffer->slot_id_ = slot;

        // 2. Access pre-allocated context
        SendContext* ctx = &send_ctx_pool_[slot];
        ctx->slot_id = slot;
        ctx->buffer = buffer; 

        // 3. Enqueue ptr (Spin wait)
        while (jring_enqueue_burst(send_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            cpu_relax();
        }
    }
    else if (OpCode::RECV == opcode) {
        uint64_t slot = recv_slot_id_.fetch_add(1, std::memory_order_relaxed) % MAX_FIFO_DEPTH;
        buffer->slot_id_ = slot;

        RecvContext* ctx = &recv_ctx_pool_[slot];
        ctx->slot_id = slot;
        ctx->buffer = buffer;

        while (jring_enqueue_burst(recv_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::sendProxy()
{
    bindToSocket(socketId(data_ctx_->device_name_));
    
    // V2: Batch arrays
    void* buf_ptrs[BURST_SIZE];
    void* meta_ptrs[BURST_SIZE];

    while (!stop_send_proxy_signal_.load(std::memory_order_relaxed)) {

        // 1. Batch Dequeue Send Requests
        int n = jring_dequeue_burst(send_buffer_ring_, buf_ptrs, BURST_SIZE, nullptr);

        if (n > 0) {
            // 2. Batch Dequeue Meta Tokens (blocking until we get 'n')
            int m = 0;
            while (m < n) {
                if (stop_send_proxy_signal_.load(std::memory_order_relaxed)) return 0;
                int fetched = jring_dequeue_burst(meta_recv_assign_ring_, &meta_ptrs[m], n - m, nullptr);
                m += fetched;
                if (fetched == 0) cpu_relax();
            }

            // 3. Process Batch
            for (int i = 0; i < n; ++i) {
                SendContext* s_ctx = static_cast<SendContext*>(buf_ptrs[i]);
                TokenRecvContext* m_ctx = static_cast<TokenRecvContext*>(meta_ptrs[i]);

                // Wait for meta to arrive
                m_ctx->assign->wait();

                meta_info_t meta = remote_meta_info_[m_ctx->idx];
                
                // Replenish Meta Recv Queue (Reuse m_ctx)
                std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
                auto pre_recv_assign = meta_ctx_->submit(OpCode::RECV, batch);
                m_ctx->assign = pre_recv_assign;

                // Push back to DATA recv ring (Logic from V0 preserved)
                // This seems to be the pool for tokens used when we receive data? 
                // In V0 logic: "new_meta_item -> data_recv_assign_ring"
                while (jring_enqueue_burst(data_recv_assign_ring_, (void**)&m_ctx, 1, nullptr) == 0) cpu_relax();

                // Send Data
                // Note: register_remote_memory_region is usually fast if key is cached, 
                // but checking map every time is V1-style overhead. 
                // V0 assumes it handles it. Ideally, remote key caching should be optimized.
                data_ctx_->registerRemoteMemoryRegion(
                    meta.view_.data_ptr, meta.view_.data_ptr, meta.view_.length, meta.r_key_);

                auto assign = Assignment(uintptr_t(s_ctx->buffer->ptr_), meta.view_.data_ptr, 0, 0, s_ctx->buffer->data_size_);
                auto assign_batch   = AssignmentBatch{assign};
                
                // Store handler in SendContext
                s_ctx->assign_handler = data_ctx_->submit(OpCode::WRITE_WITH_IMM, assign_batch);

                // Enqueue to completion
                while (jring_enqueue_burst(send_complete_ring_, (void**)&s_ctx, 1, nullptr) == 0) cpu_relax();
            }
        }
        else {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::recvProxy()
{
    bindToSocket(socketId(data_ctx_->device_name_));

    void* buf_ptrs[BURST_SIZE];
    void* token_ptrs[BURST_SIZE];

    while (!stop_recv_proxy_signal_.load(std::memory_order_relaxed)) {

        int n = jring_dequeue_burst(recv_buffer_ring_, buf_ptrs, BURST_SIZE, nullptr);

        if (n > 0) {
            // Need matching data tokens
            int m = 0;
            while (m < n) {
                if (stop_recv_proxy_signal_.load(std::memory_order_relaxed)) return 0;
                int fetched = jring_dequeue_burst(data_recv_assign_ring_, &token_ptrs[m], n - m, nullptr);
                m += fetched;
                if (fetched == 0) cpu_relax();
            }

            for (int i = 0; i < n; ++i) {
                RecvContext* r_ctx = static_cast<RecvContext*>(buf_ptrs[i]);
                TokenRecvContext* t_ctx = static_cast<TokenRecvContext*>(token_ptrs[i]);

                auto local_mr = data_ctx_->get_mr(r_ctx->buffer->ptr_);

                // Update Local Meta
                local_meta_info_[r_ctx->slot_id] = meta_info_t(local_mr->rkey, r_ctx->buffer->view_);

                // Send Meta to Remote (Write with IMM)
                auto assign_batch = AssignmentBatch{Assignment(reinterpret_cast<uintptr_t>(local_meta_info_),
                                                               remote_meta_key_,
                                                               r_ctx->slot_id * sizeof(meta_info_t),
                                                               r_ctx->slot_id * sizeof(meta_info_t),
                                                               sizeof(meta_info_t))};

                meta_ctx_->submit(OpCode::WRITE_WITH_IMM, assign_batch);

                // Wait for the DATA completion.
                // In V0, the 'complete_item' held 'data_recv_assign_item->assign_'.
                // So we point the RecvContext handler to the Token's handler.
                r_ctx->assign_handler = t_ctx->assign;

                while (jring_enqueue_burst(recv_complete_ring_, (void**)&r_ctx, 1, nullptr) == 0) cpu_relax();

                // Replenish Data Token
                std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
                auto pre_recv_assign = data_ctx_->submit(OpCode::RECV, batch);
                t_ctx->assign = pre_recv_assign;

                // Recycle Token (Logic from V0: new_data_item -> data_recv_assign_ring)
                while (jring_enqueue_burst(data_recv_assign_ring_, (void**)&t_ctx, 1, nullptr) == 0) cpu_relax();
            }
        }
        else {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::sendFinish()
{
    bindToSocket(socketId(data_ctx_->device_name_));
    void* comp_ptrs[BURST_SIZE];

    while (!stop_send_finish_signal_.load(std::memory_order_relaxed)) {

        int n = jring_dequeue_burst(send_complete_ring_, comp_ptrs, BURST_SIZE, nullptr);
        
        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                SendContext* s_ctx = static_cast<SendContext*>(comp_ptrs[i]);

                // Wait for RDMA Write completion
                s_ctx->assign_handler->wait();
                
                // Callback
                s_ctx->buffer->sendDoneCallback();
                
                // Reset Context (Release shared_ptr ref count)
                s_ctx->reset();
            }
        }
        else {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::recvFinish()
{
    bindToSocket(socketId(data_ctx_->device_name_));
    void* comp_ptrs[BURST_SIZE];

    while (!stop_recv_finish_signal_.load(std::memory_order_relaxed)) {

        int n = jring_dequeue_burst(recv_complete_ring_, comp_ptrs, BURST_SIZE, nullptr);

        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                RecvContext* r_ctx = static_cast<RecvContext*>(comp_ptrs[i]);

                // Wait for RDMA Recv completion (Data arrived)
                r_ctx->assign_handler->wait();
                
                // Callback
                r_ctx->buffer->recvDoneCallback();
                
                // Reset
                r_ctx->reset();
            }
        }
        else {
            cpu_relax();
        }
    }
    return 0;
}

}  // namespace slime