#include "rdma_endpoint_v0.h"

#include "device/device_api.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_channel.h"
#include "engine/rdma/rdma_common.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_env.h"
#include "engine/rdma/rdma_utils.h"
#include "logging.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdlib.h>
#include <sys/types.h>
#include <thread>
#include <vector>

namespace slime {

jring_t* RDMAEndpointV0::createRing(const char* name, size_t count)
{
    size_t ring_sz = jring_get_buf_ring_size(sizeof(void*), count);

    void* mem = nullptr;
    // Align to 64 bytes to match cache line size, preventing false sharing.
    if (posix_memalign(&mem, 64, ring_sz) != 0) {
        throw std::runtime_error(std::string("Failed to allocate ring memory: ") + name);
    }

    jring_t* r = (jring_t*)mem;

    // Initialize ring: MP=1 (Multi-Producer safe), MC=1 (Multi-Consumer safe).
    // This allows multiple threads to enqueue requests if needed.
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

RDMAEndpointV0::RDMAEndpointV0(std::shared_ptr<RDMAContext> ctx, size_t num_qp): ctx_(ctx), num_qp_(num_qp)
{
    SLIME_LOG_INFO("Init RDMAEndpointV0 Contexts and Devices...");
    SLIME_LOG_INFO("bypass Signal: ", SLIME_BYPASS_DEVICE_SIGNAL);
    if (SLIME_BYPASS_DEVICE_SIGNAL)
        bypass_signal_ = true;

    num_qp_ = num_qp;

    // Aggregation logic is not supported in V0 Send/Recv mode.
    SLIME_ASSERT(1 == SLIME_AGG_QP_NUM, "cannot aggqp when sendrecv");
    SLIME_ASSERT(64 > SLIME_QP_NUM, "QP NUM must less than 64");

    // Allocate dummy buffer for Immediate Data payload or signaling.
    void* dummy_mem = nullptr;
    if (posix_memalign(&dummy_mem, 64, sizeof(int64_t)) != 0)
        throw std::runtime_error("dummy alloc fail");
    dummy_ = (int64_t*)dummy_mem;

    // Allocate context pools aligned to cache lines.
    void* raw_send_ctx = nullptr;
    if (posix_memalign(&raw_send_ctx, 64, sizeof(SendContext) * MAX_FIFO_DEPTH) != 0)
        throw std::runtime_error("remote meta alloc fail");
    send_ctx_pool_ = static_cast<SendContext*>(raw_send_ctx);

    void* raw_recv_ctx = nullptr;
    if (posix_memalign(&raw_recv_ctx, 64, sizeof(RecvContext) * MAX_FIFO_DEPTH) != 0)
        throw std::runtime_error("remote meta alloc fail");
    recv_ctx_pool_ = static_cast<RecvContext*>(raw_recv_ctx);

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        send_ctx_pool_[i].signal = slime::device::createSignal(bypass_signal_);
        recv_ctx_pool_[i].signal = slime::device::createSignal(bypass_signal_);
    }

    // Register Memory Regions (MR) upfront.
    // Dynamic registration during the datapath is expensive and should be avoided.
    ctx_->registerOrAccessMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        ctx_->registerOrAccessMemoryRegion(reinterpret_cast<uintptr_t>(&(send_ctx_pool_[i].remote_meta_info_)),
                                           reinterpret_cast<uintptr_t>(&(send_ctx_pool_[i].remote_meta_info_)),
                                           sizeof(meta_info_t));
        ctx_->registerOrAccessMemoryRegion(reinterpret_cast<uintptr_t>(&(recv_ctx_pool_[i].local_meta_info_)),
                                           reinterpret_cast<uintptr_t>(&(recv_ctx_pool_[i].local_meta_info_)),
                                           sizeof(meta_info_t));
    }

    SLIME_LOG_INFO("Memory Regions Registered.");

    data_channel_ = std::make_unique<RDMAChannel>();
    meta_channel_ = std::make_unique<RDMAChannel>();

    // Meta channel uses 1 QP (latency sensitive), Data channel uses num_qp_ (throughput sensitive).
    meta_channel_->init(ctx_, 1, 256);
    data_channel_->init(ctx_, num_qp_, 0);

    // Initialize Rings. Size is double the depth to handle potential overflow gracefully.
    size_t ring_size      = MAX_FIFO_DEPTH * 2;
    send_buffer_ring_ = createRing("send_buf", ring_size);
    recv_buffer_ring_ = createRing("recv_buf", ring_size);

    SLIME_LOG_INFO("RDMA Endpoint Initialization Completed.");
}

RDMAEndpointV0::~RDMAEndpointV0()
{
    try {
        // Ensure proxy threads are stopped before freeing memory to avoid use-after-free.
        proxyDestroy();

        free(dummy_);
        free(send_ctx_pool_);
        free(recv_ctx_pool_);
        freeRing(send_buffer_ring_);
        freeRing(recv_buffer_ring_);

        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully.");
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Exception in RDMAEndpoint destructor: ", e.what());
    }
}

void RDMAEndpointV0::proxyInit()
{
    send_proxy_thread_ = std::thread([this]() { this->sendProxy(); });
    recv_proxy_thread_ = std::thread([this]() { this->recvProxy(); });

    SLIME_LOG_INFO("RDMA Proxy Threads Started.");
}

void RDMAEndpointV0::proxyDestroy()
{
    SLIME_LOG_INFO("Stopping RDMA proxy threads...");

    stop_send_proxy_signal_.store(true, std::memory_order_release);
    stop_recv_proxy_signal_.store(true, std::memory_order_release);

    if (send_proxy_thread_.joinable())
        send_proxy_thread_.join();
    if (recv_proxy_thread_.joinable())
        recv_proxy_thread_.join();

    SLIME_LOG_INFO("RDMA Proxy Threads Stopped.");
}

json RDMAEndpointV0::endpointInfo() const
{
    json remote_meta_key = {};

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        remote_meta_key.push_back((uintptr_t)(&(send_ctx_pool_[i].remote_meta_info_)));
    }
    json endpoint_info = json{{"mr_info", ctx_->memory_pool_->mr_info()},
                              {"meta_channel_info", meta_channel_->channelInfo()},
                              {"data_channel_info", data_channel_->channelInfo()},
                              {"remote_meta_key", remote_meta_key}};
    return endpoint_info;
}

void RDMAEndpointV0::connect(const json& remote_endpoint_info)
{
    SLIME_LOG_INFO("Establishing RDMA Connection...");

    for (auto& item : remote_endpoint_info["mr_info"].items()) {
        ctx_->registerOrAccessRemoteMemoryRegion(item.value()["mr_key"].get<uintptr_t>(), item.value());
    }

    meta_channel_->connect(remote_endpoint_info["meta_channel_info"]);
    data_channel_->connect(remote_endpoint_info["data_channel_info"]);

    SLIME_LOG_INFO("Connection Established. Pre-posting RECV requests...");

    SLIME_ASSERT_EQ(remote_endpoint_info["remote_meta_key"].size(), MAX_FIFO_DEPTH, "FIFO Depth mismatch");

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        recv_ctx_pool_[i].remote_meta_key_ = remote_endpoint_info["remote_meta_key"][i];
    }

    // Pre-post RECV requests for Meta Channel to handle incoming handshake signals.
    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        SendContext* send_ctx = &(send_ctx_pool_[i]);
        std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
        send_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, batch, [send_ctx](int32_t status, int32_t imm) {
            send_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
        });
        meta_channel_->post_recv_batch(0, &(send_ctx->meta_recv_assign_));
    }

    // Pre-post RECV requests for Data Channel to handle completion signals (Imm Data).
    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        RecvContext* recv_ctx = &(recv_ctx_pool_[i]);
        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

            recv_ctx->data_recv_assign_.reset(OpCode::RECV, qpi, batch, [recv_ctx, qpi](int32_t status, int32_t imm) {
                if (status == 0) {
                    recv_ctx->signal->set_comm_done(qpi);
                }
                else {
                    SLIME_LOG_ERROR("Data Recv Failed during pre-post");
                }
            });
            data_channel_->post_recv_batch(qpi, &(recv_ctx->data_recv_assign_));
        }
    }

    proxyInit();

    SLIME_LOG_INFO("RDMA Contexts Launched.");
}

int32_t RDMAEndpointV0::send(uintptr_t data_ptr, size_t offset, size_t length, void* stream_handle)
{
    // Fast path: check MR cache.
    storage_view_t view{data_ptr, offset, length};
    auto           buffer_mr = ctx_->get_mr(data_ptr);
    if (not(buffer_mr and buffer_mr->length == length)) {
        SLIME_LOG_DEBUG("Registering new MR for buffer: ", data_ptr);
        ctx_->registerOrAccessMemoryRegion(data_ptr, data_ptr, length);
    }

    // Acquire a slot from the FIFO pool.
    uint32_t target_mask = (1 << num_qp_) - 1;
    uint64_t slot        = send_slot_id_.fetch_add(1, std::memory_order_release) % MAX_FIFO_DEPTH;

    SendContext* s_ctx = &(send_ctx_pool_[slot]);

    s_ctx->slot_id                    = slot;
    s_ctx->local_meta_info_.view_ = {data_ptr, offset, length};
    s_ctx->expected_mask          = target_mask;

    // Reset signal and bind to the compute stream for synchronization.
    s_ctx->signal->reset_all();
    s_ctx->signal->bind_stream(stream_handle);
    s_ctx->signal->record_gpu_ready();

    // Enqueue to the ring (lock-free producer).
    while (jring_enqueue_burst(send_buffer_ring_, (void**)&s_ctx, 1, nullptr) == 0) {
        cpu_relax();
    }

    return slot;
}

int32_t RDMAEndpointV0::recv(uintptr_t data_ptr, size_t offset, size_t length, void* stream_handle)
{
    auto buffer_mr = ctx_->get_mr(data_ptr);
    if (not(buffer_mr and buffer_mr->length == length)) {
        SLIME_LOG_DEBUG("Registering new MR for buffer: ", data_ptr);
        ctx_->registerOrAccessMemoryRegion(data_ptr, data_ptr, length);
    }

    uint32_t target_mask = (1 << num_qp_) - 1;
    uint64_t slot        = recv_slot_id_.fetch_add(1, std::memory_order_release) % MAX_FIFO_DEPTH;

    RecvContext* r_ctx = &(recv_ctx_pool_[slot]);

    r_ctx->slot_id       = slot;
    r_ctx->view_         = {data_ptr, offset, length};
    r_ctx->expected_mask = target_mask;

    r_ctx->signal->reset_all();
    r_ctx->signal->bind_stream(stream_handle);
    r_ctx->signal->record_gpu_ready();

    r_ctx->local_meta_info_.r_key_ = ctx_->get_mr(data_ptr)->rkey;
    r_ctx->local_meta_info_.view_  = {data_ptr, offset, length};

    while (jring_enqueue_burst(recv_buffer_ring_, (void**)&r_ctx, 1, nullptr) == 0) {
        cpu_relax();
    }

    return slot;
}

int32_t RDMAEndpointV0::waitSend(int32_t slot_id)
{
    // Blocking wait on CPU until the communication is marked done.
    send_ctx_pool_[slot_id].signal->wait_comm_done_cpu((1 << send_ctx_pool_[slot_id].expected_mask) - 1);
    return 0;
}

int32_t RDMAEndpointV0::waitRecv(int32_t slot_id)
{
    recv_ctx_pool_[slot_id].signal->wait_comm_done_cpu((1 << recv_ctx_pool_[slot_id].expected_mask) - 1);
    return 0;
}

int32_t RDMAEndpointV0::sendProxy()
{
    SLIME_LOG_INFO("SendProxy Reactor Thread Started.");

    // Bind to the NUMA node closest to the NIC to minimize PCIe/Memory latency.
    bindToSocket(socketId(ctx_->device_name_));

    void* new_burst_buf[BURST_SIZE];

    while (!stop_send_proxy_signal_.load(std::memory_order_relaxed)) {

        // ============================================================
        // Stage 1: Ingest - Dequeue from Ring
        // ============================================================
        int n = jring_dequeue_burst(send_buffer_ring_, new_burst_buf, BURST_SIZE, nullptr);
        for (int i = 0; i < n; ++i) {
            auto* s_ctx = (SendContext*)new_burst_buf[i];
            pending_send_queue_.push_back(s_ctx);
        }

        // ============================================================
        // Stage 2: State Machine Execution
        // ============================================================
        auto it = pending_send_queue_.begin();

        if (it != pending_send_queue_.end()) {
            SendContext* s_ctx          = *it;
            bool         task_completed = false;

            switch (s_ctx->state_) {
                case SendContextState::WAIT_GPU_READY:
                    // Check if GPU has finished producing data.
                    if (s_ctx->signal->is_gpu_ready()) {
                        s_ctx->state_ = SendContextState::WAIT_META;
                        goto CHECK_META_READY;
                    }
                    break;

                CHECK_META_READY:
                case SendContextState::WAIT_META:
                    // Check if the receiver has notified us (via RDMA Write or Recv completion).
                    if (s_ctx->meta_arrived_flag_.val.load(std::memory_order_acquire)) {
                        s_ctx->meta_arrived_flag_.val.store(false, std::memory_order_release);
                        s_ctx->state_ = SendContextState::POST_DATA_SEND;

                        // Ensure remote MR info is updated.
                        ctx_->registerOrAccessRemoteMemoryRegion(s_ctx->remote_meta_info_.view_.data_ptr,
                                                                 s_ctx->remote_meta_info_.view_.data_ptr,
                                                                 s_ctx->remote_meta_info_.view_.length,
                                                                 s_ctx->remote_meta_info_.r_key_);

                        // Chunk the data across multiple QPs for parallel transmission.
                        size_t total_len  = s_ctx->remote_meta_info_.view_.length;
                        size_t chunk_size = (total_len + num_qp_ - 1) / num_qp_;

                        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                            size_t offset = qpi * chunk_size;
                            if (offset >= total_len)
                                break;

                            size_t current_len = std::min(chunk_size, total_len - offset);

                            // Note: Offset applies to both source and destination (assuming 1:1 mapping).
                            Assignment      assign(s_ctx->local_meta_info_.view_.data_ptr,
                                              s_ctx->remote_meta_info_.view_.data_ptr,
                                              offset,
                                              offset,
                                              current_len);
                            AssignmentBatch batch{assign};

                            // Use WRITE_WITH_IMM to notify the receiver upon completion.
                            s_ctx->data_send_assign_.reset(
                                OpCode::WRITE_WITH_IMM,
                                qpi,
                                batch,
                                [s_ctx, qpi](int32_t stat, int32_t imm_data) { s_ctx->signal->set_comm_done(qpi); },
                                false);

                            data_channel_->post_rc_oneside_batch(qpi, &(s_ctx->data_send_assign_));
                        }

                        // Post a RECV WQE for the next handshake (Meta channel).
                        std::vector<Assignment> meta_batch{
                            Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

                        s_ctx->meta_recv_assign_.reset(
                            OpCode::RECV, 0, meta_batch, [this, s_ctx](int32_t status, int32_t imm) {
                                s_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
                            });
                        meta_channel_->post_recv_batch(0, &(s_ctx->meta_recv_assign_));

                        task_completed = true;
                    }
                    break;

                default:
                    break;
            }

            if (task_completed) {
                pending_send_queue_.pop_front();
            }
        }

        // Backoff to reduce CPU usage when idle.
        if (n == 0 && pending_send_queue_.empty()) {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::recvProxy()
{
    bindToSocket(socketId(ctx_->device_name_));
    SLIME_LOG_INFO("RecvProxy Reactor Thread Started.");

    void* new_burst_buf[BURST_SIZE];

    while (!stop_recv_proxy_signal_.load(std::memory_order_relaxed)) {

        int n = jring_dequeue_burst(recv_buffer_ring_, new_burst_buf, BURST_SIZE, nullptr);
        for (int i = 0; i < n; ++i) {
            auto* r_ctx = (RecvContext*)new_burst_buf[i];
            // Initial state: wait for local GPU buffer safety.
            r_ctx->state_ = RecvContextState::WAIT_GPU_BUF;
            pending_recv_queue_.push_back(r_ctx);
        }

        auto it = pending_recv_queue_.begin();
        if (it != pending_recv_queue_.end()) {
            RecvContext* r_ctx          = *it;
            bool         task_completed = false;

            switch (r_ctx->state_) {
                case RecvContextState::WAIT_GPU_BUF: {
                    // Ensure the buffer is not being used by kernels before RDMA overwrites it.
                    if (r_ctx->signal->is_gpu_ready()) {
                        r_ctx->state_ = RecvContextState::INIT_SEND_META;
                        goto SEND_META;
                    }
                    break;
                }

                SEND_META:
                case RecvContextState::INIT_SEND_META: {
                    // Critical Order: Post Recv WQEs BEFORE sending Meta info.
                    // This prevents RNR (Receiver Not Ready) errors if the sender responds immediately.
                    for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                        std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, 8)};

                        r_ctx->data_recv_assign_.reset(
                            OpCode::RECV, qpi, batch, [r_ctx, qpi](int32_t status, int32_t imm) {
                                if (status == 0) {
                                    r_ctx->signal->set_comm_done(qpi);
                                }
                                else {
                                    SLIME_LOG_ERROR("Data Recv Failed during completion");
                                }
                            });
                        data_channel_->post_recv_batch(qpi, &(r_ctx->data_recv_assign_));
                    }

                    // After WQEs are posted, safe to exchange Meta info.
                    int slot = r_ctx->slot_id;
                    Assignment      assign(reinterpret_cast<uintptr_t>(&(r_ctx->local_meta_info_)),
                                      r_ctx->remote_meta_key_,
                                      0,
                                      0,
                                      sizeof(meta_info_t));
                    AssignmentBatch assign_batch{assign};

                    r_ctx->meta_send_assign_.reset(OpCode::WRITE_WITH_IMM, 0, assign_batch, nullptr, true);
                    meta_channel_->post_rc_oneside_batch(0, &(r_ctx->meta_send_assign_));

                    // Reset state for next round (though r_ctx is effectively done for this op).
                    r_ctx->state_ = RecvContextState::WAIT_GPU_BUF;
                    task_completed = true;
                    break;
                }

                default:
                    break;
            }

            if (task_completed) {
                pending_recv_queue_.pop_front();
            }
        }

        if (n == 0 && pending_recv_queue_.empty()) {
            cpu_relax();
        }
    }
    return 0;
}

}  // namespace slime
