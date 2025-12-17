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
    if (posix_memalign(&mem, 64, ring_sz) != 0) {
        throw std::runtime_error(std::string("Failed to allocate ring memory: ") + name);
    }

    jring_t* r = (jring_t*)mem;

    // Initialize ring: MP=1 (Multi-Producer safe), MC=1 (Multi-Consumer safe)
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

    SLIME_ASSERT(1 == SLIME_AGG_QP_NUM, "cannot aggqp when sendrecv");
    SLIME_ASSERT(64 > SLIME_QP_NUM, "QP NUM must less than 64");

    size_t meta_buffer_size = sizeof(meta_info_t) * MAX_FIFO_DEPTH;

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

    send_ctx_pool_.resize(MAX_FIFO_DEPTH);
    recv_ctx_pool_.resize(MAX_FIFO_DEPTH);

    size_t assign_buffer_size = sizeof(RDMAAssign) * MAX_FIFO_DEPTH;

    void* data_send_assign_raw = nullptr;
    if (posix_memalign(&data_send_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    data_send_assign_ = static_cast<RDMAAssign*>(data_send_assign_raw);

    void* data_recv_assign_raw = nullptr;
    if (posix_memalign(&data_recv_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    data_recv_assign_ = static_cast<RDMAAssign*>(data_recv_assign_raw);

    void* meta_send_assign_raw = nullptr;
    if (posix_memalign(&meta_send_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    meta_send_assign_ = static_cast<RDMAAssign*>(meta_send_assign_raw);

    void* meta_recv_assign_raw = nullptr;
    if (posix_memalign(&meta_recv_assign_raw, 64, assign_buffer_size) != 0)
        throw std::runtime_error("send data assign alloc fail");
    meta_recv_assign_ = static_cast<RDMAAssign*>(meta_recv_assign_raw);

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        send_ctx_pool_[i].signal = slime::device::createSignal(bypass_signal_);
        recv_ctx_pool_[i].signal = slime::device::createSignal(bypass_signal_);
    }

    // Register Memory Regions (MR)
    // Registering these upfront prevents expensive registration calls during runtime.
    ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));
    ctx_->registerMemoryRegion(reinterpret_cast<uintptr_t>(remote_meta_info_),
                               reinterpret_cast<uintptr_t>(remote_meta_info_),
                               meta_buffer_size);
    ctx_->registerMemoryRegion(
        reinterpret_cast<uintptr_t>(local_meta_info_), reinterpret_cast<uintptr_t>(local_meta_info_), meta_buffer_size);

    SLIME_LOG_INFO("Memory Regions Registered.");

    data_channel_ = std::make_unique<RDMAChannel>();
    meta_channel_ = std::make_unique<RDMAChannel>();

    meta_channel_->init(ctx_, 1, 256);
    data_channel_->init(ctx_, num_qp_, 0);

    // Initialize Scoreboards
    // These atomic flags serve as signaling mechanism between RDMA callback thread and Proxy threads.
    SLIME_LOG_DEBUG("Initializing scoreboards...");
    meta_arrived_scoreboard_ = new PaddedAtomicUint64[MAX_FIFO_DEPTH];
    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        meta_arrived_scoreboard_[i].val.store(0);
    }

    // Initialize Rings
    // Size is double the depth to handle potential overflow gracefully.
    size_t ring_size  = MAX_FIFO_DEPTH * 2;
    send_buffer_ring_ = createRing("send_buf", ring_size);
    recv_buffer_ring_ = createRing("recv_buf", ring_size);

    SLIME_LOG_INFO("RDMA Endpoint Initialization Completed.");
}

RDMAEndpointV0::~RDMAEndpointV0()
{
    try {
        proxyDestroy();

        free(dummy_);
        free(local_meta_info_);
        free(remote_meta_info_);
        delete[] meta_arrived_scoreboard_;

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

void RDMAEndpointV0::connect(const json& remote_endpoint_info)
{
    SLIME_LOG_INFO("Establishing RDMA Connection...");

    for (auto& item : remote_endpoint_info["mr_info"].items()) {
        ctx_->registerRemoteMemoryRegion(item.value()["mr_key"].get<uintptr_t>(), item.value());
    }

    meta_channel_->connect(remote_endpoint_info["meta_channel_info"]);
    data_channel_->connect(remote_endpoint_info["data_channel_info"]);

    remote_meta_key_ = remote_endpoint_info["remote_meta_key"];

    SLIME_LOG_INFO("Connection Established. Pre-posting RECV requests...");

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};
        meta_recv_assign_[i].reset(OpCode::RECV, 0, batch, [this, i](int32_t status, int32_t imm) {
            meta_arrived_scoreboard_[i].val.store(1, std::memory_order_release);
        });
        meta_channel_->post_recv_batch(0, &(meta_recv_assign_[i]));
    }

    for (int i = 0; i < MAX_FIFO_DEPTH; ++i) {
        auto signal = recv_ctx_pool_[i].signal;
        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

            data_recv_assign_[i].reset(OpCode::RECV, qpi, batch, [signal, qpi](int32_t status, int32_t imm) {
                if (status == 0) {
                    signal->set_comm_done(qpi);
                }
                else {
                    SLIME_LOG_ERROR("Data Recv Failed during pre-post");
                }
            });
            data_channel_->post_recv_batch(qpi, &(data_recv_assign_[i]));
        }
    }

    proxyInit();

    SLIME_LOG_INFO("RDMA Contexts Launched.");
}

int32_t RDMAEndpointV0::addBuffer(OpCode opcode, std::shared_ptr<RDMABuffer> buffer, void* stream_handle)
{
    auto buffer_mr = ctx_->get_mr(buffer->ptr_);
    if (not(buffer_mr and buffer_mr->length == buffer->data_size_)) {
        SLIME_LOG_DEBUG("Registering new MR for buffer: ", buffer->ptr_);
        ctx_->registerMemoryRegion(buffer->ptr_, buffer->ptr_, buffer->data_size_);
    }

    uint32_t target_mask = (1 << num_qp_) - 1;
    buffer->num_pack_    = num_qp_;

    if (OpCode::SEND == opcode) {
        uint64_t slot    = send_slot_id_.fetch_add(1, std::memory_order_release) % MAX_FIFO_DEPTH;
        buffer->slot_id_ = slot;

        SendContext* ctx = &send_ctx_pool_[slot];

        ctx->slot_id       = slot;
        ctx->buffer        = buffer;
        ctx->expected_mask = target_mask;

        ctx->signal->reset_all();
        ctx->signal->bind_stream(stream_handle);
        ctx->signal->record_gpu_ready();
        buffer->signal_ = ctx->signal;

        while (jring_enqueue_burst(send_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            cpu_relax();
        }
    }
    else if (OpCode::RECV == opcode) {
        uint64_t slot    = recv_slot_id_.fetch_add(1, std::memory_order_release) % MAX_FIFO_DEPTH;
        buffer->slot_id_ = slot;

        RecvContext* ctx = &recv_ctx_pool_[slot];

        ctx->slot_id       = slot;
        ctx->buffer        = buffer;
        ctx->expected_mask = target_mask;

        ctx->signal->reset_all();
        ctx->signal->bind_stream(stream_handle);
        ctx->signal->record_gpu_ready();
        buffer->signal_ = ctx->signal;

        local_meta_info_[slot].r_key_ = ctx_->get_mr(buffer->ptr_)->rkey;
        local_meta_info_[slot].view_  = buffer->view_;

        while (jring_enqueue_burst(recv_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            cpu_relax();
        }
    }
    return 0;
}

int32_t RDMAEndpointV0::sendProxy()
{
    bindToSocket(socketId(ctx_->device_name_));

    void* buf_ptrs[BURST_SIZE];

    while (!stop_send_proxy_signal_.load(std::memory_order_relaxed)) {

        int n = jring_dequeue_burst(send_buffer_ring_, buf_ptrs, BURST_SIZE, nullptr);

        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                auto* s_ctx = (SendContext*)buf_ptrs[i];
                int   slot  = s_ctx->slot_id;

                while (!s_ctx->signal->is_gpu_ready()) {
                    cpu_relax();
                }

                while (!meta_arrived_scoreboard_[slot].val.load(std::memory_order_acquire)) {
                    cpu_relax();
                }

                meta_arrived_scoreboard_[slot].val.store(false, std::memory_order_relaxed);

                auto meta = remote_meta_info_[slot];

                ctx_->registerRemoteMemoryRegion(
                    meta.view_.data_ptr, meta.view_.data_ptr, meta.view_.length, meta.r_key_);

                size_t total_len  = s_ctx->buffer->data_size_;
                size_t chunk_size = (total_len + num_qp_ - 1) / num_qp_;

                for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                    size_t offset = qpi * chunk_size;

                    if (offset >= total_len)
                        break;
                    size_t current_len = std::min(chunk_size, total_len - offset);

                    Assignment assign(s_ctx->buffer->ptr_,
                                      meta.view_.data_ptr,
                                      offset,  // target offset
                                      offset,  // source offset
                                      current_len);

                    AssignmentBatch batch{assign};

                    data_send_assign_[slot].reset(
                        OpCode::WRITE_WITH_IMM,
                        qpi,
                        batch,
                        [s_ctx, qpi](int32_t stat, int32_t imm_data) { s_ctx->signal->set_comm_done(qpi); },
                        false);
                    data_channel_->post_rc_oneside_batch(qpi, &(data_send_assign_[slot]));
                }

                std::vector<Assignment> meta_batch{
                    Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

                meta_recv_assign_[s_ctx->slot_id].reset(
                    OpCode::RECV, 0, meta_batch, [this, s_ctx](int32_t status, int32_t imm) {
                        meta_arrived_scoreboard_[s_ctx->slot_id].val.store(1, std::memory_order_release);
                    });
                auto assign = meta_channel_->post_recv_batch(0, &(meta_recv_assign_[s_ctx->slot_id]));
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
    bindToSocket(socketId(ctx_->device_name_));
    SLIME_LOG_INFO("RecvProxy Thread running on NUMA node.");

    void* buf_ptrs[BURST_SIZE];

    while (!stop_recv_proxy_signal_.load(std::memory_order_relaxed)) {

        int n = jring_dequeue_burst(recv_buffer_ring_, buf_ptrs, BURST_SIZE, nullptr);

        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                RecvContext* r_ctx = static_cast<RecvContext*>(buf_ptrs[i]);
                int          slot  = r_ctx->slot_id;

                Assignment      assign(reinterpret_cast<uintptr_t>(local_meta_info_),
                                  remote_meta_key_,
                                  slot * sizeof(meta_info_t),
                                  slot * sizeof(meta_info_t),
                                  sizeof(meta_info_t));
                AssignmentBatch assign_batch{assign};
                meta_send_assign_[slot].reset(OpCode::WRITE_WITH_IMM, 0, assign_batch, nullptr, true);
                meta_channel_->post_rc_oneside_batch(0, &(meta_send_assign_[slot]));

                while (!r_ctx->signal->is_gpu_ready()) {
                    cpu_relax();
                }

                const uint32_t TARGET_MASK = r_ctx->expected_mask;

                for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                    std::vector<Assignment> batch{
                        Assignment(reinterpret_cast<uintptr_t>(dummy_), 0, 0, sizeof(int64_t))};

                    data_recv_assign_[slot].reset(OpCode::RECV, qpi, batch, [r_ctx, qpi](int32_t status, int32_t imm) {
                        if (status == 0) {
                            r_ctx->signal->set_comm_done(qpi);
                        }
                        else {
                            SLIME_LOG_ERROR("Data Recv Failed during pre-post");
                        }
                    });
                    data_channel_->post_recv_batch(qpi, &(data_recv_assign_[slot]));
                }
            }
        }
        else {
            cpu_relax();
        }
    }
    return 0;
}

}  // namespace slime