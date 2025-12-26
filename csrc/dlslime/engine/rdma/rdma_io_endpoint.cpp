#include "rdma_io_endpoint.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <emmintrin.h>
#include <memory>
#include <new>
#include <stdexcept>

#include "dlslime/engine/assignment.h"

#include "rdma_assignment.h"
#include "rdma_env.h"
#include "rdma_future.h"

#include "dlslime/logging.h"
#include "dlslime/utils.h"

namespace dlslime {

void RDMAIOEndpoint::dummyReset(ImmRecvContext* ctx)
{
    for (int qpi = 0; qpi < num_qp_; ++qpi) {
        ctx->assigns_[qpi].opcode_ = OpCode::RECV;

        ctx->assigns_[qpi].batch_size_ = 1;

        ctx->assigns_[qpi].batch_[0].length        = sizeof(*dummy_);
        ctx->assigns_[qpi].batch_[0].mr_key        = (uintptr_t)dummy_;
        ctx->assigns_[qpi].batch_[0].remote_mr_key = (uintptr_t)dummy_;
        ctx->assigns_[qpi].batch_[0].target_offset = 0;
        ctx->assigns_[qpi].batch_[0].source_offset = 0;

        ctx->assigns_[qpi].callback_ = [ctx, qpi](int32_t status, int32_t imm) {
            ctx->signal->set_comm_done(qpi);
            ctx->assigns_[qpi].imm_data_ = imm;
        };

        ctx->assigns_[qpi].is_inline_ = false;

        ctx->assigns_[qpi].imm_data_ = 0;
    }
}

// ============================================================
// Helper function (Striping)
// ============================================================

int32_t
RDMAIOEndpoint::dispatchTask(OpCode op_code, const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    size_t req_idx    = 0;
    size_t req_offset = 0;
    size_t total_reqs = assign.size();

    int32_t last_slot = -1;

    while (req_idx < total_reqs) {

        uint64_t          slot = rw_slot_id_.fetch_add(1, std::memory_order_relaxed) % SLIME_MAX_IO_FIFO_DEPTH;
        ReadWriteContext* ctx  = &read_write_ctx_pool_[slot];
        ctx->slot_id           = slot;
        ctx->expected_mask     = (1 << num_qp_) - 1;
        ctx->finished_qp_mask.store(0);

        last_slot = (int32_t)slot;

        OpCode slot_op_code = op_code;

        size_t current_batch_size = 0;

        while (req_idx < total_reqs && current_batch_size < SLIME_MAX_SEND_WR) {

            size_t total_len  = std::get<4>(assign[req_idx]);
            size_t remain_len = total_len - req_offset;
            size_t chunk_len  = std::min(remain_len, SLIME_MAX_LENGTH_PER_ASSIGNMENT);

            bool is_request_tail = (req_offset + chunk_len >= total_len);
            bool is_overall_tail = (req_idx == total_reqs - 1) && is_request_tail;

            if (op_code == OpCode::WRITE_WITH_IMM && !is_overall_tail) {
                slot_op_code = OpCode::WRITE;
            }

            uintptr_t l_base     = std::get<0>(assign[req_idx]);
            uintptr_t r_base     = std::get<1>(assign[req_idx]);
            uintptr_t t_off_base = std::get<2>(assign[req_idx]) + req_offset;
            uintptr_t s_off_base = std::get<3>(assign[req_idx]) + req_offset;

            size_t qp_chunk_size = (chunk_len + num_qp_ - 1) / num_qp_;

            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                size_t qp_rel_off  = qpi * qp_chunk_size;
                auto&  assign_elem = ctx->assigns_[qpi].batch_[current_batch_size];

                if (qp_rel_off >= chunk_len) {
                    assign_elem.length = 0;
                }
                else {
                    assign_elem.length = std::min(qp_chunk_size, chunk_len - qp_rel_off);
                }

                assign_elem.mr_key        = l_base;
                assign_elem.remote_mr_key = r_base;
                assign_elem.target_offset = t_off_base + qp_rel_off;
                assign_elem.source_offset = s_off_base + qp_rel_off;
            }

            current_batch_size++;
            req_offset += chunk_len;

            if (req_offset >= total_len) {
                req_idx++;
                req_offset = 0;
            }
        }
        bool is_final_slot = (req_idx >= total_reqs);

        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            auto& assign       = ctx->assigns_[qpi];
            assign.batch_size_ = current_batch_size;
            assign.opcode_     = slot_op_code;
            assign.is_inline_  = false;

            assign.callback_ = [this, ctx, qpi, is_final_slot](int32_t status, int32_t imm) {
                uint32_t old_mask = ctx->finished_qp_mask.fetch_or(1 << qpi, std::memory_order_acq_rel);
                uint32_t new_mask = old_mask | (1 << qpi);

                token_bucket_[qpi].fetch_add(ctx->assigns_[qpi].batch_size_);

                if (is_final_slot) {
                    ctx->signal->set_comm_done(qpi);
                }
            };

            if (slot_op_code == OpCode::WRITE_WITH_IMM) {
                assign.imm_data_ = imm_data;
            }
        }

        ctx->signal->reset_all();
        ctx->signal->bind_stream(stream);
        ctx->signal->record_gpu_ready();

        while (jring_enqueue_burst(read_write_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
            _mm_pause();
        }
    }

    return last_slot;
}

RDMAIOEndpoint::RDMAIOEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp): ctx_(ctx), num_qp_(num_qp)
{
    SLIME_LOG_INFO("Initializing RDMA IO Endpoint. QPs: ", num_qp, ", Token Bucket: ", SLIME_MAX_SEND_WR);

    data_channel_ = std::make_shared<RDMAChannel>();
    data_channel_->init(ctx_, num_qp_, 0);

    for (int i = 0; i < num_qp_; ++i) {
        token_bucket_[i].store(SLIME_MAX_SEND_WR);
    }

    size_t ring_size        = SLIME_MAX_IO_FIFO_DEPTH;
    read_write_buffer_ring_ = createRing("io_rw_ring", ring_size);
    imm_recv_buffer_ring_   = createRing("io_recv_ring", ring_size);

    void* raw_rw_ctx = nullptr;
    if (posix_memalign(&raw_rw_ctx, 64, sizeof(ReadWriteContext) * SLIME_MAX_IO_FIFO_DEPTH) != 0) {
        throw std::runtime_error("Failed to allocate memory for ReadWriteContext pool");
    }
    read_write_ctx_pool_ = static_cast<ReadWriteContext*>(raw_rw_ctx);

    void* raw_recv_ctx = nullptr;
    if (posix_memalign(&raw_recv_ctx, 64, sizeof(ImmRecvContext) * SLIME_MAX_IO_FIFO_DEPTH) != 0) {
        free(raw_rw_ctx);
        throw std::runtime_error("Failed to allocate memory for ImmRecvContext pool");
    }
    imm_recv_ctx_pool_ = static_cast<ImmRecvContext*>(raw_recv_ctx);

    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        new (&read_write_ctx_pool_[i]) ReadWriteContext();
        read_write_ctx_pool_[i].signal = dlslime::device::createSignal(false);
        read_write_ctx_pool_[i].assigns_.resize(num_qp_);
        read_write_ctx_pool_[i].finished_qp_mask.store(0);

        new (&imm_recv_ctx_pool_[i]) ImmRecvContext();
        imm_recv_ctx_pool_[i].signal = dlslime::device::createSignal(false);
        imm_recv_ctx_pool_[i].assigns_.resize(num_qp_);
    }

    for (size_t i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        read_write_future_pool_.push_back(std::make_shared<ReadWriteFuture>(&(read_write_ctx_pool_[i])));
        imm_recv_future_pool_.push_back(std::make_shared<ImmRecvFuture>(&(imm_recv_ctx_pool_[i])));
    }

    void* dummy_mem = nullptr;
    if (posix_memalign(&dummy_mem, 64, sizeof(int64_t)) != 0) {
        throw std::runtime_error("Failed to allocate dummy memory");
    }
    dummy_ = (int64_t*)dummy_mem;

    ctx_->registerOrAccessMemoryRegion(
        reinterpret_cast<uintptr_t>(dummy_), reinterpret_cast<uintptr_t>(dummy_), sizeof(int64_t));
    SLIME_LOG_INFO("IOEndpoint initialized");
}

RDMAIOEndpoint::~RDMAIOEndpoint()
{
    freeRing(read_write_buffer_ring_);
    freeRing(imm_recv_buffer_ring_);

    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        read_write_ctx_pool_[i].~ReadWriteContext();
        imm_recv_ctx_pool_[i].~ImmRecvContext();
    }
    free(read_write_ctx_pool_);
    free(imm_recv_ctx_pool_);
    free(dummy_);
}

// ============================================================
// Connection
// ============================================================

void RDMAIOEndpoint::connect(const json& remote_endpoint_info)
{
    for (auto& item : remote_endpoint_info["mr_info"].items()) {
        ctx_->registerOrAccessRemoteMemoryRegion(item.value()["mr_key"], item.value());
    }
    data_channel_->connect(remote_endpoint_info["data_channel_info"]);
}

json RDMAIOEndpoint::endpointInfo() const
{
    return json{{"data_channel_info", data_channel_->channelInfo()}};
}

// ============================================================
// Producer (Enqueue to Ring)
// ============================================================

std::shared_ptr<ReadWriteFuture> RDMAIOEndpoint::read(const std::vector<assign_tuple_t>& assign, void* stream)
{
    int32_t slot_id = dispatchTask(OpCode::READ, assign, 0, stream);
    return read_write_future_pool_[slot_id];
}

std::shared_ptr<ReadWriteFuture> RDMAIOEndpoint::write(const std::vector<assign_tuple_t>& assign, void* stream)
{
    int32_t slot_id = dispatchTask(OpCode::WRITE, assign, 0, stream);
    return read_write_future_pool_[slot_id];
}

std::shared_ptr<ReadWriteFuture>
RDMAIOEndpoint::writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    int32_t slot_id = dispatchTask(OpCode::WRITE_WITH_IMM, assign, imm_data, stream);
    return read_write_future_pool_[slot_id];
}

std::shared_ptr<ImmRecvFuture> RDMAIOEndpoint::immRecv(void* stream)
{
    uint64_t        slot = recv_slot_id_.fetch_add(1, std::memory_order_relaxed) % SLIME_MAX_IO_FIFO_DEPTH;
    ImmRecvContext* ctx  = &imm_recv_ctx_pool_[slot];

    ctx->slot_id       = slot;
    ctx->expected_mask = (1 << num_qp_) - 1;
    ctx->signal->reset_all();
    ctx->signal->bind_stream(stream);

    dummyReset(ctx);

    while (jring_enqueue_burst(imm_recv_buffer_ring_, (void**)&ctx, 1, nullptr) == 0)
        _mm_pause();
    return imm_recv_future_pool_[slot];
}

// ============================================================
// Consumer / Progress Engine (CORE LOGIC)
// ============================================================

int32_t RDMAIOEndpoint::readWriteProcess()
{
    int32_t work_done = 0;

    int n_rw = jring_dequeue_burst(read_write_buffer_ring_, burst_buf_, IO_BURST_SIZE, nullptr);
    if (n_rw > 0) {
        for (int i = 0; i < n_rw; ++i) {
            auto* ctx = (ReadWriteContext*)burst_buf_[i];
            pending_rw_queue_.push_back(ctx);
        }
    }

    while (!pending_rw_queue_.empty()) {
        auto* ctx = pending_rw_queue_.front();

        if (!ctx->signal->is_gpu_ready()) {
            break;
        }

        bool has_token = true;
        for (int qpi = 0; qpi < num_qp_; ++qpi) {
            if (token_bucket_[qpi].load(std::memory_order_acquire) < ctx->assigns_[qpi].batch_size_) {
                has_token = false;
                break;
            }
        }
        if (not has_token)
            break;

        pending_rw_queue_.pop_front();

        work_done++;

        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            token_bucket_[qpi].fetch_sub(ctx->assigns_[qpi].batch_size_, std::memory_order_release);
            data_channel_->post_rc_oneside_batch(qpi, &(ctx->assigns_[qpi]));
        }

        ctx->state_ = IOContextState::POSTED;
    }
    return work_done;
}

int32_t RDMAIOEndpoint::immRecvProcess()
{
    int32_t work_done = 0;
    int     n_recv    = jring_dequeue_burst(imm_recv_buffer_ring_, burst_buf_, IO_BURST_SIZE, nullptr);

    if (n_recv > 0) {
        work_done += n_recv;
        for (int i = 0; i < n_recv; ++i) {
            auto* ctx = (ImmRecvContext*)burst_buf_[i];
            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                data_channel_->post_recv_batch(qpi, &(ctx->assigns_[qpi]));
            }
            ctx->state_ = IOContextState::POSTED;
        }
    }
    return work_done;
}

int32_t RDMAIOEndpoint::process()
{
    int32_t work_done = 0;
    work_done += readWriteProcess();
    work_done += immRecvProcess();
    return work_done;
}

}  // namespace dlslime
