#include "engine/rdma/rdma_io_endpoint.h"

#include <emmintrin.h> // for _mm_pause
#include <stdexcept>
#include <algorithm>

#include "logging.h"
#include "engine/assignment.h" // 确保包含 Assignment 定义

namespace slime {

jring_t* RDMAIOEndpoint::createRing(const char* name, size_t count) {
    size_t ring_sz = jring_get_buf_ring_size(sizeof(void*), count);
    void* mem = nullptr;
    if (posix_memalign(&mem, 64, ring_sz) != 0) {
        throw std::runtime_error(std::string("Failed to allocate ring memory: ") + name);
    }
    jring_t* r = (jring_t*)mem;
    if (jring_init(r, count, sizeof(void*), 1, 1) < 0) {
        free(mem);
        throw std::runtime_error(std::string("Failed to init ring: ") + name);
    }
    return r;
}

void RDMAIOEndpoint::freeRing(jring_t* ring) {
    if (ring) free(ring);
}

RDMAIOEndpoint::RDMAIOEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp)
    : ctx_(ctx), num_qp_(num_qp) 
{
    SLIME_LOG_INFO("Initializing RDMA IO Endpoint with ", num_qp, " QPs (Striping Enabled)");

    data_channel_ = std::make_shared<RDMAChannel>();
    data_channel_->init(ctx_, num_qp_, 0);

    size_t ring_size = MAX_IO_FIFO_DEPTH * 2;
    read_write_buffer_ring_ = createRing("io_rw_ring", ring_size);
    imm_recv_buffer_ring_   = createRing("io_recv_ring", ring_size);

    void* raw_rw_ctx = nullptr;
    posix_memalign(&raw_rw_ctx, 64, sizeof(ReadWriteContext) * MAX_IO_FIFO_DEPTH);
    read_write_ctx_pool_ = static_cast<ReadWriteContext*>(raw_rw_ctx);

    void* raw_recv_ctx = nullptr;
    posix_memalign(&raw_recv_ctx, 64, sizeof(ImmRecvContext) * MAX_IO_FIFO_DEPTH);
    imm_recv_ctx_pool_ = static_cast<ImmRecvContext*>(raw_recv_ctx);

    // Initialize Context Pools
    for (int i = 0; i < MAX_IO_FIFO_DEPTH; ++i) {
        // Construct shared_ptr signals. Assuming createSignal returns a shared_ptr.
        read_write_ctx_pool_[i].signal = slime::device::createSignal(false);
        // Resize vector to hold pre-allocated RDMAAssign objects for each QP
        read_write_ctx_pool_[i].assigns_.resize(num_qp_);
        
        imm_recv_ctx_pool_[i].signal = slime::device::createSignal(false);
        imm_recv_ctx_pool_[i].assigns_.resize(num_qp_);
    }

    void* dummy_mem = nullptr;
    posix_memalign(&dummy_mem, 64, sizeof(int64_t));
    dummy_ = (int64_t*)dummy_mem;
    
    ctx_->registerOrAccessMemoryRegion(reinterpret_cast<uintptr_t>(dummy_), 
                                       reinterpret_cast<uintptr_t>(dummy_), 
                                       sizeof(int64_t));
}

RDMAIOEndpoint::~RDMAIOEndpoint() {
    freeRing(read_write_buffer_ring_);
    freeRing(imm_recv_buffer_ring_);
    
    // Note: shared_ptr signals will auto-release when context pools are freed, 
    // but since the pools are raw pointers (posix_memalign), we need to manually
    // destruct the objects before freeing the memory block to avoid leaks.
    for (int i = 0; i < MAX_IO_FIFO_DEPTH; ++i) {
        read_write_ctx_pool_[i].~ReadWriteContext();
        imm_recv_ctx_pool_[i].~ImmRecvContext();
    }

    free(read_write_ctx_pool_);
    free(imm_recv_ctx_pool_);
    free(dummy_);
}

void RDMAIOEndpoint::connect(const json& remote_endpoint_info) {
    SLIME_LOG_INFO("Establishing One-Sided RDMA Connection...");
    for (auto& item : remote_endpoint_info["mr_info"].items()) {
        ctx_->registerOrAccessRemoteMemoryRegion(item.value()["mr_key"].get<uintptr_t>(), item.value());
    }
    data_channel_->connect(remote_endpoint_info["data_channel_info"]);
    SLIME_LOG_INFO("One-Sided Connection Established.");
}

json RDMAIOEndpoint::endpointInfo() const {
    return json{
        {"mr_info", ctx_->memory_pool_->mr_info()}, 
        {"data_channel_info", data_channel_->channelInfo()}
    };
}

int32_t RDMAIOEndpoint::registerMemoryRegion(uintptr_t ptr, size_t length) {
    return ctx_->registerOrAccessMemoryRegion(ptr, ptr, length);
}

// ============================================================
// Producer Logic
// ============================================================

int32_t RDMAIOEndpoint::read(uintptr_t local_ptr, uintptr_t remote_ptr, size_t len, uint32_t rkey, void* stream) {
    uint64_t slot = rw_slot_id_.fetch_add(1, std::memory_order_relaxed) % MAX_IO_FIFO_DEPTH;
    ReadWriteContext* ctx = &read_write_ctx_pool_[slot];

    ctx->slot_id    = slot;
    ctx->local_ptr  = local_ptr;
    ctx->remote_ptr = remote_ptr;
    ctx->length     = len;
    ctx->rkey       = rkey;
    ctx->op_code    = OpCode::READ;
    ctx->expected_mask = (1 << num_qp_) - 1;

    ctx->signal->reset_all();
    ctx->signal->bind_stream(stream);
    ctx->signal->record_gpu_ready();

    while (jring_enqueue_burst(read_write_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
        _mm_pause();
    }
    return slot;
}

int32_t RDMAIOEndpoint::write(uintptr_t local_ptr, uintptr_t remote_ptr, size_t len, uint32_t rkey, void* stream) {
    uint64_t slot = rw_slot_id_.fetch_add(1, std::memory_order_relaxed) % MAX_IO_FIFO_DEPTH;
    ReadWriteContext* ctx = &read_write_ctx_pool_[slot];
    
    ctx->slot_id    = slot;
    ctx->local_ptr  = local_ptr;
    ctx->remote_ptr = remote_ptr;
    ctx->length     = len;
    ctx->rkey       = rkey;
    ctx->op_code    = OpCode::WRITE;
    ctx->expected_mask = (1 << num_qp_) - 1;

    ctx->signal->reset_all();
    ctx->signal->bind_stream(stream);
    ctx->signal->record_gpu_ready();

    while (jring_enqueue_burst(read_write_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
        _mm_pause();
    }
    return slot;
}

int32_t RDMAIOEndpoint::writeWithImm(uintptr_t local_ptr, uintptr_t remote_ptr, size_t len, uint32_t rkey, int32_t imm_data, void* stream) {
    uint64_t slot = rw_slot_id_.fetch_add(1, std::memory_order_relaxed) % MAX_IO_FIFO_DEPTH;
    ReadWriteContext* ctx = &read_write_ctx_pool_[slot];

    ctx->slot_id    = slot;
    ctx->local_ptr  = local_ptr;
    ctx->remote_ptr = remote_ptr;
    ctx->length     = len;
    ctx->rkey       = rkey;
    ctx->imm_data   = imm_data;
    ctx->op_code    = OpCode::WRITE_WITH_IMM;
    ctx->expected_mask = (1 << num_qp_) - 1;

    ctx->signal->reset_all();
    ctx->signal->bind_stream(stream);
    ctx->signal->record_gpu_ready();

    while (jring_enqueue_burst(read_write_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
        _mm_pause();
    }
    return slot;
}

int32_t RDMAIOEndpoint::recvImm(void* stream) {
    uint64_t slot = recv_slot_id_.fetch_add(1, std::memory_order_relaxed) % MAX_IO_FIFO_DEPTH;
    ImmRecvContext* ctx = &imm_recv_ctx_pool_[slot];

    ctx->slot_id = slot;
    ctx->expected_mask = (1 << num_qp_) - 1; 

    ctx->signal->reset_all();
    ctx->signal->bind_stream(stream);
    
    while (jring_enqueue_burst(imm_recv_buffer_ring_, (void**)&ctx, 1, nullptr) == 0) {
        _mm_pause();
    }
    return slot;
}

// ============================================================
// Consumer Logic
// ============================================================

int32_t RDMAIOEndpoint::process() {
    int32_t work_done = 0;

    // --- Process Read/Write Requests ---
    int n_rw = jring_dequeue_burst(read_write_buffer_ring_, burst_buf_, IO_BURST_SIZE, nullptr);
    if (n_rw > 0) {
        work_done += n_rw;
        for (int i = 0; i < n_rw; ++i) {
            auto* ctx = (ReadWriteContext*)burst_buf_[i];
            
            while (!ctx->signal->is_gpu_ready()) { _mm_pause(); }

            size_t total_len  = ctx->length;
            size_t chunk_size = (total_len + num_qp_ - 1) / num_qp_;

            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                size_t offset = qpi * chunk_size;
                if (offset >= total_len) break;

                size_t current_len = std::min(chunk_size, total_len - offset);

                Assignment assign(ctx->local_ptr, ctx->remote_ptr, offset, offset, current_len);
                AssignmentBatch batch{assign};

                // Use RDMAAssign type here
                RDMAAssign* assign_ptr = &(ctx->assigns_[qpi]);

                auto cb = [ctx, qpi](int32_t status, int32_t imm) { 
                    ctx->signal->set_comm_done(qpi); 
                };
                
                if (ctx->op_code == OpCode::WRITE_WITH_IMM) {
                    assign_ptr->reset(ctx->op_code, qpi, batch, cb, false, ctx->imm_data);
                } else {
                    assign_ptr->reset(ctx->op_code, qpi, batch, cb, false);
                }
                
                data_channel_->post_rc_oneside_batch(qpi, assign_ptr);
            }
            ctx->state_ = IOContextState::POSTED;
        }
    }

    // --- Process Recv Requests ---
    int n_recv = jring_dequeue_burst(imm_recv_buffer_ring_, burst_buf_, IO_BURST_SIZE, nullptr);
    if (n_recv > 0) {
        work_done += n_recv;
        for (int i = 0; i < n_recv; ++i) {
            auto* ctx = (ImmRecvContext*)burst_buf_[i];
            
            for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                Assignment assign(reinterpret_cast<uintptr_t>(dummy_), 0, 0, 0, sizeof(int64_t));
                std::vector<Assignment> batch_vec{assign};
                
                // Use RDMAAssign type here
                RDMAAssign* assign_ptr = &(ctx->assigns_[qpi]);

                assign_ptr->reset(OpCode::RECV, qpi, batch_vec, 
                                  [ctx, qpi](int32_t status, int32_t imm) { 
                                      ctx->signal->set_comm_done(qpi); 
                                  });

                data_channel_->post_recv_batch(qpi, assign_ptr);
            }
            ctx->state_ = IOContextState::POSTED;
        }
    }

    return work_done;
}

int32_t RDMAIOEndpoint::wait(int32_t slot_id) {
    auto* ctx = &read_write_ctx_pool_[slot_id];
    ctx->signal->wait_comm_done_cpu(ctx->expected_mask);
    return 0;
}

int32_t RDMAIOEndpoint::waitRecv(int32_t slot_id) {
    auto* ctx = &imm_recv_ctx_pool_[slot_id];
    ctx->signal->wait_comm_done_cpu(ctx->expected_mask);
    return 0;
}

} // namespace slime