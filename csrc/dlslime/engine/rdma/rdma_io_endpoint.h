#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "dlslime/device/device_api.h"
#include "dlslime/engine/assignment.h"
#include "dlslime/utils.h"

#include "rdma_assignment.h"
#include "rdma_channel.h"
#include "rdma_context.h"

#include "dlslime/jring.h"
#include "dlslime/json.hpp"

namespace dlslime {

using json = nlohmann::json;

class ReadWriteFuture;
class ImmRecvFuture;

constexpr int IO_BURST_SIZE = 32;

enum class IOContextState {
    FREE,
    PENDING,
    WAIT_TOKEN,
    POSTED,
    DONE
};

// --- Read/Write Context (Initiator) ---
struct ReadWriteContext {
    int32_t slot_id;

    std::shared_ptr<dlslime::device::DeviceSignal> signal;

    std::vector<RDMAAssign> assigns_;

    uintptr_t local_ptr;
    uintptr_t remote_ptr;
    size_t    length;
    uint32_t  rkey;
    int32_t   imm_data;
    OpCode    op_code;
    uint32_t  expected_mask;

    std::atomic<uint32_t> finished_qp_mask{0};

    IOContextState state_ = IOContextState::FREE;
};

struct ImmRecvContext {
    int32_t                                        slot_id;
    std::shared_ptr<dlslime::device::DeviceSignal> signal;
    std::vector<RDMAAssign>                        assigns_;

    uint32_t              expected_mask;
    std::atomic<uint32_t> finished_qp_mask{0};
    std::atomic<int32_t>  completion_status{RDMAAssign::SUCCESS};
    std::atomic<int32_t>  imm_data{0};
    IOContextState        state_ = IOContextState::FREE;

    std::atomic<ImmRecvContext*> next_refill_{nullptr};
};

struct ImmRecvOpState {
    std::shared_ptr<dlslime::device::DeviceSignal> signal;
    uint32_t                                       expected_mask{0};
    std::atomic<int32_t>                           completion_status{RDMAAssign::SUCCESS};
    std::atomic<int32_t>                           imm_data{0};
};

struct ImmRecvEvent {
    int32_t status{RDMAAssign::SUCCESS};
    int32_t imm_data{0};
};

class RDMAIOEndpoint {
public:
    RDMAIOEndpoint() = default;
    ~RDMAIOEndpoint();

    explicit RDMAIOEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp);

    void connect(const json& remote_endpoint_info);
    json endpointInfo() const;

    int32_t process();

    std::shared_ptr<ReadWriteFuture> read(const std::vector<assign_tuple_t>&, void* stream);
    std::shared_ptr<ReadWriteFuture> write(const std::vector<assign_tuple_t>&, void* stream);
    std::shared_ptr<ReadWriteFuture> writeWithImm(const std::vector<assign_tuple_t>&, int32_t imm_data, void* stream);

    std::shared_ptr<ImmRecvFuture> immRecv(void* stream = nullptr);

private:
    void dummyReset(ImmRecvContext* ctx);
    void postImmRecvSlot(ImmRecvContext* ctx);
    void postImmRecvWindow();
    void completeImmRecvOp(const std::shared_ptr<ImmRecvOpState>& op_state, const ImmRecvEvent& event);
    void enqueueImmRecvCompletion(ImmRecvContext* ctx);
    void pushRefill(ImmRecvContext* ctx);
    ImmRecvContext* popAllRefill();

    int32_t
    dispatchTask(OpCode op_code, const std::vector<assign_tuple_t>&, int32_t imm_data = 0, void* stream = nullptr);

    int32_t readWriteProcess();
    int32_t immRecvProcess();

    std::shared_ptr<RDMAContext> ctx_;
    std::shared_ptr<RDMAChannel> data_channel_;
    size_t                       num_qp_;

    ReadWriteContext* read_write_ctx_pool_;
    ImmRecvContext*   imm_recv_ctx_pool_;

    std::vector<std::shared_ptr<ReadWriteFuture>> read_write_future_pool_;

    jring_t* read_write_buffer_ring_;
    jring_t* imm_recv_buffer_ring_;

    std::deque<ReadWriteContext*> pending_rw_queue_;

    std::atomic<uint64_t> rw_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    std::atomic<int32_t> token_bucket_[64];

    SpinLock                                    imm_recv_match_lock_;
    std::deque<std::shared_ptr<ImmRecvOpState>> pending_imm_recv_ops_;
    std::deque<ImmRecvEvent>                    completed_imm_recv_events_;
    std::atomic<ImmRecvContext*>                refill_head_{nullptr};

    // Scratchpad buffers
    void*    burst_buf_[IO_BURST_SIZE];
    int64_t* dummy_;
};

}  // namespace dlslime
