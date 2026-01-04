#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "dlslime/device/device_api.h"
#include "dlslime/engine/assignment.h"
#include "dlslime/jring.h"
#include "dlslime/json.hpp"
#include "memory_pool.h"
#include "rdma_assignment.h"
#include "rdma_channel.h"
#include "rdma_context.h"

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

    uint32_t       expected_mask;
    IOContextState state_ = IOContextState::FREE;
};

class RDMAIOEndpoint {
public:
    RDMAIOEndpoint() = default;
    ~RDMAIOEndpoint();

    explicit RDMAIOEndpoint(std::shared_ptr<RDMAContext>    ctx,
                            std::shared_ptr<RDMAMemoryPool> memory_pool,
                            size_t                          num_qp);

    void connect(const json& remote_endpoint_info);
    json endpointInfo() const;
    void cancelAll();

    int32_t process();

    std::shared_ptr<ReadWriteFuture> read(const std::vector<assign_tuple_t>&, void* stream);
    std::shared_ptr<ReadWriteFuture> write(const std::vector<assign_tuple_t>&, void* stream);
    std::shared_ptr<ReadWriteFuture> writeWithImm(const std::vector<assign_tuple_t>&, int32_t imm_data, void* stream);

    std::shared_ptr<ImmRecvFuture> immRecv(void* stream = nullptr);

private:
    void dummyReset(ImmRecvContext* ctx);

    int32_t
    dispatchTask(OpCode op_code, const std::vector<assign_tuple_t>&, int32_t imm_data = 0, void* stream = nullptr);

    int32_t readWriteProcess();
    int32_t immRecvProcess();

    std::shared_ptr<RDMAContext>    ctx_;
    std::shared_ptr<RDMAMemoryPool> memory_pool_;

    std::shared_ptr<RDMAChannel> data_channel_;

    size_t num_qp_;

    ReadWriteContext* read_write_ctx_pool_;
    ImmRecvContext*   imm_recv_ctx_pool_;

    std::vector<std::shared_ptr<ReadWriteFuture>> read_write_future_pool_;
    std::vector<std::shared_ptr<ImmRecvFuture>>   imm_recv_future_pool_;

    jring_t* read_write_buffer_ring_;
    jring_t* imm_recv_buffer_ring_;

    std::deque<ReadWriteContext*> pending_rw_queue_;

    std::atomic<uint64_t> rw_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    // Track how many Recvs we have actually posted to HW
    std::atomic<uint64_t> posted_recv_cnt_{0};

    std::atomic<int32_t> token_bucket_[64];

    // Scratchpad buffers
    void*    burst_buf_[IO_BURST_SIZE];
    int64_t* dummy_;
};

}  // namespace dlslime
