#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "engine/rdma/rdma_assignment.h" // 确保这里定义了 RDMAAssign
#include "engine/rdma/rdma_channel.h"
#include "engine/rdma/rdma_context.h"
#include "device/device_api.h" 
#include "jring.h"
#include "json.hpp"

namespace slime {

using json = nlohmann::json;

constexpr int MAX_IO_FIFO_DEPTH = 1024;
constexpr int IO_BURST_SIZE     = 32;

enum class IOContextState {
    FREE,
    PENDING,
    POSTED,
    DONE
};

struct ReadWriteContext {
    int32_t         slot_id;
    
    // Modified: use shared_ptr for DeviceSignal
    std::shared_ptr<slime::device::DeviceSignal> signal;
    
    // Modified: use RDMAAssign instead of RDMAAssignment
    // Vector size will be resized to num_qp_ in constructor for striping
    std::vector<RDMAAssign> assigns_; 

    uintptr_t local_ptr;
    uintptr_t remote_ptr;
    size_t    length;
    uint32_t  rkey;
    int32_t   imm_data;
    
    OpCode    op_code; 
    uint32_t  expected_mask;
    
    IOContextState state_ = IOContextState::FREE;
};

struct ImmRecvContext {
    int32_t         slot_id;
    
    // Modified: use shared_ptr for DeviceSignal
    std::shared_ptr<slime::device::DeviceSignal> signal;
    
    // Modified: use RDMAAssign
    std::vector<RDMAAssign> assigns_;
    
    uint32_t  expected_mask;
    IOContextState state_ = IOContextState::FREE;
};

class RDMAIOEndpoint {
public:
    RDMAIOEndpoint() = default;
    ~RDMAIOEndpoint();

    explicit RDMAIOEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp);

    void connect(const json& remote_endpoint_info);
    json endpointInfo() const;

    int32_t registerMemoryRegion(uintptr_t ptr, size_t length);

    int32_t process();

    // Initiator Operations
    int32_t read(uintptr_t local_ptr, uintptr_t remote_ptr, size_t len, uint32_t rkey, void* stream = nullptr);
    int32_t write(uintptr_t local_ptr, uintptr_t remote_ptr, size_t len, uint32_t rkey, void* stream = nullptr);
    int32_t writeWithImm(uintptr_t local_ptr, uintptr_t remote_ptr, size_t len, uint32_t rkey, int32_t imm_data, void* stream = nullptr);

    // Target Operations
    int32_t recvImm(void* stream = nullptr);

    // Synchronization
    int32_t wait(int32_t slot_id);
    int32_t waitRecv(int32_t slot_id);

private:
    jring_t* createRing(const char* name, size_t count);
    void     freeRing(jring_t* ring);

    std::shared_ptr<RDMAContext> ctx_;
    std::shared_ptr<RDMAChannel> data_channel_;
    size_t                       num_qp_;

    ReadWriteContext* read_write_ctx_pool_;
    ImmRecvContext* imm_recv_ctx_pool_;

    jring_t* read_write_buffer_ring_;
    jring_t* imm_recv_buffer_ring_;

    std::atomic<uint64_t> rw_slot_id_{0};
    std::atomic<uint64_t> recv_slot_id_{0};

    void* burst_buf_[IO_BURST_SIZE];
    int64_t* dummy_; 
};

}  // namespace slime