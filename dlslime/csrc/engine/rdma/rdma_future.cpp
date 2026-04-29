#include "rdma_future.h"

#include <stdexcept>

#include "dlslime/csrc/device/signal.h"
#include "rdma_endpoint.h"
#include "rdma_env.h"
#include "rdma_op_state.h"

namespace dlslime {

SendFuture::SendFuture(std::shared_ptr<EndpointOpState> op_state): op_state_(std::move(op_state))
{
    if (!op_state_) {
        throw std::runtime_error("SendFuture created with null EndpointOpState");
    }
}

int32_t SendFuture::wait() const
{
    if (op_state_->signal) {
        op_state_->signal->wait_comm_done_cpu(op_state_->expected_mask);
    }
    return static_cast<int32_t>(op_state_->completion_status.load(std::memory_order_acquire));
}

RecvFuture::RecvFuture(std::shared_ptr<EndpointOpState> op_state): op_state_(std::move(op_state))
{
    if (!op_state_) {
        throw std::runtime_error("RecvFuture created with null EndpointOpState");
    }
}

int32_t RecvFuture::wait() const
{
    if (op_state_->signal) {
        op_state_->signal->wait_comm_done_cpu(op_state_->expected_mask);
    }
    return static_cast<int32_t>(op_state_->completion_status.load(std::memory_order_acquire));
}

ReadWriteFuture::ReadWriteFuture(std::shared_ptr<EndpointOpState> op_state): op_state_(std::move(op_state))
{
    if (!op_state_) {
        throw std::runtime_error("ReadWriteFuture created with null EndpointOpState");
    }
}

int32_t ReadWriteFuture::wait() const
{
    if (op_state_->signal) {
        op_state_->signal->wait_comm_done_cpu(op_state_->expected_mask);
    }
    if (op_state_->completion_status.load(std::memory_order_acquire) != RDMAAssign::SUCCESS) {
        throw std::runtime_error("RDMA read/write completion failed");
    }
    return 0;
}

ImmRecvFuture::ImmRecvFuture(std::shared_ptr<EndpointOpState> op_state): op_state_(std::move(op_state))
{
    if (!op_state_) {
        throw std::runtime_error("ImmRecvFuture created with null EndpointOpState");
    }
}

int32_t ImmRecvFuture::wait() const
{
    if (op_state_->signal) {
        op_state_->signal->wait_comm_done_cpu(op_state_->expected_mask);
    }
    return static_cast<int32_t>(op_state_->completion_status.load(std::memory_order_acquire));
}

int32_t ImmRecvFuture::immData() const
{
    return op_state_->imm_data.load(std::memory_order_acquire);
}

bool ImmRecvFuture::timeTraceEnabled() const
{
    return SLIME_WITH_TIME_TRACE != 0;
}

uint64_t ImmRecvFuture::timeTraceStartNs() const
{
    return op_state_->trace_start_ns.load(std::memory_order_acquire);
}

uint64_t ImmRecvFuture::timeTraceEndNs() const
{
    return op_state_->trace_end_ns.load(std::memory_order_acquire);
}

uint64_t ImmRecvFuture::timeTraceElapsedNs() const
{
    uint64_t start_ns = timeTraceStartNs();
    uint64_t end_ns   = timeTraceEndNs();
    if (start_ns == 0 || end_ns <= start_ns) {
        return 0;
    }
    return end_ns - start_ns;
}

}  // namespace dlslime
