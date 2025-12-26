#include "rdma_future.h"

#include "device/signal.h"
#include "rdma_io_endpoint.h"
#include "rdma_msg_endpoint.h"

#include <stdexcept>

namespace slime {

SendFuture::SendFuture(SendContext* ctx): ctx_(ctx)
{
    if (!ctx_) {
        throw std::runtime_error("ImmFuture created with null context");
    }
}

int32_t SendFuture::wait() const
{
    if (ctx_->signal) {
        ctx_->signal->wait_comm_done_cpu(ctx_->expected_mask);
    }
    return 0;
}

RecvFuture::RecvFuture(RecvContext* ctx): ctx_(std::move(ctx))
{
    if (!ctx_) {
        throw std::runtime_error("ImmFuture created with null context");
    }
}

int32_t RecvFuture::wait() const
{
    if (ctx_->signal) {
        ctx_->signal->wait_comm_done_cpu(ctx_->expected_mask);
    }
    return 0;
}

ReadWriteFuture::ReadWriteFuture(ReadWriteContext* ctx): ctx_(std::move(ctx))
{
    if (!ctx_) {
        throw std::runtime_error("ImmFuture created with null context");
    }
}

int32_t ReadWriteFuture::wait() const
{
    if (ctx_->signal) {
        ctx_->signal->wait_comm_done_cpu(ctx_->expected_mask);
    }
    return 0;
}

ImmRecvFuture::ImmRecvFuture(ImmRecvContext* ctx): ctx_(std::move(ctx))
{
    if (!ctx_) {
        throw std::runtime_error("ImmFuture created with null context");
    }
}

int32_t ImmRecvFuture::wait() const
{
    if (ctx_->signal) {
        ctx_->signal->wait_comm_done_cpu(ctx_->expected_mask);
    }
    return 0;
}

int32_t ImmRecvFuture::immData() const
{
    return ctx_->assigns_[0].imm_data_;
}

}  // namespace slime
