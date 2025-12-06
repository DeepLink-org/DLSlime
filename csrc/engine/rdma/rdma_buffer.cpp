
#include "engine/rdma/rdma_buffer.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_env.h"
#include "logging.h"

namespace slime {

void RDMABuffer::send()
{
    endpointv0_->addBuffer(OpCode::SEND, shared_from_this());
}

void RDMABuffer::recv()
{
    endpointv0_->addBuffer(OpCode::RECV, shared_from_this());
}

void RDMABuffer::sendDoneCallback()
{
    std::unique_lock<std::mutex> lock(send_mutex_);
    SLIME_LOG_DEBUG("Send done callback triggered");
    ++send_completed_;
    send_cv_.notify_all();
}

void RDMABuffer::recvDoneCallback()
{
    std::unique_lock<std::mutex> lock(recv_mutex_);
    SLIME_LOG_DEBUG("Recv done callback triggered");
    ++recv_completed_;
    recv_cv_.notify_all();
}

bool RDMABuffer::waitSend()
{
    std::unique_lock<std::mutex> lock(send_mutex_);

    if (send_completed_ == num_pack_) {
        return true;
    }
    send_cv_.wait(lock, [this]() { return send_completed_ >= num_pack_; });
    send_pending_ = false;
    SLIME_LOG_INFO("complete to send the data.");
    return true;
}

bool RDMABuffer::waitRecv()
{
    std::unique_lock<std::mutex> lock(recv_mutex_);

    if (recv_completed_ == 1) {
        return true;
    }

    recv_cv_.wait(lock, [this]() { return recv_completed_ >= 1; });
    recv_pending_ = false;
    SLIME_LOG_INFO("complete to recv the data.");
    return true;
}
}  // namespace slime
