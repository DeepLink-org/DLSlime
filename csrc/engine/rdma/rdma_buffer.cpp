
#include "engine/rdma/rdma_buffer.h"

namespace slime {

void RDMABuffer::send()
{
    endpoint_->addSendTask(shared_from_this());
}

void RDMABuffer::recv()
{
    endpoint_->addRecvTask(shared_from_this());
}

void RDMABuffer::send_done_callback()
{
    std::unique_lock<std::mutex> lock(send_mutex_);
    ++send_completed_;
    send_cv_.notify_all();
}

void RDMABuffer::recv_done_callback()
{
    std::unique_lock<std::mutex> lock(recv_mutex_);
    ++recv_completed_;
    recv_cv_.notify_all();
}

bool RDMABuffer::waitSend()
{
    std::unique_lock<std::mutex> lock(send_mutex_);

    if (send_completed_)
        return send_completed_;

    send_cv_.wait(lock, [this]() { return send_completed_ > 0; });
    send_pending_ = false;
    SLIME_LOG_INFO("complete to send the data.");
    return send_completed_;
}

bool RDMABuffer::waitRecv()
{
    std::unique_lock<std::mutex> lock(recv_mutex_);

    if (recv_completed_)
        return recv_completed_;

    // waiting for the recv complete...
    recv_cv_.wait(lock, [this]() { return recv_completed_ > 0; });
    recv_pending_ = false;
    SLIME_LOG_INFO("complete to recv the data.");

    return recv_completed_;
}

}  // namespace slime
