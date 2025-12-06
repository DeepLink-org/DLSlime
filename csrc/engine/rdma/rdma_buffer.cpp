
#include "engine/rdma/rdma_buffer.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_env.h"
#include "logging.h"
#include <atomic>
#include <emmintrin.h>

namespace slime {

void RDMABuffer::send()
{
    send_completed_.store(0, std::memory_order_release); 
    endpointv0_->addBuffer(OpCode::SEND, shared_from_this());
}

void RDMABuffer::recv()
{
    recv_completed_.store(0, std::memory_order_release); 
    endpointv0_->addBuffer(OpCode::RECV, shared_from_this());
}

void RDMABuffer::sendDoneCallback()
{
    SLIME_LOG_DEBUG("Send done callback triggered");
    send_completed_.fetch_add(1);
}

void RDMABuffer::recvDoneCallback()
{
    SLIME_LOG_DEBUG("Recv done callback triggered");
    recv_completed_.fetch_add(1);
}

bool RDMABuffer::waitSend()
{
    if (send_completed_ == num_pack_) {
        return true;
    }
    while (send_completed_.load(std::memory_order_acquire) < num_pack_) _mm_pause();
    SLIME_LOG_INFO("complete to send the data.");
    return true;
}

bool RDMABuffer::waitRecv()
{
    if (recv_completed_ == 1) {
        return true;
    }

    while (recv_completed_.load(std::memory_order_acquire) < 1) _mm_pause();
    SLIME_LOG_INFO("complete to recv the data.");
    return true;
}
}  // namespace slime
