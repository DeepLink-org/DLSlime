
#include "engine/rdma/rdma_buffer.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_env.h"
#include "logging.h"
#include <atomic>
#include <emmintrin.h>

namespace slime {

bool RDMABuffer::waitSend()
{
    signal_->wait_comm_done_cpu((1 << num_pack_) - 1);
    SLIME_LOG_DEBUG("complete to send the data.");
    return true;
}

bool RDMABuffer::waitRecv()
{
    signal_->wait_comm_done_cpu((1 << num_pack_) - 1);
    SLIME_LOG_DEBUG("complete to recv the data.");
    return true;
}
}  // namespace slime
