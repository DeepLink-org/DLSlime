#include "ascend_future.h"

#include "dlslime/csrc/logging.h"

namespace dlslime {

AscendFuture::AscendFuture(AscendContext* ctx): ctx_(ctx)
{
    if (ctx_ == nullptr) {
        SLIME_LOG_ERROR("AscendFuture constructed with null context");
    }
}

int32_t AscendFuture::wait() const
{
    if (ctx_ == nullptr) {
        SLIME_LOG_ERROR("AscendFuture::wait() called with null context");
        return -1;
    }

    // If we have a device signal, use it for synchronization
    if (ctx_->signal) {
        // Wait on CPU for the operation to complete
        // This uses the DeviceSignal interface which abstracts
        // device-specific synchronization (CUDA events, ACL events, etc.)
        ctx_->signal->wait_comm_done_cpu(1);
    }

    // Mark as completed
    ctx_->completed.store(true, std::memory_order_release);
    ctx_->state_ = AscendIOContextState::DONE;

    return 0;
}

}  // namespace dlslime
