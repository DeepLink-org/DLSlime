#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include "dlslime/csrc/common/pause.h"
#include "dlslime/csrc/device/device_future.h"
#include "tcp_op_state.h"

namespace dlslime {
namespace tcp {

class TcpFuture : public DeviceFuture {
public:
    explicit TcpFuture(std::shared_ptr<TcpOpState> op)
        : op_state_(std::move(op)) {
        if (!op_state_)
            throw std::runtime_error("TcpFuture: null op_state");
    }

    int32_t wait() const override {
        if (op_state_->signal)
            op_state_->signal->wait_comm_done_cpu(op_state_->expected_mask);
        return op_state_->completion_status.load(std::memory_order_acquire);
    }

    // Block up to timeout_ms milliseconds.  Returns true iff completed;
    // writes status to *out.  On timeout the operation is still in-flight.
    bool wait_for(int64_t timeout_ms, int32_t* out) const {
        auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);
        while (true) {
            if (op_state_->signal) {
                uint32_t m = op_state_->signal->get_comm_done_mask();
                if ((m & op_state_->expected_mask) == op_state_->expected_mask) {
                    if (out) *out = op_state_->completion_status.load(
                                       std::memory_order_acquire);
                    return true;
                }
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                if (op_state_->signal) {
                    uint32_t m = op_state_->signal->get_comm_done_mask();
                    if ((m & op_state_->expected_mask) == op_state_->expected_mask) {
                        if (out) *out = op_state_->completion_status.load(
                                           std::memory_order_acquire);
                        return true;
                    }
                }
                return false;
            }
            machnet_pause();
        }
    }

protected:
    std::shared_ptr<TcpOpState> op_state_;
};

class TcpSendFuture      : public TcpFuture { public: using TcpFuture::TcpFuture; };
class TcpRecvFuture      : public TcpFuture { public: using TcpFuture::TcpFuture; };
class TcpReadWriteFuture : public TcpFuture { public: using TcpFuture::TcpFuture; };

}  // namespace tcp
}  // namespace dlslime
