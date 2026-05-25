#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/device/signal.h"

namespace dlslime {
namespace tcp {

enum Status : int32_t {
    TCP_SUCCESS = 0,
    TCP_FAILED  = -1,
    TCP_TIMEOUT = -2,
    TCP_CLOSED  = -3,
};

// One per in-flight operation.  The io_context thread (or caller for sync
// ops) signals completion via DeviceSignal; the future's wait() spins on
// wait_comm_done_cpu().
struct TcpOpState {
    std::shared_ptr<dlslime::device::DeviceSignal> signal;
    uint32_t              expected_mask{1};
    std::atomic<uint32_t> completion_mask{0};
    std::atomic<int32_t>  completion_status{TCP_SUCCESS};

    uintptr_t user_buffer{0};
    size_t    user_length{0};
    size_t    bytes_copied{0};

    static std::shared_ptr<TcpOpState> create() {
        auto s = std::make_shared<TcpOpState>();
        s->signal = dlslime::device::createSignal(false);
        return s;
    }
};

}  // namespace tcp
}  // namespace dlslime
