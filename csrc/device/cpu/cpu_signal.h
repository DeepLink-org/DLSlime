#pragma once
#include "logging.h"
#include "device/signal.h"
#include <cstdint>

namespace slime {
namespace device {

class CPUDeviceSignal : public DeviceSignal {
public:
    CPUDeviceSignal() {};
    ~CPUDeviceSignal() = default;

    void init() {};

    void record_on_stream(void* stream_handle) {}

    void wait_on_stream(void* stream_handle, uint32_t target_val=1) {}
    void set_signal_from_cpu(int val = 1) {}

    bool is_busy() {return false;};

    void reset() {};
};

} // namespace device
} // namespace slime
