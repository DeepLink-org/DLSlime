#pragma once
#include <cstdint>

namespace slime {
namespace device {

class DeviceSignal {
public:
    virtual ~DeviceSignal() = default;

    virtual void init() = 0;

    virtual void record_on_stream(void* stream_handle) = 0;

    virtual void set_signal_from_cpu(int val) = 0;
    virtual void wait_on_stream(void* stream_handle, uint32_t target_val) = 0;

    virtual bool is_busy() = 0;

    virtual void reset() = 0;
};

}  // namespace device
}  // namespace slime
