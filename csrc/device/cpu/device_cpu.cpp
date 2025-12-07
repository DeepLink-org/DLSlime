#include "device/cpu/cpu_signal.h"
#include "device/device_api.h"
#include "logging.h"

#include <cstddef>

namespace slime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass)
{
    SLIME_LOG_INFO("create signal cpu.");
    return std::make_shared<CPUDeviceSignal>();
}

void* get_current_stream_handle()
{
    return nullptr;
}

}  // namespace device
}  // namespace slime
