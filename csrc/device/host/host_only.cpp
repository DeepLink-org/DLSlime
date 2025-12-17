#include "device/device_api.h"
#include "device/host/host_signal.h"
#include "logging.h"

#include <cstddef>

namespace slime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass)
{
    SLIME_LOG_DEBUG("create signal cpu.");
    return std::make_shared<HostOnlySignal>();
}

void* get_current_stream_handle()
{
    return nullptr;
}

}  // namespace device
}  // namespace slime
