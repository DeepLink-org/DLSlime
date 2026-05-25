#include <cstddef>

#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/device/host/host_signal.h"
#include "dlslime/csrc/logging.h"

namespace dlslime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass)
{
    SLIME_LOG_DEBUG("create signal cpu.");
    return std::make_shared<HostOnlySignal>();
}

}  // namespace device
}  // namespace dlslime
