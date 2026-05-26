#include "cuda_signal.h"
#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/device/host/host_signal.h"

namespace dlslime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass)
{
#ifdef SLIME_USE_CUDA
    if (bypass) {
        return std::make_shared<HostOnlySignal>();
    }
    return std::make_shared<CudaDeviceSignal>();
#else
    return std::make_shared<HostOnlySignal>();
#endif
}

}  // namespace device
}  // namespace dlslime
