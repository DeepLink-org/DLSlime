#include "cuda_signal.h"
#include "dlslime/device/device_api.h"
#include "dlslime/device/host/host_signal.h"

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

void* get_current_stream_handle()
{
    return nullptr;
}

}  // namespace device
}  // namespace dlslime
