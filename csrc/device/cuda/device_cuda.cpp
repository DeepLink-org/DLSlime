#include "cuda_signal.h"
#include "device/cpu/cpu_signal.h"
#include "device/device_api.h"
#include "logging.h"

namespace slime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass)
{
#ifdef SLIME_USE_CUDA
    if (bypass) {
        return std::make_shared<CPUDeviceSignal>();
    }
    return std::make_shared<CudaDeviceSignal>();
#else
    return std::make_shared<CPUDeviceSignal>();
#endif
}

void* get_current_stream_handle()
{
    return nullptr;
}

}  // namespace device
}  // namespace slime
