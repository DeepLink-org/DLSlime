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
    SLIME_LOG_INFO("USE_CUDA_SIGNAL");
    return std::make_shared<CudaDeviceSignal>();
#else
    SLIME_LOG_INFO("USE_CPU_SIGNAL");
    return std::make_shared<CPUDeviceSignal>();
#endif
}

void* get_current_stream_handle()
{
    return nullptr;
}

}  // namespace device
}  // namespace slime
