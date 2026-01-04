#pragma once
#include <memory>

#include "signal.h"

namespace dlslime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass = false);

void* get_current_stream_handle();

}  // namespace device
}  // namespace dlslime
