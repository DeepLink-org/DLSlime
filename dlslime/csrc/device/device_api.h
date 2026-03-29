#pragma once
#include <memory>

#include "signal.h"

namespace dlslime {
namespace device {

std::shared_ptr<DeviceSignal> createSignal(bool bypass = false);

}  // namespace device
}  // namespace dlslime
