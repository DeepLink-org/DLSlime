#pragma once
#include <cstdint>

#include "dlslime/csrc/device/device_future.h"

namespace dlslime {

/**
 * @brief Future for NVLink operations
 *
 * Inherits from DeviceFuture for interface consistency.
 * NVLink operations complete synchronously via cudaMemcpyAsync,
 * so wait() is a no-op (stream synchronization happens externally).
 */
class NVLinkFuture: public DeviceFuture {
public:
    NVLinkFuture()  = default;
    ~NVLinkFuture() = default;

    int32_t wait() const override
    {
        return 0;
    }
};
}  // namespace dlslime
