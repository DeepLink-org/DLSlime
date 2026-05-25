#pragma once

#include <cstdint>

namespace dlslime {

/**
 * @brief Base class for device-specific future implementations
 *
 * This provides a common interface for async operations across different
 * device types (RDMA, Ascend, NVLink, etc.). Each device implementation
 * inherits from this base class and implements the wait() method according
 * to its specific async model.
 */
class DeviceFuture {
public:
    virtual ~DeviceFuture() = default;

    /**
     * @brief Wait for the async operation to complete
     * @return 0 on success, non-zero error code on failure
     */
    virtual int32_t wait() const = 0;
};

}  // namespace dlslime
