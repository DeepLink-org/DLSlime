#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "dlslime/csrc/device/device_future.h"
#include "dlslime/csrc/device/signal.h"
#include "dlslime/csrc/engine/assignment.h"

namespace dlslime {

// Forward declaration
struct AscendContext;

/**
 * @brief Context state for Ascend async operations
 */
enum class AscendIOContextState {
    FREE,
    PENDING,
    POSTED,
    DONE
};

/**
 * @brief Context structure for Ascend read operations
 *
 * Tracks the state of an async read operation, including assignments
 * and completion signaling. Similar to ReadWriteContext in RDMA but
 * simplified for Ascend's model.
 */
struct AscendContext {
    int32_t slot_id{-1};

    // Device signal for async completion notification
    std::shared_ptr<dlslime::device::DeviceSignal> signal;

    // List of assignments for this operation
    std::vector<Assignment> assigns_;

    // Operation state
    AscendIOContextState state_{AscendIOContextState::FREE};

    // Completion tracking (for multi-device scenarios)
    std::atomic<bool> completed{false};
};

/**
 * @brief Future for Ascend Direct read operations
 *
 * Inherits from DeviceFuture to provide a consistent async interface
 * across different device types. Waits for Ascend operations to complete.
 */
class AscendFuture: public DeviceFuture {
public:
    /**
     * @brief Construct an AscendFuture
     * @param ctx Pointer to the AscendContext tracking this operation
     */
    explicit AscendFuture(AscendContext* ctx);

    /**
     * @brief Wait for the operation to complete
     * @return 0 on success, non-zero error code on failure
     */
    int32_t wait() const override;

private:
    AscendContext* ctx_;
};

}  // namespace dlslime
