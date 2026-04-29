#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "dlslime/csrc/device/signal.h"
#include "rdma_assignment.h"

namespace dlslime {

// EndpointOpState is the stable completion owner for one logical operation
// issued by an RDMAEndpoint. It exists so that user-visible futures and the
// endpoint progress loop do not fight over the same slot-local state when
// reusable contexts are recycled for later operations.
//
// Lifecycle:
//   * Created at user-submission time (send / recv / read / write /
//     writeWithImm / immRecv).
//   * Owned jointly by (a) the returned future, (b) any in-flight callback
//     that captured it by shared_ptr at submission time, and (c) the endpoint
//     in-flight registry.
//   * Released when the future is dropped and the final transport callback
//     has run.
//
// The DeviceSignal held here is unique to this operation; the endpoint used
// to pool one signal per slot, which is the exact source of the ABA hazard
// PR #70 addresses.
struct EndpointOpState {
    std::shared_ptr<dlslime::device::DeviceSignal> signal;

    // op_id is captured by transport callbacks at submission time. Slot
    // generation must match for the callback to touch slot-local transport
    // bookkeeping; otherwise the callback is treated as a stale completion
    // belonging to an already-retired operation.
    uint64_t op_id{0};

    uint32_t              expected_mask{0};
    std::atomic<uint32_t> completion_mask{0};
    std::atomic<int32_t>  completion_status{RDMAAssign::SUCCESS};
    std::atomic<int32_t>  imm_data{0};

    // Optional tracing fields used by the imm-recv fast path. Left as zeros
    // for operations that do not opt in to time tracing.
    std::atomic<uint64_t> trace_start_ns{0};
    std::atomic<uint64_t> trace_end_ns{0};
};

}  // namespace dlslime
