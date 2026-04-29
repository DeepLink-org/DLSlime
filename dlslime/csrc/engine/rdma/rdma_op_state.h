#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "dlslime/csrc/device/signal.h"
#include "rdma_assignment.h"

namespace dlslime {

// EndpointOpState is the completion owner for one logical RDMAEndpoint
// operation (send / recv / read / write / writeWithImm / immRecv).
//
// It is the ONLY object user-visible futures observe, and the ONLY object
// transport callbacks update for user-visible state. It does not know what
// transport resources (QPs, RECVs, WRs) are serving it; that is a slot
// concern.
//
// Lifetime
// --------
// An op_state is created at user submission time and released when both
// (a) the returned future is dropped and (b) every transport callback that
// captured a shared_ptr to it has fired. Slots do not own the op_state;
// they merely reference the current tenant for the duration of one lease.
//
// Thread-safety
// -------------
// All user-visible fields are atomic or immutable after construction. The
// signal is the synchronization primitive a future.wait() blocks on; the
// completion_mask / completion_status / imm_data atomics carry the result.
struct EndpointOpState {
    std::shared_ptr<dlslime::device::DeviceSignal> signal;

    uint32_t              expected_mask{0};
    std::atomic<uint32_t> completion_mask{0};
    std::atomic<int32_t>  completion_status{RDMAAssign::SUCCESS};
    std::atomic<int32_t>  imm_data{0};

    // Optional tracing for the imm-recv fast path. Zero if unused.
    std::atomic<uint64_t> trace_start_ns{0};
    std::atomic<uint64_t> trace_end_ns{0};
};

}  // namespace dlslime
