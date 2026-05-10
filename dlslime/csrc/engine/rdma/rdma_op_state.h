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

    // ============================================================
    // Observability metadata (v0: one-sided read/write/writeWithImm).
    //
    // Semantic accounting is anchored here — not on RDMAAssign or CQ
    // callbacks — so that one user-visible op decrements pending_ops
    // exactly once regardless of num_qp or slot count.
    //
    // obs_enabled is the per-op gate: set to obs::obs_enabled() at
    // submit time so callbacks can short-circuit cheaply.
    // obs_completed is the once-only guard: callbacks do
    //   !obs_completed.exchange(true, acq_rel)
    // and the winner records completion (success or failure).
    // ============================================================
    uint64_t          obs_bytes{0};
    uint32_t          obs_assign_count{0};
    uint8_t           obs_op{0};  // ObsOpIndex
    uint16_t          obs_nic_id{0};
    bool              obs_enabled{false};
    std::atomic<bool> obs_completed{false};
};

}  // namespace dlslime
