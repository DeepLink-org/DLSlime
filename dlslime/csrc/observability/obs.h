#pragma once

/// @file obs.h
/// @brief Low-overhead observability counters for DLSlime RDMA data plane.
///
/// Design constraints (hot-path safe):
///   - No Redis, JSON, malloc, mutex, or string operations on hot path.
///   - All record functions are gated by `obs_enabled()` (single branch).
///   - Counters use std::atomic<uint64_t> + memory_order_relaxed.
///   - Counter structs are alignas(64) to avoid false sharing.
///   - NIC slots are pre-allocated (fixed array, no map lookup).
///
/// Enable via: export DLSLIME_OBS=1
/// Snapshot (slow path only): obs_snapshot_json()

#include <atomic>
#include <cstdint>
#include <cstring>

#include "dlslime/csrc/common/json.hpp"

namespace dlslime {
namespace obs {

// ============================================================
// Op index enum — maps to fixed array slots, not string labels
// ============================================================

enum ObsOpIndex : uint8_t {
    OBS_OP_READ = 0,
    OBS_OP_WRITE,
    OBS_OP_WRITE_WITH_IMM,
    OBS_OP_SEND,
    OBS_OP_RECV,
    OBS_OP_IMM_RECV,
    OBS_OP_COUNT  // sentinel
};

/// Map from engine OpCode to ObsOpIndex.  Defined as constexpr array
/// so the hot-path lookup is a single indexed load.
/// OpCode enum: READ=0, WRITE=1, SEND=2, RECV=3, SEND_WITH_IMM=4, WRITE_WITH_IMM=5
inline constexpr ObsOpIndex OPCODE_TO_OBS[] = {
    OBS_OP_READ,           // OpCode::READ          = 0
    OBS_OP_WRITE,          // OpCode::WRITE         = 1
    OBS_OP_SEND,           // OpCode::SEND          = 2
    OBS_OP_RECV,           // OpCode::RECV          = 3
    OBS_OP_SEND,           // OpCode::SEND_WITH_IMM = 4  (same counter as SEND)
    OBS_OP_WRITE_WITH_IMM  // OpCode::WRITE_WITH_IMM= 5
};

inline const char* obs_op_name(ObsOpIndex op)
{
    static const char* names[] = {"read", "write", "write_with_imm", "send", "recv", "imm_recv"};
    return (op < OBS_OP_COUNT) ? names[op] : "unknown";
}

// ============================================================
// Counter structs
// ============================================================

constexpr int OBS_MAX_NICS = 16;

/// Peer-level aggregate counters.
struct alignas(64) PeerCounters {
    std::atomic<uint64_t> assign_total{0};
    std::atomic<uint64_t> batch_total{0};
    std::atomic<uint64_t> submitted_bytes_total{0};
    std::atomic<uint64_t> completed_bytes_total{0};
    std::atomic<uint64_t> failed_bytes_total{0};
    std::atomic<int64_t>  pending_ops{0};
    std::atomic<uint64_t> error_total{0};

    // Per-op pending breakdown (peer-level authoritative; stays correct
    // even if nic_id == -1 at the call site).
    std::atomic<int64_t> pending_by_op[OBS_OP_COUNT]{};

    // User-registered MRs. Back-compat aliases mr_count / mr_bytes map
    // to these.
    std::atomic<uint64_t> user_mr_count{0};
    std::atomic<uint64_t> user_mr_bytes{0};
    // System MRs (internal bookkeeping: sys.io_dummy, sys.msg_dummy,
    // sys.send_ctx). Tracked separately so user-facing MR counts are
    // not inflated by internal allocations.
    std::atomic<uint64_t> sys_mr_count{0};
    std::atomic<uint64_t> sys_mr_bytes{0};
};

/// Per-NIC counters with per-op breakdown.
struct alignas(64) NicCounters {
    char device_name[64]{};
    bool active{false};

    // Semantic-level (from RDMAEndpoint)
    std::atomic<uint64_t> assign_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> batch_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> submitted_bytes_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> completed_bytes_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> failed_bytes_total[OBS_OP_COUNT]{};
    std::atomic<int64_t>  pending_ops[OBS_OP_COUNT]{};
    std::atomic<uint64_t> error_total[OBS_OP_COUNT]{};

    // Transport-level (from RDMAChannel)
    std::atomic<uint64_t> post_batch_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> post_wr_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> post_bytes_total[OBS_OP_COUNT]{};
    std::atomic<uint64_t> post_failures_total[OBS_OP_COUNT]{};

    // CQ-level
    std::atomic<uint64_t> cq_errors_total{0};
};

// ============================================================
// Global state
// ============================================================

/// Returns true if observability is enabled (DLSLIME_OBS=1).
/// Evaluated once at first call via static local.
bool obs_enabled();

// ============================================================
// Slow-path API (init / snapshot)
// ============================================================

/// Register a NIC device name, returns a nic_id (0-based slot index).
/// Thread-safe (uses atomic CAS). Called during RDMAContext::init.
int obs_register_nic(const char* device_name);

/// Produce a JSON snapshot of all counters (slow path, allocates).
nlohmann::json obs_snapshot_json();

/// Reset all counters (for testing only).
void obs_reset_for_test();

// ============================================================
// Hot-path recording API — all inline, gated by obs_enabled()
// ============================================================

/// Record a semantic submit (called from RDMAEndpoint for one-sided ops only).
inline void obs_record_submit(int nic_id, ObsOpIndex op, uint32_t assign_count, uint64_t bytes)
{
    if (!obs_enabled())
        return;

    // Access global state defined in obs.cpp via extern declarations.
    extern PeerCounters g_peer;
    extern NicCounters  g_nics[OBS_MAX_NICS];

    g_peer.assign_total.fetch_add(assign_count, std::memory_order_relaxed);
    g_peer.batch_total.fetch_add(1, std::memory_order_relaxed);
    g_peer.submitted_bytes_total.fetch_add(bytes, std::memory_order_relaxed);
    g_peer.pending_ops.fetch_add(1, std::memory_order_relaxed);
    if (op < OBS_OP_COUNT) {
        g_peer.pending_by_op[op].fetch_add(1, std::memory_order_relaxed);
    }

    if (nic_id >= 0 && nic_id < OBS_MAX_NICS) {
        auto& nic = g_nics[nic_id];
        nic.assign_total[op].fetch_add(assign_count, std::memory_order_relaxed);
        nic.batch_total[op].fetch_add(1, std::memory_order_relaxed);
        nic.submitted_bytes_total[op].fetch_add(bytes, std::memory_order_relaxed);
        nic.pending_ops[op].fetch_add(1, std::memory_order_relaxed);
    }
}

/// Record a successful completion. Must be called at most once per semantic
/// user op — idempotency is enforced at the call site via
/// EndpointOpState::obs_completed.exchange(), not inside this primitive.
inline void obs_record_complete(int nic_id, ObsOpIndex op, uint64_t bytes)
{
    if (!obs_enabled())
        return;

    extern PeerCounters g_peer;
    extern NicCounters  g_nics[OBS_MAX_NICS];

    g_peer.completed_bytes_total.fetch_add(bytes, std::memory_order_relaxed);
    g_peer.pending_ops.fetch_sub(1, std::memory_order_relaxed);
    if (op < OBS_OP_COUNT) {
        g_peer.pending_by_op[op].fetch_sub(1, std::memory_order_relaxed);
    }

    if (nic_id >= 0 && nic_id < OBS_MAX_NICS) {
        auto& nic = g_nics[nic_id];
        nic.completed_bytes_total[op].fetch_add(bytes, std::memory_order_relaxed);
        nic.pending_ops[op].fetch_sub(1, std::memory_order_relaxed);
    }
}

/// Record a failure. Same once-per-op contract as obs_record_complete.
inline void obs_record_fail(int nic_id, ObsOpIndex op, uint64_t bytes)
{
    if (!obs_enabled())
        return;

    extern PeerCounters g_peer;
    extern NicCounters  g_nics[OBS_MAX_NICS];

    g_peer.failed_bytes_total.fetch_add(bytes, std::memory_order_relaxed);
    g_peer.pending_ops.fetch_sub(1, std::memory_order_relaxed);
    g_peer.error_total.fetch_add(1, std::memory_order_relaxed);
    if (op < OBS_OP_COUNT) {
        g_peer.pending_by_op[op].fetch_sub(1, std::memory_order_relaxed);
    }

    if (nic_id >= 0 && nic_id < OBS_MAX_NICS) {
        auto& nic = g_nics[nic_id];
        nic.failed_bytes_total[op].fetch_add(bytes, std::memory_order_relaxed);
        nic.pending_ops[op].fetch_sub(1, std::memory_order_relaxed);
        nic.error_total[op].fetch_add(1, std::memory_order_relaxed);
    }
}

/// Record a transport-level post batch (called from RDMAChannel).
inline void obs_record_post_batch(int nic_id, ObsOpIndex op, uint32_t wr_count, uint64_t bytes)
{
    if (!obs_enabled())
        return;

    extern NicCounters g_nics[OBS_MAX_NICS];

    if (nic_id >= 0 && nic_id < OBS_MAX_NICS) {
        auto& nic = g_nics[nic_id];
        nic.post_batch_total[op].fetch_add(1, std::memory_order_relaxed);
        nic.post_wr_total[op].fetch_add(wr_count, std::memory_order_relaxed);
        nic.post_bytes_total[op].fetch_add(bytes, std::memory_order_relaxed);
    }
}

/// Record a transport-level post failure (ibv_post_send/recv returned error).
inline void obs_record_post_failure(int nic_id, ObsOpIndex op)
{
    if (!obs_enabled())
        return;

    extern NicCounters g_nics[OBS_MAX_NICS];

    if (nic_id >= 0 && nic_id < OBS_MAX_NICS) {
        g_nics[nic_id].post_failures_total[op].fetch_add(1, std::memory_order_relaxed);
    }
}

/// Record a CQ error (wc.status != IBV_WC_SUCCESS and not flush).
inline void obs_record_cq_error(int nic_id)
{
    if (!obs_enabled())
        return;

    extern PeerCounters g_peer;
    extern NicCounters  g_nics[OBS_MAX_NICS];

    g_peer.error_total.fetch_add(1, std::memory_order_relaxed);

    if (nic_id >= 0 && nic_id < OBS_MAX_NICS) {
        g_nics[nic_id].cq_errors_total.fetch_add(1, std::memory_order_relaxed);
    }
}

/// Record an MR registration.
inline void obs_record_mr_register(uint64_t bytes, bool is_system)
{
    if (!obs_enabled())
        return;

    extern PeerCounters g_peer;

    if (is_system) {
        g_peer.sys_mr_count.fetch_add(1, std::memory_order_relaxed);
        g_peer.sys_mr_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }
    else {
        g_peer.user_mr_count.fetch_add(1, std::memory_order_relaxed);
        g_peer.user_mr_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }
}

/// Record an MR unregistration.
inline void obs_record_mr_unregister(uint64_t bytes, bool is_system)
{
    if (!obs_enabled())
        return;

    extern PeerCounters g_peer;

    if (is_system) {
        g_peer.sys_mr_count.fetch_sub(1, std::memory_order_relaxed);
        g_peer.sys_mr_bytes.fetch_sub(bytes, std::memory_order_relaxed);
    }
    else {
        g_peer.user_mr_count.fetch_sub(1, std::memory_order_relaxed);
        g_peer.user_mr_bytes.fetch_sub(bytes, std::memory_order_relaxed);
    }
}

/// Record a delta increase to an existing MR (used when re-registering an
/// MR with a larger length). bytes is the positive delta.
inline void obs_record_mr_grow(uint64_t delta_bytes, bool is_system)
{
    if (!obs_enabled() || delta_bytes == 0)
        return;

    extern PeerCounters g_peer;

    if (is_system) {
        g_peer.sys_mr_bytes.fetch_add(delta_bytes, std::memory_order_relaxed);
    }
    else {
        g_peer.user_mr_bytes.fetch_add(delta_bytes, std::memory_order_relaxed);
    }
}

}  // namespace obs
}  // namespace dlslime
