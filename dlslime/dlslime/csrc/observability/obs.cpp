#include "dlslime/csrc/observability/obs.h"

#include <cstdlib>
#include <cstring>
#include <mutex>

namespace dlslime {
namespace obs {

// ============================================================
// Global counter storage — defined here, extern-declared in obs.h
// ============================================================

PeerCounters            g_peer;
NicCounters             g_nics[OBS_MAX_NICS];
static std::atomic<int> g_nic_count{0};
static std::mutex       g_nic_register_mu;

// ============================================================
// obs_enabled(): one-shot init from DLSLIME_OBS env var
// ============================================================

bool obs_enabled()
{
    static const bool enabled = [] {
        const char* val = std::getenv("DLSLIME_OBS");
        return val != nullptr && val[0] == '1' && val[1] == '\0';
    }();
    return enabled;
}

// ============================================================
// obs_register_nic(): allocate a fixed slot for a NIC device
//
// Slow path — concurrent PeerAgent endpoint creation may race on the
// same device_name. A std::mutex makes the check-then-insert atomic
// so each device_name maps to exactly one nic_id. The hot path still
// reads directly from the fixed g_nics[] array via nic_id.
// ============================================================

int obs_register_nic(const char* device_name)
{
    if (!device_name)
        return -1;

    std::lock_guard<std::mutex> lk(g_nic_register_mu);

    int count = g_nic_count.load(std::memory_order_acquire);
    for (int i = 0; i < count; ++i) {
        if (g_nics[i].active && std::strncmp(g_nics[i].device_name, device_name, 63) == 0) {
            return i;
        }
    }

    if (count >= OBS_MAX_NICS) {
        return -1;  // out of slots
    }

    int slot = count;
    std::strncpy(g_nics[slot].device_name, device_name, 63);
    g_nics[slot].device_name[63] = '\0';
    g_nics[slot].active          = true;

    // Publish the new count last so the lock-free readers in
    // obs_snapshot_json() observe a fully-initialized slot.
    g_nic_count.store(slot + 1, std::memory_order_release);

    return slot;
}

// ============================================================
// obs_snapshot_json(): slow path — reads all atomics, builds JSON
// ============================================================

nlohmann::json obs_snapshot_json()
{
    using json = nlohmann::json;

    if (!obs_enabled()) {
        return json{{"enabled", false}};
    }

    // Peer-level summary
    json summary;
    summary["assign_total"]          = g_peer.assign_total.load(std::memory_order_relaxed);
    summary["batch_total"]           = g_peer.batch_total.load(std::memory_order_relaxed);
    summary["submitted_bytes_total"] = g_peer.submitted_bytes_total.load(std::memory_order_relaxed);
    summary["completed_bytes_total"] = g_peer.completed_bytes_total.load(std::memory_order_relaxed);
    summary["failed_bytes_total"]    = g_peer.failed_bytes_total.load(std::memory_order_relaxed);
    summary["pending_ops"]           = g_peer.pending_ops.load(std::memory_order_relaxed);
    summary["error_total"]           = g_peer.error_total.load(std::memory_order_relaxed);

    uint64_t user_mr_count   = g_peer.user_mr_count.load(std::memory_order_relaxed);
    uint64_t user_mr_bytes   = g_peer.user_mr_bytes.load(std::memory_order_relaxed);
    summary["user_mr_count"] = user_mr_count;
    summary["user_mr_bytes"] = user_mr_bytes;
    summary["sys_mr_count"]  = g_peer.sys_mr_count.load(std::memory_order_relaxed);
    summary["sys_mr_bytes"]  = g_peer.sys_mr_bytes.load(std::memory_order_relaxed);
    // Back-compat aliases: mr_count / mr_bytes now mean user-MR counts.
    summary["mr_count"] = user_mr_count;
    summary["mr_bytes"] = user_mr_bytes;

    // Peer-level per-op pending breakdown (authoritative, unaffected by
    // NIC registration state).
    json pending_by_op = json::object();
    for (int op = 0; op < OBS_OP_COUNT; ++op) {
        pending_by_op[obs_op_name(static_cast<ObsOpIndex>(op))] =
            g_peer.pending_by_op[op].load(std::memory_order_relaxed);
    }
    summary["pending_by_op"] = pending_by_op;

    // Per-NIC
    json nics_arr = json::array();
    int  count    = g_nic_count.load(std::memory_order_acquire);
    for (int i = 0; i < count && i < OBS_MAX_NICS; ++i) {
        if (!g_nics[i].active)
            continue;

        auto& nic = g_nics[i];

        // Aggregate across ops for the summary fields
        uint64_t n_assign = 0, n_batch = 0, n_sub_bytes = 0;
        uint64_t n_comp_bytes = 0, n_fail_bytes = 0;
        int64_t  n_pending    = 0;
        uint64_t n_errors     = 0;
        uint64_t n_post_batch = 0, n_post_wr = 0, n_post_bytes = 0;
        uint64_t n_post_fail = 0;

        json by_op = json::object();
        for (int op = 0; op < OBS_OP_COUNT; ++op) {
            uint64_t a   = nic.assign_total[op].load(std::memory_order_relaxed);
            uint64_t b   = nic.batch_total[op].load(std::memory_order_relaxed);
            uint64_t sb  = nic.submitted_bytes_total[op].load(std::memory_order_relaxed);
            uint64_t cb  = nic.completed_bytes_total[op].load(std::memory_order_relaxed);
            uint64_t fb  = nic.failed_bytes_total[op].load(std::memory_order_relaxed);
            int64_t  p   = nic.pending_ops[op].load(std::memory_order_relaxed);
            uint64_t e   = nic.error_total[op].load(std::memory_order_relaxed);
            uint64_t pb  = nic.post_batch_total[op].load(std::memory_order_relaxed);
            uint64_t pw  = nic.post_wr_total[op].load(std::memory_order_relaxed);
            uint64_t pby = nic.post_bytes_total[op].load(std::memory_order_relaxed);
            uint64_t pf  = nic.post_failures_total[op].load(std::memory_order_relaxed);

            n_assign += a;
            n_batch += b;
            n_sub_bytes += sb;
            n_comp_bytes += cb;
            n_fail_bytes += fb;
            n_pending += p;
            n_errors += e;
            n_post_batch += pb;
            n_post_wr += pw;
            n_post_bytes += pby;
            n_post_fail += pf;

            // Include per-op detail if non-zero
            if (a > 0 || b > 0 || sb > 0 || cb > 0) {
                by_op[obs_op_name(static_cast<ObsOpIndex>(op))] = json{{"assign", a},
                                                                       {"batch", b},
                                                                       {"submitted_bytes", sb},
                                                                       {"completed_bytes", cb},
                                                                       {"failed_bytes", fb},
                                                                       {"pending", p},
                                                                       {"errors", e},
                                                                       {"post_batch", pb},
                                                                       {"post_wr", pw},
                                                                       {"post_bytes", pby},
                                                                       {"post_failures", pf}};
            }
        }

        json nic_obj;
        nic_obj["nic"]                   = std::string(nic.device_name);
        nic_obj["nic_bdf"]               = "";  // v0: not populated
        nic_obj["assign_total"]          = n_assign;
        nic_obj["batch_total"]           = n_batch;
        nic_obj["submitted_bytes_total"] = n_sub_bytes;
        nic_obj["completed_bytes_total"] = n_comp_bytes;
        nic_obj["failed_bytes_total"]    = n_fail_bytes;
        nic_obj["pending_ops"]           = n_pending;
        nic_obj["error_total"]           = n_errors;
        nic_obj["post_batch_total"]      = n_post_batch;
        nic_obj["post_wr_total"]         = n_post_wr;
        nic_obj["post_bytes_total"]      = n_post_bytes;
        nic_obj["post_failures_total"]   = n_post_fail;
        nic_obj["cq_errors_total"]       = nic.cq_errors_total.load(std::memory_order_relaxed);
        if (!by_op.empty()) {
            nic_obj["by_op"] = by_op;
        }

        nics_arr.push_back(nic_obj);
    }

    json result;
    result["enabled"] = true;
    result["summary"] = summary;
    result["nics"]    = nics_arr;
    return result;
}

// ============================================================
// obs_reset_for_test(): zero all counters
// ============================================================

void obs_reset_for_test()
{
    g_peer.assign_total.store(0, std::memory_order_relaxed);
    g_peer.batch_total.store(0, std::memory_order_relaxed);
    g_peer.submitted_bytes_total.store(0, std::memory_order_relaxed);
    g_peer.completed_bytes_total.store(0, std::memory_order_relaxed);
    g_peer.failed_bytes_total.store(0, std::memory_order_relaxed);
    g_peer.pending_ops.store(0, std::memory_order_relaxed);
    g_peer.error_total.store(0, std::memory_order_relaxed);
    g_peer.user_mr_count.store(0, std::memory_order_relaxed);
    g_peer.user_mr_bytes.store(0, std::memory_order_relaxed);
    g_peer.sys_mr_count.store(0, std::memory_order_relaxed);
    g_peer.sys_mr_bytes.store(0, std::memory_order_relaxed);
    for (int op = 0; op < OBS_OP_COUNT; ++op) {
        g_peer.pending_by_op[op].store(0, std::memory_order_relaxed);
    }

    int count = g_nic_count.load(std::memory_order_acquire);
    for (int i = 0; i < count && i < OBS_MAX_NICS; ++i) {
        auto& nic = g_nics[i];
        for (int op = 0; op < OBS_OP_COUNT; ++op) {
            nic.assign_total[op].store(0, std::memory_order_relaxed);
            nic.batch_total[op].store(0, std::memory_order_relaxed);
            nic.submitted_bytes_total[op].store(0, std::memory_order_relaxed);
            nic.completed_bytes_total[op].store(0, std::memory_order_relaxed);
            nic.failed_bytes_total[op].store(0, std::memory_order_relaxed);
            nic.pending_ops[op].store(0, std::memory_order_relaxed);
            nic.error_total[op].store(0, std::memory_order_relaxed);
            nic.post_batch_total[op].store(0, std::memory_order_relaxed);
            nic.post_wr_total[op].store(0, std::memory_order_relaxed);
            nic.post_bytes_total[op].store(0, std::memory_order_relaxed);
            nic.post_failures_total[op].store(0, std::memory_order_relaxed);
        }
        nic.cq_errors_total.store(0, std::memory_order_relaxed);
    }
}

}  // namespace obs
}  // namespace dlslime
