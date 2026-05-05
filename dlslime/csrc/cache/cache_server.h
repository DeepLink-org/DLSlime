// cache_server.h - In-process cache server state.
//
// V0 scope (this file): shallow mode plus assignment-directory metadata.
//   * store(key, extents)         : record key -> extents as-is
//   * store_assignments(peer, b)  : record peer_agent_id/version -> AssignmentBatch
//   * load(key)                   : return the stored manifest
//   * query_assignments(peer, v)  : return the stored AssignmentBatch manifest
//   * delete(key)                 : drop the mapping
//   * stats()                     : # keys, # extents, bytes addressed
//
// The assignment-directory path is intentionally simple: record the
// original Engine PeerAgent id and a generated version, then hand back the
// exact AssignmentBatch needed for the consumer to load through the existing
// DLSlime endpoint. No page allocator and no extra data-plane abstraction.
//
// Thread-safety: the server is designed to be driven by one RPC
// listener thread today, but the hot-path state (kv_) is protected by a
// shared_mutex to allow many concurrent loads. Store/delete take the
// write lock.
//
// Concurrency note for V1: when deep mode lands we'll need extent pin
// counts so delete/evict cannot race with in-flight RDMA reads issued
// by a client against returned extents. For shallow mode this is not a
// concern because the cache holds no bytes; if the source peer's MR
// gets recycled while a client still has the manifest, the client's
// RDMA read is what fails, not the cache.
#pragma once

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "dlslime/csrc/engine/assignment.h"
#include "extent.h"

namespace dlslime::cache {

struct AssignmentManifest {
    std::string              peer_agent_id;
    dlslime::AssignmentBatch assignments;
    uint64_t                 version{0};

    uint64_t total_bytes() const
    {
        uint64_t n = 0;
        for (const auto& a : assignments)
            n += a.length;
        return n;
    }
};

struct CacheStats {
    uint64_t num_keys{0};
    uint64_t num_shallow_keys{0};
    uint64_t num_deep_keys{0};
    uint64_t num_extents{0};
    uint64_t bytes_addressed{0};  // sum of extent lengths across all manifests
    uint64_t num_assignment_peers{0};
    uint64_t num_assignment_entries{0};
    uint64_t num_assignments{0};
    uint64_t assignment_bytes{0};
    uint64_t slab_size{0};
    uint64_t memory_size{0};
    uint64_t num_slabs{0};
    uint64_t used_slabs{0};
    uint64_t free_slabs{0};
};

class CacheServer {
public:
    static constexpr uint64_t kDefaultSlabSize   = 256 * 1024;
    static constexpr uint64_t kDefaultMemorySize = 0;

    explicit CacheServer(uint64_t slab_size = kDefaultSlabSize, uint64_t memory_size = kDefaultMemorySize);

    // Record a mapping from `key` to the supplied extents.
    //
    // V0: `mode` must be CacheMode::Shallow; Deep is rejected with a
    // runtime_error. Deep copying is deliberately left out of this path;
    // the fast route is storing ready-to-run AssignmentBatch manifests.
    //
    // Bumps the per-key version monotonically. Returns the resulting
    // manifest (same extents, same mode, new version) for callers that
    // want to advertise it downstream without a follow-up load.
    Manifest store(const std::string& key, std::vector<Extent> extents, CacheMode mode = CacheMode::Shallow);

    // Fetch the manifest for `key`, or nullopt on miss.
    std::optional<Manifest> load(const std::string& key) const;

    uint64_t slab_size() const
    {
        return slab_size_;
    }
    uint64_t memory_size() const
    {
        return memory_size_;
    }
    uint64_t num_slabs() const
    {
        return num_slabs_;
    }

    // Record ready-to-run assignments for one Engine / PeerAgent owner.
    // Returns a generated version; clients query by (peer_agent_id, version)
    // and then feed the returned batch to the existing endpoint read path.
    // Store-time normalization splits large assignments into slab-sized
    // chunks so query() always returns a batch ready for direct I/O.
    AssignmentManifest store_assignments(const std::string& peer_agent_id, dlslime::AssignmentBatch assignments);

    std::optional<AssignmentManifest> query_assignments(const std::string& peer_agent_id, uint64_t version) const;

    bool erase_assignments(const std::string& peer_agent_id, uint64_t version);

    // Drop the mapping. Returns true if the key existed, false on miss.
    bool erase(const std::string& key);

    CacheStats stats() const;

    // Test-only helper: drop every key. Never call this in production.
    void clear();

private:
    mutable std::shared_mutex                                                         mu_;
    std::unordered_map<std::string, Manifest>                                         kv_;
    std::unordered_map<std::string, std::unordered_map<uint64_t, AssignmentManifest>> assignment_kv_;
    uint64_t                                                                          next_version_{1};
    uint64_t                                                                          next_assignment_version_{1};
    uint64_t                                                                          slab_size_{kDefaultSlabSize};
    uint64_t                                                                          memory_size_{kDefaultMemorySize};
    uint64_t                                                                          num_slabs_{0};
};

}  // namespace dlslime::cache
