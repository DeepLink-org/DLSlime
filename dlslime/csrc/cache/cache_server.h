// cache_server.h - In-process cache server state.
//
// Scope (this file): cache-service assignment directory.
//   * store_assignments(peer, b)  : record peer_agent_id/version -> AssignmentBatch
//   * query_assignments(peer, v)  : return the stored AssignmentBatch manifest
//   * delete_assignments(peer, v) : drop one AssignmentBatch manifest
//   * stats()                     : assignment/slab counters
//
// The assignment-directory path is what the Python cache service uses for
// real data-plane examples. The client writes bytes into the cache service's
// registered MR first, then stores the ready-to-run AssignmentBatch under the
// original Engine PeerAgent id and a generated version. Query hands back the
// exact batch needed for the consumer to read from the cache MR through the
// existing DLSlime endpoint. A fixed-size slab free list owns offsets inside
// the preallocated cache MR; no extra data-plane abstraction lives here.
//
// Thread-safety: the assignment directory is protected by a shared_mutex.
// Query/stats take the shared lock; store/delete/clear take the write lock.
//
// Concurrency note for V1: delete/evict should grow slab leases or pin counts
// so a manifest cannot be freed while a client has an in-flight RDMA read.
// Today examples delete after read_future.wait().
#pragma once

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "dlslime/csrc/engine/assignment.h"

namespace dlslime::cache {

struct AssignmentManifest {
    std::string              peer_agent_id;
    dlslime::AssignmentBatch assignments;
    std::vector<uint64_t>    slab_ids;
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
    static constexpr uint64_t kMinSlabSize       = 128 * 1024;
    static constexpr uint64_t kMaxSlabSize       = 1024ULL * 1024 * 1024;
    static constexpr uint64_t kDefaultSlabSize   = 256 * 1024;
    static constexpr uint64_t kDefaultMemorySize = 0;

    explicit CacheServer(uint64_t slab_size = kDefaultSlabSize, uint64_t memory_size = kDefaultMemorySize);

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
    // chunks, allocates slab ids, and rewrites cache-side source_offset values
    // to slab offsets so query() always returns a batch ready for direct I/O.
    AssignmentManifest store_assignments(const std::string& peer_agent_id, dlslime::AssignmentBatch assignments);

    std::optional<AssignmentManifest> query_assignments(const std::string& peer_agent_id, uint64_t version) const;

    bool erase_assignments(const std::string& peer_agent_id, uint64_t version);

    CacheStats stats() const;

    // Test-only helper: drop every key. Never call this in production.
    void clear();

private:
    mutable std::shared_mutex                                                         mu_;
    std::unordered_map<std::string, std::unordered_map<uint64_t, AssignmentManifest>> assignment_kv_;
    std::vector<uint64_t>                                                             free_slabs_;
    uint64_t                                                                          next_assignment_version_{1};
    uint64_t                                                                          slab_size_{kDefaultSlabSize};
    uint64_t                                                                          memory_size_{kDefaultMemorySize};
    uint64_t                                                                          num_slabs_{0};
};

}  // namespace dlslime::cache
