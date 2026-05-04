// cache_server.h - In-process cache server state.
//
// V0 scope (this file): shallow mode only.
//   * store(key, extents)         : record key -> extents as-is
//   * load(key)                   : return the stored manifest
//   * delete(key)                 : drop the mapping
//   * stats()                     : # keys, # extents, bytes addressed
//
// V0 has no allocator, no LRU, no pin counts, no RDMA — shallow mode is
// pure metadata. V1 will add deep mode with an MR-backed page pool and
// LRU eviction, on top of this same interface.
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

#include "extent.h"

namespace dlslime::cache {

struct CacheStats {
    uint64_t num_keys{0};
    uint64_t num_shallow_keys{0};
    uint64_t num_deep_keys{0};  // reserved for V1
    uint64_t num_extents{0};
    uint64_t bytes_addressed{0};  // sum of extent lengths across all manifests
};

class CacheServer {
public:
    CacheServer() = default;

    // Record a mapping from `key` to the supplied extents.
    //
    // V0: `mode` must be CacheMode::Shallow; Deep is rejected with a
    // runtime_error. V1 lifts this restriction and wires in the page
    // allocator.
    //
    // Bumps the per-key version monotonically. Returns the resulting
    // manifest (same extents, same mode, new version) for callers that
    // want to advertise it downstream without a follow-up load.
    Manifest store(const std::string& key, std::vector<Extent> extents, CacheMode mode = CacheMode::Shallow);

    // Fetch the manifest for `key`, or nullopt on miss.
    std::optional<Manifest> load(const std::string& key) const;

    // Drop the mapping. Returns true if the key existed, false on miss.
    bool erase(const std::string& key);

    CacheStats stats() const;

    // Test-only helper: drop every key. Never call this in production.
    void clear();

private:
    mutable std::shared_mutex                 mu_;
    std::unordered_map<std::string, Manifest> kv_;
    uint64_t                                  next_version_{1};  // monotonic
};

}  // namespace dlslime::cache
