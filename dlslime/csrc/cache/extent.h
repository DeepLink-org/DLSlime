// extent.h - Wire-level types for DLSlimeCache.
//
// See docs/design/dlslime-cache.md. The cache server is tensor-agnostic:
// every value is just a list of Extents, each naming a byte range of some
// peer's registered memory region. A Manifest is what `store` returns and
// `load` accepts — enough information for the holder to reconstruct the
// value into their own tensor via RDMA scatter.
//
// This header is deliberately header-only and STL-only so it can be
// shared between the server, the client, and the Python bindings without
// dragging in ibverbs or RDMA context.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "dlslime/csrc/common/json.hpp"

namespace dlslime::cache {

using json = nlohmann::json;

// A byte slice of some peer's registered memory.
//
// The peer name references a PeerAgent alias; both the cache server and
// the cache client look it up through their own PeerAgent to resolve it
// to a live RDMA endpoint. Extents never encode raw addresses — just the
// (peer, mr_handle, offset, length) triple that is stable across the
// cache's lifetime. Byte offsets are pre-computed client-side using the
// shared stride math (see nanodeploy/context/cache.py for the prior art
// in the KV-cache workload).
struct Extent {
    std::string peer;          // PeerAgent alias owning the MR
    uint64_t    mr_handle{0};  // Remote-side MR handle (stable per peer/pool)
    uint64_t    offset{0};     // Byte offset within the MR
    uint64_t    length{0};     // Byte length of this slice

    json to_json() const
    {
        return json{{"peer", peer}, {"mr_handle", mr_handle}, {"offset", offset}, {"length", length}};
    }

    static Extent from_json(const json& j)
    {
        return Extent{j.at("peer").get<std::string>(),
                      j.at("mr_handle").get<uint64_t>(),
                      j.at("offset").get<uint64_t>(),
                      j.at("length").get<uint64_t>()};
    }
};

enum class CacheMode : uint8_t {
    // Cache server allocated its own MR slice, RDMA-read the source's
    // bytes into it, and owns a durable (until evicted) copy. Extents in
    // the manifest point at the cache server's MR.
    Deep = 0,

    // Cache server recorded extents as-is; no bytes were copied. Extents
    // in the manifest point at whichever peer held the bytes at store
    // time. Shallow keys go stale if that peer's memory is recycled.
    Shallow = 1,
};

inline const char* to_cstr(CacheMode m)
{
    return m == CacheMode::Deep ? "deep" : "shallow";
}

// What `store` returns and `load` emits. The client uses this to
// schedule its own RDMA reads; the cache server never moves bytes on
// `load`.
//
// Versioning: `version` is bumped on every `store(key, ...)` call for
// the same key. Lets the client detect "the manifest I cached is now
// stale" without polling — compare versions, refetch on mismatch.
struct Manifest {
    CacheMode           mode{CacheMode::Deep};
    std::vector<Extent> extents;
    uint64_t            version{0};

    uint64_t total_bytes() const
    {
        uint64_t n = 0;
        for (const auto& e : extents)
            n += e.length;
        return n;
    }

    json to_json() const
    {
        json exs = json::array();
        for (const auto& e : extents)
            exs.push_back(e.to_json());
        return json{{"mode", to_cstr(mode)}, {"extents", exs}, {"version", version}};
    }

    static Manifest from_json(const json& j)
    {
        Manifest m;
        m.mode    = (j.at("mode").get<std::string>() == "shallow") ? CacheMode::Shallow : CacheMode::Deep;
        m.version = j.at("version").get<uint64_t>();
        for (const auto& ej : j.at("extents"))
            m.extents.push_back(Extent::from_json(ej));
        return m;
    }
};

}  // namespace dlslime::cache
