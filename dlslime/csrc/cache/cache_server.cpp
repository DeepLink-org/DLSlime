#include "cache_server.h"

#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <utility>

namespace dlslime::cache {

Manifest CacheServer::store(const std::string& key, std::vector<Extent> extents, CacheMode mode)
{
    if (mode == CacheMode::Deep) {
        // V0: deep mode is not yet implemented. A deep store would need
        // the page allocator + RDMA ingestion path. Reject cleanly so
        // client-side callers can fall back or retry without confusion.
        throw std::runtime_error("CacheServer::store: mode=deep not implemented in V0; "
                                 "use mode=shallow, or wait for V1.");
    }

    std::unique_lock lk(mu_);
    Manifest         m;
    m.mode    = mode;
    m.extents = std::move(extents);
    m.version = next_version_++;
    kv_[key]  = m;
    return m;
}

std::optional<Manifest> CacheServer::load(const std::string& key) const
{
    std::shared_lock lk(mu_);
    auto             it = kv_.find(key);
    if (it == kv_.end())
        return std::nullopt;
    return it->second;
}

bool CacheServer::erase(const std::string& key)
{
    std::unique_lock lk(mu_);
    return kv_.erase(key) > 0;
}

CacheStats CacheServer::stats() const
{
    std::shared_lock lk(mu_);
    CacheStats       s;
    s.num_keys = kv_.size();
    for (const auto& [k, m] : kv_) {
        if (m.mode == CacheMode::Shallow)
            ++s.num_shallow_keys;
        else
            ++s.num_deep_keys;
        s.num_extents += m.extents.size();
        s.bytes_addressed += m.total_bytes();
    }
    return s;
}

void CacheServer::clear()
{
    std::unique_lock lk(mu_);
    kv_.clear();
    next_version_ = 1;
}

}  // namespace dlslime::cache
