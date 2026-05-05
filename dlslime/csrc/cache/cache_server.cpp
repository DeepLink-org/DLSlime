#include "cache_server.h"

#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <utility>

namespace dlslime::cache {

CacheServer::CacheServer(uint64_t slab_size, uint64_t memory_size): slab_size_(slab_size), memory_size_(memory_size)
{
    if (slab_size_ == 0)
        throw std::invalid_argument("CacheServer: slab_size must be > 0");
    if (memory_size_ > 0 && memory_size_ % slab_size_ != 0)
        throw std::invalid_argument("CacheServer: memory_size must be 0 or a multiple of slab_size");
    num_slabs_ = memory_size_ == 0 ? 0 : memory_size_ / slab_size_;
}

Manifest CacheServer::store(const std::string& key, std::vector<Extent> extents, CacheMode mode)
{
    if (mode == CacheMode::Deep) {
        throw std::runtime_error("CacheServer::store: mode=deep not implemented in metadata V0; "
                                 "store assignment batches instead.");
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

AssignmentManifest CacheServer::store_assignments(const std::string&       peer_agent_id,
                                                  dlslime::AssignmentBatch assignments)
{
    if (peer_agent_id.empty())
        throw std::invalid_argument("CacheServer::store_assignments: peer_agent_id must not be empty");

    dlslime::AssignmentBatch slabbed;
    dlslime::split_assign_by_max_length(dlslime::OpCode::READ, assignments, slabbed, slab_size_);

    std::unique_lock lk(mu_);
    if (num_slabs_ > 0) {
        uint64_t used_slabs = 0;
        for (const auto& [peer, versions] : assignment_kv_) {
            for (const auto& [version, m] : versions)
                used_slabs += m.assignments.size();
        }
        if (used_slabs + slabbed.size() > num_slabs_) {
            throw std::runtime_error("CacheServer::store_assignments: not enough preallocated cache slabs");
        }
    }

    AssignmentManifest m;
    m.peer_agent_id                          = peer_agent_id;
    m.assignments                            = std::move(slabbed);
    m.version                                = next_assignment_version_++;
    assignment_kv_[peer_agent_id][m.version] = m;
    return m;
}

std::optional<AssignmentManifest> CacheServer::query_assignments(const std::string& peer_agent_id,
                                                                 uint64_t           version) const
{
    std::shared_lock lk(mu_);
    auto             peer_it = assignment_kv_.find(peer_agent_id);
    if (peer_it == assignment_kv_.end())
        return std::nullopt;
    auto version_it = peer_it->second.find(version);
    if (version_it == peer_it->second.end())
        return std::nullopt;
    return version_it->second;
}

bool CacheServer::erase_assignments(const std::string& peer_agent_id, uint64_t version)
{
    std::unique_lock lk(mu_);
    auto             peer_it = assignment_kv_.find(peer_agent_id);
    if (peer_it == assignment_kv_.end())
        return false;
    const bool erased = peer_it->second.erase(version) > 0;
    if (peer_it->second.empty())
        assignment_kv_.erase(peer_it);
    return erased;
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
    s.slab_size            = slab_size_;
    s.memory_size          = memory_size_;
    s.num_slabs            = num_slabs_;
    s.num_assignment_peers = assignment_kv_.size();
    for (const auto& [peer, versions] : assignment_kv_) {
        s.num_assignment_entries += versions.size();
        for (const auto& [version, m] : versions) {
            s.num_assignments += m.assignments.size();
            s.assignment_bytes += m.total_bytes();
        }
    }
    s.used_slabs = s.num_assignments;
    s.free_slabs = num_slabs_ == 0 ? 0 : num_slabs_ - s.used_slabs;
    return s;
}

void CacheServer::clear()
{
    std::unique_lock lk(mu_);
    kv_.clear();
    assignment_kv_.clear();
    next_version_            = 1;
    next_assignment_version_ = 1;
}

}  // namespace dlslime::cache
