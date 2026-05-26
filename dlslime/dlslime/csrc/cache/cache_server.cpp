#include "cache_server.h"

#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <utility>

namespace dlslime::cache {

CacheServer::CacheServer(uint64_t slab_size, uint64_t memory_size): slab_size_(slab_size), memory_size_(memory_size)
{
    if (slab_size_ < kMinSlabSize || slab_size_ > kMaxSlabSize)
        throw std::invalid_argument("CacheServer: slab_size must be in [128K, 1G]");
    if (memory_size_ > 0 && memory_size_ % slab_size_ != 0)
        throw std::invalid_argument("CacheServer: memory_size must be 0 or a multiple of slab_size");
    num_slabs_ = memory_size_ == 0 ? 0 : memory_size_ / slab_size_;
    free_slabs_.reserve(num_slabs_);
    for (uint64_t slab_id = num_slabs_; slab_id > 0; --slab_id)
        free_slabs_.push_back(slab_id - 1);
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
        if (slabbed.size() > free_slabs_.size()) {
            throw std::runtime_error("CacheServer::store_assignments: not enough preallocated cache slabs");
        }
    }

    AssignmentManifest m;
    m.peer_agent_id = peer_agent_id;
    if (num_slabs_ > 0) {
        m.slab_ids.reserve(slabbed.size());
        for (auto& assign : slabbed) {
            uint64_t slab_id = free_slabs_.back();
            free_slabs_.pop_back();
            m.slab_ids.push_back(slab_id);
            assign.source_offset = slab_id * slab_size_;
        }
    }
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
    auto version_it = peer_it->second.find(version);
    if (version_it == peer_it->second.end())
        return false;
    for (uint64_t slab_id : version_it->second.slab_ids)
        free_slabs_.push_back(slab_id);
    peer_it->second.erase(version_it);
    const bool erased = true;
    if (peer_it->second.empty())
        assignment_kv_.erase(peer_it);
    return erased;
}

CacheStats CacheServer::stats() const
{
    std::shared_lock lk(mu_);
    CacheStats       s;
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
    s.free_slabs = num_slabs_ == 0 ? 0 : free_slabs_.size();
    s.used_slabs = num_slabs_ == 0 ? s.num_assignments : num_slabs_ - s.free_slabs;
    return s;
}

void CacheServer::clear()
{
    std::unique_lock lk(mu_);
    assignment_kv_.clear();
    next_assignment_version_ = 1;
    free_slabs_.clear();
    free_slabs_.reserve(num_slabs_);
    for (uint64_t slab_id = num_slabs_; slab_id > 0; --slab_id)
        free_slabs_.push_back(slab_id - 1);
}

}  // namespace dlslime::cache
