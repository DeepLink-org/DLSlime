#include "tcp_memory_pool.h"

#include "dlslime/csrc/logging.h"

namespace dlslime {
namespace tcp {

// ── local MR ────────────────────────────────────────────

int32_t TcpMemoryPool::register_memory_region(uintptr_t addr, size_t length, const std::string& name)
{

    if (name.empty()) {
        SLIME_LOG_WARN("TcpMemoryPool: empty name rejected");
        return -1;
    }
    if (name_to_handle_.find(name) != name_to_handle_.end()) {
        SLIME_LOG_WARN("TcpMemoryPool: duplicate name '", name, "' rejected");
        return -1;
    }

    auto pit = ptr_to_handle_.find(addr);
    if (pit != ptr_to_handle_.end()) {
        int32_t h = pit->second;
        if (h >= 0 && static_cast<size_t>(h) < handle_to_mr_.size() && handle_to_mr_[h].addr == addr
            && handle_to_mr_[h].length >= length) {
            name_to_handle_[name] = h;
            return h;
        }
    }

    int32_t h = static_cast<int32_t>(handle_to_mr_.size());
    handle_to_mr_.push_back({addr, length});
    ptr_to_handle_[addr]  = h;
    name_to_handle_[name] = h;
    return h;
}

int32_t TcpMemoryPool::unregister_memory_region(int32_t handle)
{
    if (handle < 0 || static_cast<size_t>(handle) >= handle_to_mr_.size())
        return -1;

    auto& mr = handle_to_mr_[handle];
    ptr_to_handle_.erase(mr.addr);

    // Remove all name→handle entries pointing to this handle.
    for (auto it = name_to_handle_.begin(); it != name_to_handle_.end();) {
        if (it->second == handle)
            it = name_to_handle_.erase(it);
        else
            ++it;
    }

    mr = {};
    return 0;
}

// ── remote MR ───────────────────────────────────────────

int32_t TcpMemoryPool::register_remote_memory_region(const json& mr_info, std::optional<std::string> name)
{

    std::string mr_name = name.value_or(mr_info.value("name", ""));

    if (!mr_name.empty()) {
        auto it = remote_name_to_handle_.find(mr_name);
        if (it != remote_name_to_handle_.end()) {
            int32_t h  = it->second;
            auto&   rm = remote_handle_to_mr_[h];
            rm.addr    = mr_info.value("addr", 0UL);
            rm.length  = mr_info.value("length", 0UL);
            return h;
        }
    }

    int32_t h = static_cast<int32_t>(remote_handle_to_mr_.size());
    remote_handle_to_mr_.push_back({mr_info.value("addr", 0UL), mr_info.value("length", 0UL)});
    remote_handle_to_name_.push_back(mr_name);
    if (!mr_name.empty())
        remote_name_to_handle_[mr_name] = h;
    return h;
}

int32_t TcpMemoryPool::unregister_remote_memory_region(int32_t handle)
{
    if (handle < 0 || static_cast<size_t>(handle) >= remote_handle_to_mr_.size())
        return -1;
    auto& s = remote_handle_to_name_[handle];
    if (!s.empty())
        remote_name_to_handle_.erase(s);
    remote_handle_to_mr_[handle] = {};
    s.clear();
    return 0;
}

// ── fast lookup ─────────────────────────────────────────

TcpMr TcpMemoryPool::get_mr_fast(int32_t handle) const
{
    if (handle < 0 || static_cast<size_t>(handle) >= handle_to_mr_.size())
        return {};
    return handle_to_mr_[handle];
}

TcpMr TcpMemoryPool::get_remote_mr_fast(int32_t handle) const
{
    if (handle < 0 || static_cast<size_t>(handle) >= remote_handle_to_mr_.size())
        return {};
    return remote_handle_to_mr_[handle];
}

int32_t TcpMemoryPool::get_mr_handle(const std::string& name) const
{
    auto it = name_to_handle_.find(name);
    return it != name_to_handle_.end() ? it->second : -1;
}

int32_t TcpMemoryPool::get_remote_mr_handle(const std::string& name) const
{
    auto it = remote_name_to_handle_.find(name);
    return it != remote_name_to_handle_.end() ? it->second : -1;
}

// ── serialization ───────────────────────────────────────

json TcpMemoryPool::mr_info() const
{
    json j = json::object();
    for (const auto& [name, h] : name_to_handle_)
        if (h >= 0 && static_cast<size_t>(h) < handle_to_mr_.size() && handle_to_mr_[h].length > 0)
            j[name] = handle_to_mr_[h].json_info(name);
    return j;
}

json TcpMemoryPool::remote_mr_info() const
{
    json j = json::object();
    for (const auto& [name, h] : remote_name_to_handle_)
        if (h >= 0 && static_cast<size_t>(h) < remote_handle_to_mr_.size() && remote_handle_to_mr_[h].length > 0)
            j[name] = remote_handle_to_mr_[h].json_info(name);
    return j;
}

}  // namespace tcp
}  // namespace dlslime
