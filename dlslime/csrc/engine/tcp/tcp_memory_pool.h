#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "dlslime/csrc/common/json.hpp"

namespace dlslime {
namespace tcp {

using json = nlohmann::json;

struct TcpMr {
    uintptr_t addr{0};
    uint64_t  offset{0};
    size_t    length{0};

    json json_info(const std::string& name) const {
        return {{"name", name}, {"addr", addr},
                {"offset", offset}, {"length", length}};
    }
};

// Pure-bookkeeping pool.  No hardware registration needed for TCP.
class TcpMemoryPool {
public:
    TcpMemoryPool() = default;

    int32_t register_memory_region(uintptr_t addr, uint64_t offset,
                                   size_t length,
                                   std::optional<std::string> name = std::nullopt);
    int32_t unregister_memory_region(int32_t handle);

    int32_t register_remote_memory_region(const json& mr_info,
                                          std::optional<std::string> name = std::nullopt);
    int32_t unregister_remote_memory_region(int32_t handle);

    TcpMr   get_mr_fast(int32_t handle) const;
    TcpMr   get_remote_mr_fast(int32_t handle) const;
    int32_t get_mr_handle(const std::string& name) const;
    int32_t get_remote_mr_handle(const std::string& name) const;

    json mr_info() const;
    json remote_mr_info() const;

private:
    // local MRs
    std::unordered_map<std::string, int32_t> name_to_handle_;
    std::unordered_map<uintptr_t, int32_t>   ptr_to_handle_;
    std::vector<TcpMr>                       handle_to_mr_;
    std::vector<std::string>                 handle_to_name_;

    // remote MRs
    std::unordered_map<std::string, int32_t> remote_name_to_handle_;
    std::vector<TcpMr>                       remote_handle_to_mr_;
    std::vector<std::string>                 remote_handle_to_name_;
};

}  // namespace tcp
}  // namespace dlslime
