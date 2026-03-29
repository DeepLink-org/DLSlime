#include "ascend_remote_memory_pool.h"

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

#include "dlslime/csrc/logging.h"

namespace dlslime {

int AscendRemoteMemoryPool::register_remote_memory_region(uint64_t mr_key, const json& mr_info)
{
    // Validate JSON contains required fields
    if (!mr_info.contains("addr") || !mr_info.contains("offset") || !mr_info.contains("length")) {
        SLIME_LOG_ERROR("Invalid remote mr_info JSON: missing required fields (addr, offset, length)");
        return -1;
    }

    // Check if already registered
    if (remote_mrs_.count(mr_key)) {
        SLIME_LOG_WARN("Remote memory region key ",
                       mr_key,
                       " already registered in remote pool, overwriting");
    }

    // Create remote memory region descriptor from JSON
    ascend_remote_mr_t remote_mr = ascend_remote_mr_t::from_json(mr_info);
    remote_mr.mr_key             = mr_key;  // Ensure key matches

    remote_mrs_[mr_key] = remote_mr;

    SLIME_LOG_INFO("Registered REMOTE memory region: key=",
                   mr_key,
                   " addr=0x",
                   std::hex,
                   remote_mr.addr,
                   std::dec,
                   " offset=",
                   remote_mr.offset,
                   " length=",
                   remote_mr.length);

    return 0;
}

int AscendRemoteMemoryPool::unregister_remote_memory_region(uint64_t mr_key)
{
    auto it = remote_mrs_.find(mr_key);
    if (it == remote_mrs_.end()) {
        SLIME_LOG_WARN("Attempted to unregister non-existent REMOTE memory region key: ", mr_key);
        return -1;
    }

    remote_mrs_.erase(it);
    SLIME_LOG_INFO("Unregistered REMOTE memory region: key=", mr_key);
    return 0;
}

const json AscendRemoteMemoryPool::remote_mr_info() const
{
    json all_remote_mr_info;
    for (const auto& [key, mr] : remote_mrs_) {
        all_remote_mr_info[std::to_string(key)] = mr.json_info();
    }
    return all_remote_mr_info;
}

}  // namespace dlslime
