#include "ascend_local_memory_pool.h"

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

#include "dlslime/csrc/logging.h"

namespace dlslime {

int AscendLocalMemoryPool::register_memory_region(uint64_t        mr_key,
                                                   uintptr_t       addr,
                                                   uint64_t        offset,
                                                   size_t          length,
                                                   adxl::MemHandle handle)
{
    // Check if already registered
    if (mrs_.count(mr_key)) {
        SLIME_LOG_WARN("Local memory region key ",
                       mr_key,
                       " already registered in local pool, overwriting");
    }

    // Store memory region descriptor
    ascend_local_mr_t mr;
    mr.mr_key = mr_key;
    mr.addr   = addr;
    mr.offset = offset;
    mr.length = length;
    mr.handle = handle;

    mrs_[mr_key] = mr;

    SLIME_LOG_INFO("Registered LOCAL memory region: key=",
                   mr_key,
                   " addr=0x",
                   std::hex,
                   addr,
                   std::dec,
                   " offset=",
                   offset,
                   " length=",
                   length);

    return 0;
}

int AscendLocalMemoryPool::unregister_memory_region(uint64_t mr_key)
{
    auto it = mrs_.find(mr_key);
    if (it == mrs_.end()) {
        SLIME_LOG_WARN("Attempted to unregister non-existent LOCAL memory region key: ", mr_key);
        return -1;
    }

    mrs_.erase(it);
    SLIME_LOG_INFO("Unregistered LOCAL memory region: key=", mr_key);
    return 0;
}

const json AscendLocalMemoryPool::mr_info() const
{
    json all_mr_info;
    for (const auto& [key, mr] : mrs_) {
        all_mr_info[std::to_string(key)] = mr.json_info();
    }
    return all_mr_info;
}

}  // namespace dlslime
