#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "adxl/adxl_types.h"
#include "dlslime/csrc/common/json.hpp"

namespace dlslime {
using json = nlohmann::json;

/**
 * @brief Local memory region descriptor for Ascend Direct transfers
 *
 * Encapsulates metadata about a registered LOCAL memory region on Ascend NPU.
 */
struct ascend_local_mr_t {
    uint64_t        mr_key;  // Memory region key (for user reference)
    uintptr_t       addr;    // Base address of memory region
    uint64_t        offset;  // Offset within the region
    size_t          length;  // Length of the memory region in bytes
    adxl::MemHandle handle;  // AdxlEngine memory handle

    /**
     * @brief Serialize memory region info to JSON for remote exchange
     * @return JSON object with mr_key, addr, offset, length (no handle)
     */
    const json json_info() const
    {
        json mr_info;
        mr_info["mr_key"] = mr_key;
        mr_info["addr"]   = addr;
        mr_info["offset"] = offset;
        mr_info["length"] = length;
        return mr_info;
    }
};

/**
 * @brief Local memory pool for Ascend Direct endpoint
 *
 * Manages LOCAL memory region registrations on this NPU.
 * Similar to RDMAMemoryPool - handles only local memory, can be shared
 * across multiple endpoints for memory efficiency.
 *
 * Design rationale:
 * - Separate from remote pool to avoid namespace collision
 * - Can be shared via shared_ptr across multiple endpoints
 * - Encapsulates AdxlEngine handle management
 */
class AscendLocalMemoryPool {
public:
    AscendLocalMemoryPool()  = default;
    ~AscendLocalMemoryPool() = default;

    /**
     * @brief Register a local memory region
     * @param mr_key Memory region key for identification
     * @param addr Base address of the memory region
     * @param offset Offset within the memory region
     * @param length Size of the memory region in bytes
     * @param handle AdxlEngine memory handle (obtained from RegisterMem)
     * @return 0 on success, -1 on failure
     */
    int register_memory_region(uint64_t mr_key, uintptr_t addr, uint64_t offset, size_t length, adxl::MemHandle handle);

    /**
     * @brief Unregister a local memory region
     * @param mr_key Memory region key to unregister
     * @return 0 on success, -1 on failure
     */
    int unregister_memory_region(uint64_t mr_key);

    /**
     * @brief Get local memory region descriptor
     * @param mr_key Memory region key
     * @return Memory region descriptor (empty if not found)
     */
    inline ascend_local_mr_t get_mr(uint64_t mr_key) const
    {
        auto it = mrs_.find(mr_key);
        if (it != mrs_.end()) {
            return it->second;
        }
        // Return empty descriptor if not found
        return ascend_local_mr_t{};
    }

    /**
     * @brief Check if a memory region is registered
     * @param mr_key Memory region key
     * @return true if registered, false otherwise
     */
    inline bool has_mr(uint64_t mr_key) const
    {
        return mrs_.find(mr_key) != mrs_.end();
    }

    /**
     * @brief Get AdxlEngine handle for a memory region
     * @param mr_key Memory region key
     * @return AdxlEngine handle, or empty handle if not found
     */
    inline adxl::MemHandle get_handle(uint64_t mr_key) const
    {
        auto it = mrs_.find(mr_key);
        if (it != mrs_.end()) {
            return it->second.handle;
        }
        return adxl::MemHandle{};
    }

    /**
     * @brief Get JSON info for all local memory regions
     * @return JSON object mapping mr_key → mr_info
     *
     * Used for endpoint information exchange with remote peers
     */
    const json mr_info() const;

    /**
     * @brief Get number of registered memory regions
     * @return Count of registered regions
     */
    inline size_t size() const
    {
        return mrs_.size();
    }

private:
    // Local memory regions: mr_key → ascend_local_mr_t
    std::unordered_map<uint64_t, ascend_local_mr_t> mrs_;
};

}  // namespace dlslime
