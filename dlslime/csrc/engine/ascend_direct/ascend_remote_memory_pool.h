#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "dlslime/csrc/common/json.hpp"

namespace dlslime {
using json = nlohmann::json;

/**
 * @brief Remote memory region descriptor for Ascend Direct transfers
 *
 * Encapsulates metadata about a REMOTE memory region on a peer Ascend NPU.
 * Note: Does NOT contain AdxlEngine handle - remote handles are managed
 * by the remote peer's AdxlEngine instance.
 */
struct ascend_remote_mr_t {
    uint64_t  mr_key;  // Memory region key (for user reference)
    uintptr_t addr;    // Base address of remote memory region
    uint64_t  offset;  // Offset within the region
    size_t    length;  // Length of the memory region in bytes

    /**
     * @brief Serialize memory region info to JSON
     * @return JSON object with mr_key, addr, offset, length
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

    /**
     * @brief Construct from JSON (received from remote peer)
     * @param mr_info JSON object with memory region metadata
     */
    static ascend_remote_mr_t from_json(const json& mr_info)
    {
        ascend_remote_mr_t mr;
        mr.mr_key = mr_info.value("mr_key", 0UL);
        mr.addr   = mr_info.value("addr", 0UL);
        mr.offset = mr_info.value("offset", 0UL);
        mr.length = mr_info.value("length", 0UL);
        return mr;
    }
};

/**
 * @brief Remote memory pool for Ascend Direct endpoint
 *
 * Manages REMOTE memory region registrations from ONE peer NPU.
 * Similar to RDMARemoteMemoryPool - one instance per remote peer
 * to provide namespace isolation.
 *
 * Design rationale:
 * - Separate pool per peer to avoid mr_key collision
 * - Stores only metadata (no handles - remote manages its own)
 * - Each endpoint has its own remote pool
 *
 * Example:
 *   NPU0 -> NPU1: remote_pool_1 (NPU1's memory)
 *   NPU0 -> NPU2: remote_pool_2 (NPU2's memory)
 */
class AscendRemoteMemoryPool {
public:
    AscendRemoteMemoryPool()  = default;
    ~AscendRemoteMemoryPool() = default;

    /**
     * @brief Register a remote memory region (metadata only)
     * @param mr_key Remote memory region key
     * @param mr_info JSON metadata from remote peer's endpoint_info()
     * @return 0 on success, -1 on failure
     *
     * Note: This only stores metadata. AdxlEngine manages remote access.
     */
    int register_remote_memory_region(uint64_t mr_key, const json& mr_info);

    /**
     * @brief Unregister a remote memory region
     * @param mr_key Remote memory region key
     * @return 0 on success, -1 on failure
     */
    int unregister_remote_memory_region(uint64_t mr_key);

    /**
     * @brief Get remote memory region descriptor
     * @param mr_key Remote memory region key
     * @return Remote memory region descriptor (empty if not found)
     */
    inline ascend_remote_mr_t get_remote_mr(uint64_t mr_key) const
    {
        auto it = remote_mrs_.find(mr_key);
        if (it != remote_mrs_.end()) {
            return it->second;
        }
        // Return empty descriptor if not found
        return ascend_remote_mr_t{};
    }

    /**
     * @brief Check if a remote memory region is registered
     * @param mr_key Remote memory region key
     * @return true if registered, false otherwise
     */
    inline bool has_remote_mr(uint64_t mr_key) const
    {
        return remote_mrs_.find(mr_key) != remote_mrs_.end();
    }

    /**
     * @brief Get JSON info for all remote memory regions
     * @return JSON object mapping mr_key → mr_info
     */
    const json remote_mr_info() const;

    /**
     * @brief Get number of registered remote memory regions
     * @return Count of registered regions
     */
    inline size_t size() const
    {
        return remote_mrs_.size();
    }

private:
    // Remote memory regions: mr_key → ascend_remote_mr_t (metadata only)
    std::unordered_map<uint64_t, ascend_remote_mr_t> remote_mrs_;
};

}  // namespace dlslime
