#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "adxl/adxl_engine.h"
#include "adxl/adxl_types.h"
#include "ascend_future.h"
#include "ascend_local_memory_pool.h"
#include "ascend_remote_memory_pool.h"
#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/logging.h"
#include "nanocommon/json.hpp"

namespace dlslime {

using json = nlohmann::json;

/**
 * @brief Ascend Direct Transfer Endpoint
 *
 * Provides a normalized interface aligned with RDMAEndpoint for
 * Ascend NPU-to-NPU direct transfers using AdxlEngine.
 *
 * Interface alignment:
 * - read(assign_tuple_t, stream) → AscendFuture
 * - register_memory_region(mr_key, addr, offset, length)
 * - endpoint_info() → json (for endpoint exchange)
 * - connect(remote_info) (connection establishment)
 *
 * Simplified from RDMA:
 * - Only read primitive (no write/send/recv)
 * - No complex flow control or multi-QP management
 * - AdxlEngine handles low-level details (black box)
 */
class AscendDirectEndpoint {
public:
    /**
     * @brief Default constructor (creates new pools internally)
     */
    AscendDirectEndpoint();

    /**
     * @brief Constructor with existing pools (for memory sharing)
     * @param local_pool Shared pointer to local memory pool (can be shared)
     * @param remote_pool Shared pointer to remote memory pool (per-endpoint)
     *
     * This constructor allows sharing local memory pool across multiple endpoints
     * while maintaining separate remote pools for namespace isolation.
     */
    AscendDirectEndpoint(std::shared_ptr<AscendLocalMemoryPool>  local_pool,
                         std::shared_ptr<AscendRemoteMemoryPool> remote_pool);

    ~AscendDirectEndpoint();

    /**
     * @brief Initialize the endpoint
     * @param host Local host address
     * @param port Local port number
     * @return 0 on success, -1 on failure
     *
     * Note: Initialization logic is kept as black box (AdxlEngine details)
     */
    int init(const std::string& host, int port);

    /**
     * @brief Async read operation (normalized interface)
     *
     * @param assign Vector of assignment tuples
     *        Each tuple: (mr_key, remote_mr_key, target_offset, source_offset, length)
     * @param stream_handle Optional stream handle for async execution
     * @return Shared pointer to AscendFuture for waiting on completion
     *
     * This matches RDMAEndpoint::read() signature for interface consistency
     */
    std::shared_ptr<AscendFuture> read(std::vector<assign_tuple_t>& assign, void* stream_handle = nullptr);

    /**
     * @brief Register local memory region (normalized interface)
     *
     * @param mr_key Memory region key (used for identification)
     * @param addr Base address of the memory region
     * @param offset Offset within the memory region (usually 0)
     * @param length Size of the memory region in bytes
     *
     * This matches RDMAEndpoint/NVLinkEndpoint signature
     */
    void register_memory_region(uint64_t mr_key, uintptr_t addr, size_t offset, size_t length);

    /**
     * @brief Unregister a memory region
     * @param mr_key Memory region key to unregister
     */
    void unregister_memory_region(uint64_t mr_key);

    /**
     * @brief Register remote memory region (normalized interface)
     *
     * @param remote_mr_key Remote memory region key
     * @param name Name/identifier for the remote region (unused for Ascend)
     * @param mr_info JSON metadata from remote peer's endpoint
     *
     * This matches RDMAEndpoint signature for consistency
     */
    void register_remote_memory_region(uint64_t remote_mr_key, const std::string& name, const json& mr_info);

    /**
     * @brief Get endpoint information for exchange with remote peer
     * @return JSON object containing endpoint metadata
     *
     * Returns: {"host": "...", "port": ...}
     */
    json endpoint_info() const;

    /**
     * @brief Connect to remote endpoint
     * @param remote_info JSON object with remote endpoint info
     *
     * Accepts output from remote peer's endpoint_info()
     */
    void connect(const json& remote_info);

    /**
     * @brief Disconnect from remote endpoint
     * @param host Remote host address
     * @param port Remote port number
     */
    void disconnect(const std::string& host, int port);

private:
    /**
     * @brief Internal helper to connect using host:port
     */
    int connect_internal(const std::string& host, int port);

    /**
     * @brief Internal helper to disconnect using engine name
     */
    int disconnect_internal(const std::string& adxl_engine_name);

    /**
     * @brief Get local memory pool (for sharing across endpoints)
     * @return Shared pointer to local memory pool
     */
    std::shared_ptr<AscendLocalMemoryPool> local_pool() const
    {
        return local_pool_;
    }

    /**
     * @brief Get remote memory pool
     * @return Shared pointer to remote memory pool
     */
    std::shared_ptr<AscendRemoteMemoryPool> remote_pool() const
    {
        return remote_pool_;
    }

private:
    static const int CONNECT_TIMEOUT_MILLIS = 60 * 1000;

    // Local endpoint info
    std::string local_host_;
    int         local_port_{-1};

    // AdxlEngine instance (black box)
    std::unique_ptr<adxl::AdxlEngine> adxl_ = nullptr;

    // Separate memory pools (RDMA pattern)
    // Local pool can be shared across multiple endpoints for memory efficiency
    // Remote pool is per-endpoint to avoid namespace collision
    std::shared_ptr<AscendLocalMemoryPool>  local_pool_;
    std::shared_ptr<AscendRemoteMemoryPool> remote_pool_;

    // Connected remote engines
    std::unordered_set<std::string> connected_engines_;

    // Track registered local memory regions for cleanup
    std::unordered_set<uint64_t> registered_mr_keys_;

    // Context pool for async operations (placeholder for future extension)
    // Currently using synchronous AdxlEngine API
};

}  // namespace dlslime
