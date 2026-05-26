#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "adxl/adxl_engine.h"
#include "adxl/adxl_types.h"
#include "ascend_future.h"
#include "ascend_local_memory_pool.h"
#include "ascend_remote_memory_pool.h"
#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/logging.h"

namespace dlslime {

using json = nlohmann::json;

class AscendDirectEndpoint {
public:
    AscendDirectEndpoint();

    AscendDirectEndpoint(std::shared_ptr<AscendLocalMemoryPool>  local_pool,
                         std::shared_ptr<AscendRemoteMemoryPool> remote_pool);

    ~AscendDirectEndpoint();

    int init(const std::string& host, int port);

    /** Async read (handle-based fast path) */
    std::shared_ptr<AscendFuture> read(std::vector<assign_tuple_t>& assign, void* stream_handle = nullptr);

    /** Async read (name-based, resolves to handles) */
    std::shared_ptr<AscendFuture> read(std::vector<named_assign_tuple_t>& named_assign, void* stream_handle = nullptr);

    /** Register local memory region, returns handle */
    int32_t register_memory_region(uintptr_t                  addr,
                                   size_t                     offset,
                                   size_t                     length,
                                   std::optional<std::string> name = std::nullopt);

    /** Unregister a memory region by handle */
    void unregister_memory_region(int32_t handle);

    /** Register remote memory region, returns handle */
    int32_t register_remote_memory_region(const json& mr_info, std::optional<std::string> name = std::nullopt);

    int32_t get_mr_handle(const std::string& name);
    int32_t get_remote_mr_handle(const std::string& name);

    json endpoint_info() const;
    json mr_info() const;

    void connect(const json& remote_info);
    void disconnect(const std::string& host, int port);

private:
    int connect_internal(const std::string& host, int port);
    int disconnect_internal(const std::string& adxl_engine_name);

    std::shared_ptr<AscendLocalMemoryPool> local_pool() const
    {
        return local_pool_;
    }

    std::shared_ptr<AscendRemoteMemoryPool> remote_pool() const
    {
        return remote_pool_;
    }

private:
    static const int CONNECT_TIMEOUT_MILLIS = 60 * 1000;

    std::string local_host_;
    int         local_port_{-1};

    std::unique_ptr<adxl::AdxlEngine> adxl_ = nullptr;

    std::shared_ptr<AscendLocalMemoryPool>  local_pool_;
    std::shared_ptr<AscendRemoteMemoryPool> remote_pool_;

    std::unordered_set<std::string> connected_engines_;
    std::unordered_set<int32_t>     registered_mr_handles_;
};

}  // namespace dlslime
