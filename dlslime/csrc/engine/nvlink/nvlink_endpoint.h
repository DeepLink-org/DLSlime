#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "cuda_common.cuh"
#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/logging.h"
#include "memory_pool.h"
#include "nvlink_future.h"

namespace dlslime {

class NVLinkEndpoint {
public:
    NVLinkEndpoint() = default;

    /* Async NVLink Read (handle-based fast path) */
    std::shared_ptr<NVLinkFuture> read(std::vector<assign_tuple_t>& assign, void* stream_handle = nullptr)
    {
        cudaStream_t stream = (cudaStream_t)stream_handle;

        for (size_t bid = 0; bid < assign.size(); ++bid) {
            auto local_handle  = static_cast<int32_t>(std::get<0>(assign[bid]));
            auto remote_handle = static_cast<int32_t>(std::get<1>(assign[bid]));

            auto target_offset = std::get<2>(assign[bid]);
            auto source_offset = std::get<3>(assign[bid]);
            auto length        = std::get<4>(assign[bid]);

            nvlink_mr_t source_mr   = memory_pool_.get_mr_fast(local_handle);
            uint64_t    source_addr = source_mr.addr;

            nvlink_mr_t target_mr   = memory_pool_.get_remote_mr_fast(remote_handle);
            uint64_t    target_addr = target_mr.addr;

            cudaMemcpyAsync((char*)(source_addr + source_offset),
                            (char*)(target_addr + target_offset),
                            length,
                            cudaMemcpyDeviceToDevice,
                            stream);
        }
        return std::make_shared<NVLinkFuture>();
    }

    /* Async NVLink Read (name-based, resolves to handles) */
    std::shared_ptr<NVLinkFuture> read(std::vector<named_assign_tuple_t>& named_assign, void* stream_handle = nullptr)
    {
        std::vector<assign_tuple_t> resolved;
        resolved.reserve(named_assign.size());

        for (auto& na : named_assign) {
            int32_t local_handle  = memory_pool_.get_mr_handle(std::get<0>(na));
            int32_t remote_handle = memory_pool_.get_remote_mr_handle(std::get<1>(na));

            if (local_handle < 0 || remote_handle < 0) {
                SLIME_LOG_ERROR("Named read: MR not found, local=", std::get<0>(na), " remote=", std::get<1>(na));
                return nullptr;
            }

            resolved.emplace_back(static_cast<uintptr_t>(local_handle),
                                  static_cast<uintptr_t>(remote_handle),
                                  std::get<2>(na),
                                  std::get<3>(na),
                                  std::get<4>(na));
        }
        return read(resolved, stream_handle);
    }

    /* Memory Management */
    int32_t register_memory_region(uintptr_t                  addr,
                                   uint64_t                   offset,
                                   size_t                     length,
                                   std::optional<std::string> name = std::nullopt)
    {
        return memory_pool_.register_memory_region(addr, offset, length, name);
    }

    int32_t register_remote_memory_region(const json& mr_info, std::optional<std::string> name = std::nullopt)
    {
        return memory_pool_.register_remote_memory_region(mr_info, name);
    }

    int32_t get_mr_handle(const std::string& name)
    {
        return memory_pool_.get_mr_handle(name);
    }

    int32_t get_remote_mr_handle(const std::string& name)
    {
        return memory_pool_.get_remote_mr_handle(name);
    }

    const json endpoint_info()
    {
        return {{"mr_info", memory_pool_.mr_info()}};
    }

    const json mr_info()
    {
        return memory_pool_.mr_info();
    }

    int connect(const json& endpoint_info_json)
    {
        for (auto& item : endpoint_info_json["mr_info"].items()) {
            register_remote_memory_region(item.value(), item.key());
        }
        return 0;
    }

private:
    NVLinkMemoryPool memory_pool_;
};
}  // namespace dlslime
