#pragma once

#include <cstdint>
#include <tuple>
#include <vector>

#include "cuda_common.cuh"
#include "dlslime/engine/assignment.h"
#include "memory_pool.h"
#include "nvlink_future.h"

namespace dlslime {

class NVLinkEndpoint {
public:
    NVLinkEndpoint() = default;

    /* Async NVLink Read */
    std::shared_ptr<NVLinkFuture> read(std::vector<assign_tuple_t>& assign, void* stream_handle = nullptr)
    {
        cudaStream_t stream = (cudaStream_t)stream_handle;

        size_t bs = assign.size();

        for (size_t bid = 0; bid < bs; ++bid) {
            auto mr_key        = std::get<0>(assign[bid]);
            auto remote_mr_key = std::get<1>(assign[bid]);

            auto target_offset = std::get<2>(assign[bid]);
            auto source_offset = std::get<3>(assign[bid]);

            auto length = std::get<4>(assign[bid]);

            nvlink_mr_t source_mr   = memory_pool_.get_mr(std::get<0>(assign[bid]));
            uint64_t    source_addr = source_mr.addr;

            nvlink_mr_t target_mr   = memory_pool_.get_remote_mr(std::get<1>(assign[bid]));
            uint64_t    target_addr = target_mr.addr;

            cudaMemcpyAsync((char*)(source_addr + std::get<3>(assign[bid])),
                            (char*)(target_addr + std::get<2>(assign[bid])),
                            std::get<4>(assign[bid]),
                            cudaMemcpyDeviceToDevice,
                            stream);
        }
        return std::make_shared<NVLinkFuture>();
    }

    /* Memory Management */
    int64_t register_memory_region(uintptr_t mr_key, uintptr_t addr, uint64_t offset, size_t length)
    {
        memory_pool_.register_memory_region(mr_key, addr, offset, length);
        return 0;
    }

    int64_t register_remote_memory_region(uintptr_t mr_key, const json& mr_info)
    {
        memory_pool_.register_remote_memory_region(mr_key, mr_info);
        return 0;
    }

    const json endpoint_info()
    {
        return {{"mr_info", memory_pool_.mr_info()}};
    }

    int connect(const json& endpoint_info_json)
    {
        // Register Remote Memory Region
        for (auto& item : endpoint_info_json["mr_info"].items()) {
            register_remote_memory_region(item.value()["mr_key"], item.value());
        }
        return 0;
    }

private:
    NVLinkMemoryPool memory_pool_;
};
}  // namespace dlslime
