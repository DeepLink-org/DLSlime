#pragma once

#include <cstdint>
#include <vector>

#include "cuda_common.cuh"
#include "dlslime/engine/assignment.h"
#include "memory_pool.h"

namespace dlslime {

class NVLinkEndpoint {
public:
    NVLinkEndpoint() = default;

    /* Async NVLink Read */
    int64_t read(std::vector<uintptr_t>& mr_key,
                 std::vector<uintptr_t>& remote_mr_key,
                 std::vector<size_t>&    target_offset,
                 std::vector<size_t>&    source_offset,
                 std::vector<size_t>&    length,
                 void*                   stream_handle = nullptr)
    {
        cudaStream_t stream = (cudaStream_t)stream_handle;

        SLIME_ASSERT(mr_key.size() == remote_mr_key.size() == target_offset.size() == source_offset.size()
                         == length.size(),
                     "batchsize mismatch");
        size_t bs = mr_key.size();

        for (size_t bid = 0; bid < bs; ++bid) {
            nvlink_mr_t source_mr   = memory_pool_.get_mr(mr_key[bid]);
            uint64_t    source_addr = source_mr.addr;

            nvlink_mr_t target_mr   = memory_pool_.get_remote_mr(remote_mr_key[bid]);
            uint64_t    target_addr = target_mr.addr;

            cudaMemcpyAsync((char*)(source_addr + source_offset[bid]),
                            (char*)(target_addr + target_offset[bid]),
                            length[bid],
                            cudaMemcpyDeviceToDevice,
                            stream);
        }
        return 0;
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
