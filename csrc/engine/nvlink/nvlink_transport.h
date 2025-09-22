#pragma once

#include <cstdint>

#include "cuda_common.cuh"
#include "engine/assignment.h"
#include "memory_pool.h"

namespace slime {

class NVLinkContext {
public:
    NVLinkContext() = default;

    /* Async NVLink Read */
    int64_t read_batch(AssignmentBatch& batch, uintptr_t stream_addr = (uintptr_t) nullptr)
    {
        cudaStream_t stream = (cudaStream_t)stream_addr;

        for (Assignment& assign : batch) {
            std::string mr_key = assign.mr_key;

            nvlink_mr_t source_mr     = memory_pool_.get_mr(mr_key);
            uint64_t    source_addr   = source_mr.addr;
            uint64_t    source_offset = source_mr.offset + assign.source_offset;

            nvlink_mr_t target_mr     = memory_pool_.get_remote_mr(mr_key);
            uint64_t    target_addr   = target_mr.addr;
            uint64_t    target_offset = target_mr.offset + assign.target_offset;

            size_t length = assign.length;

            cudaMemcpyAsync((char*)(source_addr + source_offset),
                            (char*)(target_addr + target_offset),
                            assign.length,
                            cudaMemcpyDeviceToDevice,
                            stream);
        }
        return 0;
    }

    /* Memory Management */
    int64_t register_memory_region(std::string mr_key, uintptr_t addr, uint64_t offset, size_t length)
    {
        memory_pool_.register_memory_region(mr_key, addr, offset, length);
        return 0;
    }

    int64_t register_remote_memory_region(std::string mr_key, const json& mr_info)
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
            register_remote_memory_region(item.key(), item.value());
        }
        return 0;
    }

private:
    NVLinkMemoryPool memory_pool_;
};
}  // namespace slime
