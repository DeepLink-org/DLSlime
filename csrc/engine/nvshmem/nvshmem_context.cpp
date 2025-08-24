#include "engine/nvshmem/kernels/exception.cuh"

#include "nvshmem_context.h"

namespace slime {

using json = nlohmann::json;

NVShmemContext::NVShmemContext(const int rank, const int world_size, const int gpu_device_id):
    rank_(rank), world_size_(world_size), gpu_device_id_(gpu_device_id)
{
    cudaSetDevice(gpu_device_id_);
    SLIME_LOG_INFO("Set GPU device to " << gpu_device_id_ << std::endl);
    SLIME_LOG_INFO("NVShmem rank ID: " << rank_ << std::endl);
    cudaDeviceProp device_prop = {};
    CUDA_CHECK(cudaGetDeviceProperties(&device_prop, gpu_device_id_));
    num_device_sms_ = device_prop.multiProcessorCount;
    SLIME_LOG_INFO("Device total SMs: " << num_device_sms_ << std::endl);
}

json NVShmemContext::getLocalNVShmemUniqueId() const
{
    json nvshmem_endpoint_info{};
    nvshmem_endpoint_info["nvshmem_info"] = {{"unique_id", nvshmem_engine::internode::get_unique_id()}};
    return nvshmem_endpoint_info;
}

int NVShmemContext::connectFullMesh(const std::vector<json> remote_info, int root_id)
{
    auto unique_ids   = remote_info[root_id]["nvshmem_info"]["unique_id"];
    int  nvshmem_rank = nvshmem_engine::internode::init(unique_ids, rank_, world_size_);
    SLIME_ASSERT(nvshmem_rank == rank_, "nvshmem_rank != rank_");
    return 0;
}

}  // namespace slime