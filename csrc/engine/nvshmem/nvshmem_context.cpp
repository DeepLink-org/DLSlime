#include "engine/nvshmem/kernels/exception.cuh"
#include "engine/nvshmem/kernels/internode_ll.cuh"

#include "nvshmem_context.h"

namespace slime {

using json = nlohmann::json;

NVShmemContext::NVShmemContext(const int rank, const int world_size, const int gpu_device_id):
    rank_(rank), world_size_(world_size), gpu_device_id_(gpu_device_id)
{
    cudaSetDevice(gpu_device_id_);
    SLIME_LOG_INFO("Set GPU device to " << gpu_device_id_);
    SLIME_LOG_INFO("NVShmem rank ID: " << rank_);
    cudaDeviceProp device_prop = {};
    CUDA_CHECK(cudaGetDeviceProperties(&device_prop, gpu_device_id_));
    num_device_sms_ = device_prop.multiProcessorCount;
    SLIME_LOG_INFO("Device total SMs: " << num_device_sms_);
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

void* NVShmemContext::allocBuffer(size_t size, size_t alignment)
{
    void* ptr = nvshmem_engine::internode::alloc(size, alignment);
    cudaMemset(ptr, 0, size);
    return ptr;
}

void NVShmemContext::registerMemoryRegion(const std::string& mr_key,
                                          const uintptr_t    data_ptr,
                                          const int          offset,
                                          const size_t       length)
{
    auto buffer                   = std::make_shared<nvshmem_context_buffer_t>();
    buffer->mr_key                = mr_key;
    buffer->data_ptr              = data_ptr;
    buffer->offset                = offset;
    buffer->length                = length;
    size_t MSG_SIZE_PER_SM        = NUM_WARP_PER_SM * MSG_SIZE_PER_WARP;
    size_t aligned_size           = (length + MSG_SIZE_PER_SM - 1) / MSG_SIZE_PER_SM * MSG_SIZE_PER_SM + 4096;
    size_t signal_size            = aligned_size / MSG_SIZE_PER_SM;
    void*  data_buffer            = allocBuffer(aligned_size, MSG_SIZE_PER_SM);
    void*  signal_buffer          = allocBuffer(signal_size, NVSHMEM_ALIGNMENT);
    buffer->nvshmem_data_buffer   = data_buffer;
    buffer->nvshmem_signal_buffer = signal_buffer;
    memory_pool_[mr_key]          = buffer;
    SLIME_LOG_INFO("Buffer Registered, "
                   << "data size: " << length << ", aligned size: " << aligned_size << ". signal size: " << signal_size
                   << ".");
}

void NVShmemContext::send(std::string mr_key, int dst)
{
    auto buffer = memory_pool_[mr_key];
    SLIME_LOG_INFO("NVSHMEM Send");
    slime::internode::send_ll(reinterpret_cast<int8_t*>(buffer->data_ptr),
                              reinterpret_cast<int8_t*>(buffer->nvshmem_data_buffer),
                              buffer->length,
                              MSG_SIZE_PER_WARP,
                              NUM_WARP_PER_SM,
                              rank_,
                              dst);
};

void NVShmemContext::recv(std::string mr_key, int src)
{
    auto buffer = memory_pool_[mr_key];
    SLIME_LOG_INFO("NVSHMEM RECV");
    slime::internode::recv_ll(reinterpret_cast<int8_t*>(buffer->data_ptr),
                              reinterpret_cast<int8_t*>(buffer->nvshmem_data_buffer),
                              buffer->length,
                              MSG_SIZE_PER_WARP,
                              NUM_WARP_PER_SM,
                              rank_,
                              src);
}

}  // namespace slime
