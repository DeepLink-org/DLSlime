#include "ibgda_device.cuh"
#include "utils/logging.h"
#include <cstdint>

namespace slime {

namespace internode {


__global__ void send_ll_kernel(int8_t* data, int8_t* buffer, int8_t* signal_buffer, size_t length, size_t msg_size_per_warp, size_t num_warps_per_sm, int rank, int dst_rank) {

    size_t alignment = msg_size_per_warp * num_warps_per_sm;
    size_t block_size = (length + alignment - 1) / alignment;
    size_t aligned_size = alignment * block_size;
    size_t warp_id = threadIdx.x / 32;
    size_t lane_id = threadIdx.x % 32;

    // Step 1. Data Copy
    size_t msg_size_per_thread = msg_size_per_warp / 32;
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    
    for (int i = 0; i < msg_size_per_thread; ++i)
    {
        int idx = i + threadIdx.x * msg_size_per_thread + blockIdx.x * blockDim.x * msg_size_per_thread;
        if (idx < length)
            buffer[idx] = data[idx];
    }
    __syncthreads();

    // Step 2. Data Transformation
    uintptr_t buffer_ptr = reinterpret_cast<uintptr_t>(buffer) + warp_id * msg_size_per_warp + num_warps_per_sm * msg_size_per_warp * blockIdx.x;
    deep_ep::nvshmemi_ibgda_put_nbi_warp(buffer_ptr, buffer_ptr, msg_size_per_warp, dst_rank, 0, lane_id, 0);

    __syncthreads();

    // Step 3. Send Signal
    if (lane_id == 0) {
        auto signal_ptr = reinterpret_cast<uintptr_t>(signal_buffer) + blockIdx.x;
        deep_ep::nvshmemi_ibgda_amo_nonfetch_add(reinterpret_cast<int8_t*>(signal_ptr), 1, dst_rank, 0);
    }
}

__global__ void recv_ll_kernel(int8_t* data, int8_t* buffer, int8_t* signal_buffer, size_t length, size_t msg_size_per_warp, size_t num_warps_per_sm, int rank, int src_rank) {

    // Step 1. Data Copy
    size_t msg_size_per_thread = msg_size_per_warp / 32;
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;

    if (threadIdx.x == 0) {
        while (__ldg(signal_buffer + blockIdx.x) != 32) {
        __threadfence_system();
        }
    }

    __syncthreads();

    for (int i = 0; i < msg_size_per_thread; ++i)
    {
        int idx = i + threadIdx.x * msg_size_per_thread + blockIdx.x * blockDim.x * msg_size_per_thread;
        if (idx < length)
            data[idx] = buffer[idx];
    }
    __syncthreads();

    signal_buffer[blockIdx.x] = 0;

}

void send_ll(int8_t* data, int8_t* buffer, int8_t* signal_buffer, size_t length, size_t msg_size_per_warp, size_t num_warps_per_sm, int rank, int dst_rank) {
    size_t alignment = msg_size_per_warp * num_warps_per_sm;
    size_t block_size = (length + alignment - 1) / alignment;
    size_t thread_size = num_warps_per_sm * 32;
    SLIME_LOG_INFO("block_size: " << block_size << ", thread_size: " << thread_size << ".");
    send_ll_kernel<<<block_size, thread_size>>>(data, buffer, signal_buffer, length, msg_size_per_warp, num_warps_per_sm, rank, dst_rank);
}

void recv_ll(int8_t* data, int8_t* buffer, int8_t* signal_buffer, size_t length, size_t msg_size_per_warp, size_t num_warps_per_sm, int rank, int src_rank) {
    size_t alignment = msg_size_per_warp * num_warps_per_sm;
    size_t block_size = (length + alignment - 1) / alignment;
    size_t thread_size = num_warps_per_sm * 32;
    SLIME_LOG_INFO("block_size: " << block_size << ", thread_size: " << thread_size << ".");
    recv_ll_kernel<<<block_size, thread_size>>>(data, buffer, signal_buffer, length, msg_size_per_warp, num_warps_per_sm, rank, src_rank);
}


}  // namespace internode
}  // namespace slime
