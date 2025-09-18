#include "ops/utils.cuh"

#include <algorithm>

#include <cstdio>

#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_runtime_api.h>

namespace slime {

__global__ void all_gather_ll_kernel(int8_t*  q_ptr,
                                     int8_t** ipc_buffer_ptr,
                                     int**    ipc_signal_ptr,
                                     int32_t  max_bs,
                                     int32_t  num_head,
                                     int32_t  head_size,
                                     int32_t  itemsize,
                                     int32_t  world_size,
                                     int32_t  rank)
{
    const int num_sms              = 128;
    const int num_warps_per_sm     = 1;
    const int num_threads_per_warp = 32;

    const int num_sms_per_rank = num_sms / world_size;

    const int sm_id                = blockIdx.x;
    const int peer_rank_id         = sm_id / num_sms_per_rank;
    const int peer_rank_channel_id = sm_id % num_sms_per_rank;

    const int num_threads_per_channel = num_warps_per_sm * num_threads_per_warp;
    const int num_threads_per_rank    = num_sms_per_rank * num_warps_per_sm * num_threads_per_warp;
    const int num_total_msg_per_rank  = max_bs * num_head * head_size * itemsize;

    const int num_msg_per_thread = num_total_msg_per_rank / num_threads_per_rank;

    int8_t* buffer_ptr = ipc_buffer_ptr[peer_rank_id];
    int*    signal_ptr = ipc_signal_ptr[peer_rank_id];

    // Vectorize Optimization
    using vec_t                          = int4;
    const int VEC_SIZE                   = 16;
    const int num_vec_msg_per_thread     = num_msg_per_thread / VEC_SIZE;
    const int num_total_vec_msg_per_rank = num_total_msg_per_rank / VEC_SIZE;
    vec_t*    vec_buffer_ptr             = reinterpret_cast<vec_t*>(buffer_ptr);
    vec_t*    vec_q_ptr                  = reinterpret_cast<vec_t*>(q_ptr);

#pragma unroll 4
    for (int i = 0; i < num_vec_msg_per_thread; ++i) {
        // Step 1. Split q to num_sms_per_rank parts;
        int q_idx = peer_rank_channel_id * num_threads_per_channel * num_vec_msg_per_thread
                    + threadIdx.x * num_vec_msg_per_thread + i;
        int buffer_idx             = rank * num_total_vec_msg_per_rank + q_idx;
        vec_buffer_ptr[buffer_idx] = vec_q_ptr[q_idx];
    }

    __syncthreads();

    // Step 2. signal <= 1
    if (threadIdx.x == 0) {
        int signal_idx = rank;
        atomicAdd_system(signal_ptr + signal_idx, 1);
    }

    __syncthreads();

    // Step 3. sync
    int* local_signal_buffer = ipc_signal_ptr[rank];
    if (threadIdx.x == 0 and blockIdx.x == num_sms_per_rank * rank) {
        for (int i = 0; i < world_size; ++i) {
            while (__ldg(local_signal_buffer + i) < num_sms_per_rank) {
                __threadfence_system();
            }
            local_signal_buffer[i] = 0;
        }
    }
}

void all_gather_ll(uintptr_t q,
                   int8_t**  ipc_buffer_ptr,
                   int**     ipc_signal_ptr,
                   int32_t   max_bs,
                   int32_t   num_head,
                   int32_t   head_size,
                   int32_t   itemsize,
                   int32_t   world_size,
                   int32_t   rank)
{
    int num_sms     = 128;
    int num_warps   = 1;
    int num_threads = 32;

    int grid_dim  = num_sms;
    int block_dim = num_warps * num_threads;

    all_gather_ll_kernel<<<grid_dim, block_dim>>>(reinterpret_cast<int8_t*>(q),
                                                  ipc_buffer_ptr,
                                                  ipc_signal_ptr,
                                                  max_bs,
                                                  num_head,
                                                  head_size,
                                                  itemsize,
                                                  world_size,
                                                  rank);
}

}  // namespace slime
