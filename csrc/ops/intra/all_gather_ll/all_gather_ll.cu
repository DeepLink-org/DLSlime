#include "ops/utils.cuh"

#include <algorithm>

#include <cstdio>

#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_runtime_api.h>

namespace slime {

__global__ __launch_bounds__(1024, 1) void all_gather_ll_kernel(int8_t*  q_ptr,
                                                                int8_t** ipc_buffer_ptr,
                                                                int**    ipc_signal_ptr,
                                                                int32_t  max_bs,
                                                                int32_t  num_head,
                                                                int32_t  head_size,
                                                                int32_t  itemsize,
                                                                int32_t  world_size,
                                                                int32_t  rank)
{

    const int num_sms = std::min(128, max_bs * num_head);

    const int sm_id   = blockIdx.x;
    const int warp_id = threadIdx.x / 32;
    const int lane_id = deep_ep::get_lane_id();

    // Vectorize Optimization
    using vec_t        = int4;
    const int VEC_SIZE = sizeof(int4);

    int num_msg_per_warp     = head_size * itemsize;
    int num_vec_msg_per_warp = num_msg_per_warp / VEC_SIZE;

    const int q_idx_base = sm_id * head_size * itemsize;
    const int q_size     = max_bs * num_head * head_size * itemsize;
    // write q to buffer
    for (int q_idx = q_idx_base; q_idx < q_size; q_idx += num_sms * head_size * itemsize) {
        int8_t* q_ptr_for_write          = q_ptr + q_idx;
        vec_t*  vec_q_ptr_for_write      = reinterpret_cast<vec_t*>(q_ptr_for_write);
        int     buffer_idx               = q_idx + q_size * rank;
        int8_t* buffer_ptr_for_write     = ipc_buffer_ptr[warp_id] + buffer_idx;
        vec_t*  vec_buffer_ptr_for_write = reinterpret_cast<vec_t*>(buffer_ptr_for_write);

        UNROLLED_WARP_COPY(8,
                           lane_id,
                           num_vec_msg_per_warp,
                           vec_buffer_ptr_for_write,
                           vec_q_ptr_for_write,
                           deep_ep::ld_nc_global,
                           deep_ep::st_na_global);
    }
    __syncwarp();

    // Step 2. signal <= 1
    // check struggler
    // barrier or signal pingpong

    int* signal_ptr = ipc_signal_ptr[warp_id];
    if (lane_id == 0) {
        atomicAdd_system(signal_ptr + rank, 1);
    }

    __syncthreads();

    // Step 3. sync
    int* local_signal_buffer = ipc_signal_ptr[rank];
    if (threadIdx.x == 0 and blockIdx.x == 0) {
        for (int i = 0; i < world_size; ++i) {
            // load acquire atomic sys global
            while (__ldg(local_signal_buffer + i) < num_sms) {
                __threadfence_system();
            }
            local_signal_buffer[i] = 0;
        }
    }
    __syncthreads();
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

    int num_sms   = std::min(128, max_bs * num_head);
    int num_warps = world_size;

    int grid_dim  = num_sms;
    int block_dim = num_warps * 32;

    // TODO: nvls (ptx)
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
