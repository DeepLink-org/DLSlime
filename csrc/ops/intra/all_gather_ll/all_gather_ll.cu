#include <algorithm>

#include <cstdio>

#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_runtime_api.h>

__global__ void all_gather_ll_kernel(int8_t*  q,
                                     int8_t** ipc_buffer_ptr,
                                     int8_t** ipc_signal_ptr,
                                     int32_t  max_bs,
                                     int32_t  num_head,
                                     int32_t  head_size,
                                     int32_t  itemsize,
                                     int32_t  world_size,
                                     int32_t  rank)
{

    int grid_dim           = world_size;
    int num_warps          = 32;
    int block_dim          = num_warps * 32 < max_bs * num_head * head_size * itemsize ?
                                 num_warps * 32 :
                                 max_bs * num_head * head_size * itemsize;
    int num_msg_per_thread = max_bs * num_head * head_size * itemsize / block_dim;

    int8_t* buffer_ptr = ipc_buffer_ptr[blockIdx.x];
    int8_t* signal_ptr = ipc_signal_ptr[blockIdx.x];

    for (int i = 0; i < num_msg_per_thread; ++i) {
        int q_idx              = threadIdx.x * num_msg_per_thread + i;
        int buffer_idx         = rank * max_bs * num_head * head_size * itemsize + threadIdx.x * num_msg_per_thread + i;
        buffer_ptr[buffer_idx] = q[q_idx];
    }

    __syncthreads();

    // Step 2. signal <= 1
    if (threadIdx.x == 0) {
        int signal_idx         = rank;
        signal_ptr[signal_idx] = 1;
    }

    __syncthreads();

    // Step 3. sync
    int8_t* local_signal_buffer = ipc_signal_ptr[rank];
    if (threadIdx.x == 0 and blockIdx.x == rank) {
        for (int i = 0; i < world_size; ++i) {
            while (__ldg(local_signal_buffer + i) != 1) {
                __threadfence_system();
            }
            local_signal_buffer[i] = 0;
        }
    }
}

void all_gather_ll(uintptr_t q,
                   int8_t**  ipc_buffer_ptr,
                   int8_t**  ipc_signal_ptr,
                   int32_t   max_bs,
                   int32_t   num_head,
                   int32_t   head_size,
                   int32_t   itemsize,
                   int32_t   world_size,
                   int32_t   rank)
{
    int grid_dim  = world_size;
    int num_warps = 32;
    int block_dim = num_warps * 32 < max_bs * num_head * head_size * itemsize ?
                        num_warps * 32 :
                        max_bs * num_head * head_size * itemsize;

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
