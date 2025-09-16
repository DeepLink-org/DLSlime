#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_runtime_api.h>

#include <cstdio>

#include "ops/intra/all_gather_ll.h"

__global__ void all_gather_ll_kernel(int8_t* q,
                                     void**  ipc_buffer_ptr,
                                     void**  ipc_signal_ptr,
                                     int     max_bs,
                                     int     num_head,
                                     int     head_size,
                                     int     itemsize,
                                     int     world_size,
                                     int     rank)
{
    int8_t* buffer_ptr = reinterpret_cast<int8_t*>(ipc_buffer_ptr[blockIdx.x]);
    int8_t* signal_ptr = reinterpret_cast<int8_t*>(ipc_signal_ptr[blockIdx.x]);

    int q_idx      = threadIdx.x;
    int buffer_idx = rank * max_bs * num_head * head_size * itemsize + threadIdx.x;
    int signal_idx = rank;

    buffer_ptr[buffer_idx] = q[q_idx];
    __syncthreads();

    // Step 2. signal <= 1
    if (threadIdx.x == 0)
        signal_ptr[signal_idx] = 1;

    __syncthreads();

    // Step 3. sync
    int8_t* local_signal_buffer = reinterpret_cast<int8_t*>(ipc_signal_ptr[rank]);
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
                   uintptr_t ipc_buffer_ptr,
                   uintptr_t ipc_signal_ptr,
                   int       max_bs,
                   int       num_head,
                   int       head_size,
                   int       itemsize,
                   int       world_size,
                   int       rank)
{
    int grid_dim  = world_size;
    int block_dim = max_bs * num_head * head_size * itemsize;

    all_gather_ll_kernel<<<grid_dim, block_dim>>>(reinterpret_cast<int8_t*>(q),
                                                  reinterpret_cast<void**>(ipc_buffer_ptr),
                                                  reinterpret_cast<void**>(ipc_signal_ptr),
                                                  max_bs,
                                                  num_head,
                                                  head_size,
                                                  itemsize,
                                                  world_size,
                                                  rank);
}
