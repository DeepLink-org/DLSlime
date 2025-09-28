#include <algorithm>
#include <cstdint>
#include <stdexcept>

#include <cstdio>

#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_runtime_api.h>

#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDADataType.h>
#include <torch/torch.h>

#include "all_gather_inter_ll.h"
#include "ops/ibgda_device.cuh"
#include "ops/launch.cuh"
#include "ops/nvshmem_api.cuh"
#include "ops/utils.cuh"

namespace slime {

#define MAX_SMS 128

__device__ void coalescing_load(int8_t* src, int8_t* des, int length)
{
    size_t warp_id = threadIdx.x / 32;
    size_t lane_id = threadIdx.x % 32;

    // Step 1. Vectorized and Coalesced Data Copy
    constexpr int VEC_SIZE = 16;
    using vec_t            = int4;

    int global_thread_idx = blockDim.x * blockIdx.x + threadIdx.x;
    int grid_stride       = gridDim.x * blockDim.x;

    size_t const vec_len = length / VEC_SIZE;
    vec_t*       vec_src = reinterpret_cast<vec_t*>(src);
    vec_t*       vec_des = reinterpret_cast<vec_t*>(des);

    for (int i = global_thread_idx; i < vec_len; i += grid_stride) {
        vec_des[i] = vec_src[i];
    }

    // handle tail part
    for (size_t i = vec_len * VEC_SIZE + global_thread_idx; i < length; i += grid_stride) {
        des[i] = src[i];
    }
}

__global__ __launch_bounds__(1024, 1) void all_gather_inter_ll_kernel(int8_t* q_ptr,
                                                                      int8_t* sym_buffer_ptr,
                                                                      int*    sym_signal_ptr,
                                                                      int32_t max_bs,
                                                                      int32_t msg_size,
                                                                      int32_t itemsize,
                                                                      int32_t world_size,
                                                                      int32_t rank,
                                                                      int32_t phases,
                                                                      bool    rdma_only)
{

    // Vectorize Optimization
    using vec_t        = int4;
    const int VEC_SIZE = sizeof(int4);

    const int num_sms = std::min(MAX_SMS, max_bs);

    const int sm_id   = blockIdx.x;
    const int warp_id = threadIdx.x / 32;
    const int lane_id = deep_ep::get_lane_id();

    const int dst_rank = warp_id;

    const int q_idx_base = sm_id * msg_size * itemsize;
    const int q_size     = max_bs * msg_size * itemsize;

    const int num_msg_per_warp     = msg_size * itemsize;
    const int num_vec_msg_per_warp = num_msg_per_warp / VEC_SIZE;
    int cnt = 0;

    if ((phases & ALL_GATHER_LL_SEND_PHASE) == 0)
        goto ALL_GATHER_LL_RECV;

    // Step 1. Write Q to buffer
    for (int q_idx = q_idx_base; q_idx < q_size; q_idx += num_sms * msg_size * itemsize) {

        if (dst_rank == rank) {
            int8_t*   q_ptr_for_write          = q_ptr + q_idx;
            vec_t*    vec_q_ptr_for_write      = reinterpret_cast<vec_t*>(q_ptr_for_write);
            const int buffer_idx               = q_idx + q_size * rank;
            int8_t*   buffer_ptr_for_write     = sym_buffer_ptr + buffer_idx;
            vec_t*    vec_buffer_ptr_for_write = reinterpret_cast<vec_t*>(buffer_ptr_for_write);

            UNROLLED_WARP_COPY(8,
                               lane_id,
                               num_vec_msg_per_warp,
                               vec_buffer_ptr_for_write,
                               vec_q_ptr_for_write,
                               deep_ep::ld_nc_global,
                               deep_ep::st_na_global);
        }
    }

    __syncthreads();

    // Step 2. Buffer Broadcast
    for (int q_idx = q_idx_base; q_idx < q_size; q_idx += num_sms * msg_size * itemsize) {
        if (dst_rank != rank) {
            const int slot_id = cnt * num_sms * world_size + sm_id * world_size + dst_rank;
            const int       buffer_idx           = q_idx + q_size * rank;
            const uintptr_t buffer_ptr_for_write = reinterpret_cast<uintptr_t>(sym_buffer_ptr + buffer_idx);
            const uintptr_t dst_buffer_p2p_ptr   = deep_ep::nvshmemi_get_p2p_ptr(buffer_ptr_for_write, rank, dst_rank);
            if (dst_buffer_p2p_ptr == 0 or rdma_only) {
                deep_ep::nvshmemi_ibgda_put_nbi_warp(
                    buffer_ptr_for_write, buffer_ptr_for_write, num_msg_per_warp, dst_rank, sm_id % 8, lane_id, 0);
            }
            else {
                vec_t* vec_buffer_ptr_for_write         = reinterpret_cast<vec_t*>(buffer_ptr_for_write);
                vec_t* vec_dst_buffer_p2p_ptr_for_write = reinterpret_cast<vec_t*>(dst_buffer_p2p_ptr);
                UNROLLED_WARP_COPY(8,
                                   lane_id,
                                   num_vec_msg_per_warp,
                                   vec_dst_buffer_p2p_ptr_for_write,
                                   vec_buffer_ptr_for_write,
                                   deep_ep::ld_nc_global,
                                   deep_ep::st_na_global);
            }
        }
        cnt += 1;
    }
    __syncwarp();

    // Step 3. Write Signal
    if (lane_id == 0) {
        const uintptr_t signal_ptr_for_write = reinterpret_cast<uintptr_t>(sym_signal_ptr + rank);
        const uintptr_t dst_signal_p2p_ptr =
            deep_ep::nvshmemi_get_p2p_ptr(reinterpret_cast<uintptr_t>(signal_ptr_for_write), rank, dst_rank);

        if (dst_signal_p2p_ptr == 0 or (dst_rank != rank and rdma_only))
            deep_ep::nvshmemi_ibgda_amo_nonfetch_add(sym_signal_ptr + rank, 1, dst_rank, sm_id % 8);
        else
            deep_ep::atomic_add_release_global(reinterpret_cast<int*>(dst_signal_p2p_ptr), 1);
    }
    __syncthreads();

    if ((phases & ALL_GATHER_LL_RECV_PHASE) == 0)
        return;

ALL_GATHER_LL_RECV:
    // Step 4. sync
    if (blockIdx.x == 0 and threadIdx.x < world_size) {

        while (deep_ep::ld_acquire_global(sym_signal_ptr + threadIdx.x) != num_sms)
            ;
        sym_signal_ptr[threadIdx.x] = 0;
    }
    return;
}

void all_gather_inter_ll(torch::Tensor q,
                         int8_t*       sym_buffer_ptr,
                         int*          sym_signal_ptr,
                         int32_t       max_bs,
                         int32_t       msg_size,
                         int32_t       itemsize,
                         int32_t       world_size,
                         int32_t       rank,
                         int           phase,
                         bool          rdma_only)
{

    int8_t* q_ptr = reinterpret_cast<int8_t*>(q.data_ptr());

    int num_sms   = std::min(128, max_bs);
    int num_warps = world_size;

    int grid_dim  = num_sms;
    int block_dim = num_warps * 32;

    auto stream = at::cuda::getCurrentCUDAStream();
    SETUP_LAUNCH_CONFIG(grid_dim, block_dim, stream);
    LAUNCH_KERNEL(&cfg,
                  all_gather_inter_ll_kernel,
                  q_ptr,
                  sym_buffer_ptr,
                  sym_signal_ptr,
                  max_bs,
                  msg_size,
                  itemsize,
                  world_size,
                  rank,
                  phase,
                  rdma_only);

    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        throw std::runtime_error(cudaGetErrorString(err));
    }

    return;
}

}  // namespace slime
