#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <cstdio>

#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_runtime_api.h>

#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDADataType.h>
#include <torch/torch.h>

#include "logging.h"
#include "ops/launch.cuh"
#include "ops/utils.cuh"

namespace slime {

#define MAX_NUM_WARPS 24

__global__ __launch_bounds__(1024, 1) void all_to_all_intra_ll_kernel(int8_t*  x_ptr,
                                                                      int32_t  bs,
                                                                      int32_t  msg_size,
                                                                      int32_t  itemsize,
                                                                      int8_t** ipc_buffer_ptr,
                                                                      int**    ipc_signal_ptr,
                                                                      int32_t  max_dispatch_per_msg,
                                                                      int32_t  max_bs,
                                                                      int32_t  rank,
                                                                      int32_t  world_size,
                                                                      bool     is_transpose = false,
                                                                      int32_t* mask         = nullptr)
{
    const int num_sms   = world_size;
    const int num_warps = MAX_NUM_WARPS < bs ? MAX_NUM_WARPS : bs;

    const int sm_id   = blockIdx.x;
    const int warp_id = threadIdx.x / 32;
    const int lane_id = deep_ep::get_lane_id();

    const int dst_rank = sm_id;

    // Vectorize Optimization
    using vec_t        = int4;
    const int VEC_SIZE = sizeof(int4);

    const int total_q_size = max_bs * msg_size * itemsize;

    const int num_msg_per_warp     = msg_size * itemsize;
    const int num_vec_msg_per_warp = num_msg_per_warp / VEC_SIZE;

    if (is_transpose) {
        x_ptr = x_ptr + dst_rank * total_q_size;
        bs    = max_bs;
    }

    for (int msg_idx = warp_id; msg_idx < bs; msg_idx += num_warps) {
        int8_t* q_ptr_for_write     = x_ptr + msg_idx * num_msg_per_warp;
        vec_t*  vec_q_ptr_for_write = reinterpret_cast<vec_t*>(q_ptr_for_write);

        int buffer_idx;
        buffer_idx = rank * total_q_size + msg_idx * num_msg_per_warp;

        int8_t* buffer_ptr_for_write     = ipc_buffer_ptr[sm_id] + buffer_idx;
        vec_t*  vec_buffer_ptr_for_write = reinterpret_cast<vec_t*>(buffer_ptr_for_write);

        if (!mask or __ldg(mask + msg_idx * world_size + sm_id)) {
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

    int* signal_ptr = ipc_signal_ptr[sm_id];
    lane_id == 0 and warp_id == 0 ? deep_ep::atomic_add_release_global(signal_ptr + rank, 1) : 0;

    // Step 3. sync
    int* local_signal_ptr = ipc_signal_ptr[rank];
    if (blockIdx.x == 0 and threadIdx.x < world_size) {
        while (deep_ep::ld_acquire_global(local_signal_ptr + threadIdx.x) != 1)
            ;
        local_signal_ptr[threadIdx.x] = 0;
    }
}

void all_to_all_intra_ll(torch::Tensor                x,
                         int8_t**                     ipc_buffer_ptr,
                         int**                        ipc_signal_ptr,
                         int32_t                      max_dispatch_per_msg,
                         int32_t                      max_bs,
                         int32_t                      rank,
                         int32_t                      world_size,
                         bool                         is_transpose,
                         c10::optional<torch::Tensor> mask)
{
    int8_t* x_ptr = reinterpret_cast<int8_t*>(x.data_ptr());

    auto shape = x.sizes();

    int32_t bs = is_transpose ? max_bs : x.size(0);

    int32_t msg_size = x.size(1);
    auto    itemsize = x.itemsize();

    int32_t* mask_ptr = mask.has_value() ? (*mask).data_ptr<int32_t>() : nullptr;

    int num_sms   = world_size;
    int num_warps = MAX_NUM_WARPS < bs ? MAX_NUM_WARPS : bs;

    SLIME_LOG_INFO("configuration: " << bs << ", " << msg_size << ", " << num_sms << ", " << num_warps << ".");

    int grid_dim  = num_sms;
    int block_dim = num_warps * 32;

    auto stream = at::cuda::getCurrentCUDAStream();
    SETUP_LAUNCH_CONFIG(grid_dim, block_dim, stream);
    LAUNCH_KERNEL(&cfg,
                  all_to_all_intra_ll_kernel,
                  x_ptr,
                  bs,
                  msg_size,
                  itemsize,
                  ipc_buffer_ptr,
                  ipc_signal_ptr,
                  max_dispatch_per_msg,
                  max_bs,
                  rank,
                  world_size,
                  is_transpose,
                  mask_ptr);

    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        throw std::runtime_error(cudaGetErrorString(err));
    }
}

}  // namespace slime
