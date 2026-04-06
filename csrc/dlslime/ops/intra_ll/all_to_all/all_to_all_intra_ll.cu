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

#include "dlslime/logging.h"
#include "dlslime/ops/launch.cuh"
#include "dlslime/ops/utils.cuh"

namespace dlslime {

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
                                                                      int32_t* mask         = nullptr,
                                                                      int32_t* offsets      = nullptr)
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
        if (offsets) {
            // Source-dependent count: each rank sends its own 'counts[rank]' to everyone
            int my_count = offsets[rank + 1] - offsets[rank];
            x_ptr = x_ptr + dst_rank * my_count * num_msg_per_warp;
            bs = my_count;
        } else {
            x_ptr = x_ptr + dst_rank * total_q_size;
            bs    = max_bs;
        }
    }

    for (int msg_idx = warp_id; msg_idx < bs; msg_idx += num_warps) {
        int8_t* q_ptr_for_write     = x_ptr + msg_idx * num_msg_per_warp;
        vec_t*  vec_q_ptr_for_write = reinterpret_cast<vec_t*>(q_ptr_for_write);

        int buffer_idx;
        if (offsets) {
            buffer_idx = (offsets[rank] + msg_idx) * num_msg_per_warp;
        } else {
            buffer_idx = rank * total_q_size + msg_idx * num_msg_per_warp;
        }

        int8_t* buffer_ptr_for_write     = ipc_buffer_ptr[sm_id] + buffer_idx;
        vec_t*  vec_buffer_ptr_for_write = reinterpret_cast<vec_t*>(buffer_ptr_for_write);

        if (!mask or __ldg(mask + sm_id * max_bs + msg_idx)) {
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

template <int kNumWarps>
__global__ void __launch_bounds__(kNumWarps * 32, 1)
intranode_alltoall_kernel(const void* x,
                          void**      buffer_ptr,
                          void**      signal_ptr,
                          int         local_rank,
                          int         world_size,
                          int         batch_size,
                          int         max_batch_size,
                          int         num_heads,
                          int         hidden_dim,
                          int         itemsize,
                          int*        local_semaphore,
                          bool        is_transpose,
                          const int*  mask    = nullptr,
                          const int*  offsets = nullptr)
{
    const int tid     = threadIdx.x;
    const int bid     = blockIdx.x;
    const int warp_id = tid / 32;
    const int lane_id = tid % 32;

    const int dst_rank            = bid % world_size;
    const int num_blocks_for_rank = gridDim.x / world_size;
    const int group_idx           = bid / world_size;

    const int tokens_per_block_avg = (batch_size + num_blocks_for_rank - 1) / num_blocks_for_rank;
    const int start_token_idx      = group_idx * tokens_per_block_avg;
    const int end_token_idx =
        (start_token_idx + tokens_per_block_avg < batch_size) ? (start_token_idx + tokens_per_block_avg) : batch_size;

    using vec_t                 = int4;
    const uint32_t bytes_per_token = num_heads * hidden_dim * itemsize;
    const int      ints_per_token  = bytes_per_token / sizeof(int4);

    const bool  use_offsets            = offsets != nullptr;
    const bool  use_target_major_input = is_transpose || (!use_offsets && mask == nullptr);
    const int4* src_base               = reinterpret_cast<const int4*>(x);
    if (use_target_major_input) {
        src_base += static_cast<uint64_t>(dst_rank) * batch_size * ints_per_token;
    }

    int8_t* dst_buffer_byte_base = reinterpret_cast<int8_t*>(buffer_ptr[dst_rank]);
    if (use_offsets) {
        dst_buffer_byte_base += static_cast<uint64_t>(offsets[local_rank]) * bytes_per_token;
    }
    else {
        dst_buffer_byte_base += static_cast<uint64_t>(local_rank) * batch_size * bytes_per_token;
    }
    int4* dst_base = reinterpret_cast<int4*>(dst_buffer_byte_base);

    if (start_token_idx < batch_size) {
        for (int token_i = start_token_idx; token_i < end_token_idx; ++token_i) {
            const int mask_stride = use_offsets ? max_batch_size : batch_size;
            if (mask != nullptr && __ldg(&mask[dst_rank * mask_stride + token_i]) == 0) {
                continue;
            }

            int token_offset  = token_i * ints_per_token;
            int ints_per_warp = (ints_per_token + kNumWarps - 1) / kNumWarps;
            int my_warp_offset = warp_id * ints_per_warp;
            int my_warp_len    = ints_per_token - my_warp_offset;
            if (my_warp_len > ints_per_warp)
                my_warp_len = ints_per_warp;
            if (my_warp_len < 0)
                my_warp_len = 0;

            if (my_warp_len > 0) {
                const int4* warp_src = src_base + token_offset + my_warp_offset;
                int4*       warp_dst = dst_base + token_offset + my_warp_offset;
                UNROLLED_WARP_COPY(
                    4, lane_id, my_warp_len, warp_dst, warp_src, deep_ep::ld_nc_global, deep_ep::st_na_global);
            }
        }
    }

    __syncthreads();

    if (tid == 0) {
        __threadfence_block();
        int finished_cnt = atomicAdd(&local_semaphore[dst_rank], 1);
        if (finished_cnt == num_blocks_for_rank - 1) {
            __threadfence_block();

            volatile int* target_signal =
                reinterpret_cast<volatile int*>(reinterpret_cast<int*>(signal_ptr[dst_rank]) + local_rank);
            *target_signal = 1;

            int*          my_signal_base     = reinterpret_cast<int*>(signal_ptr[local_rank]);
            volatile int* my_signal_from_dst = reinterpret_cast<volatile int*>(my_signal_base + dst_rank);
            while (*my_signal_from_dst == 0) {
            }
            *my_signal_from_dst = 0;
            __threadfence_block();
        }
    }
}

void intranode_alltoall(torch::Tensor                x,
                        void**                       buffer_ptr,
                        void**                       signal_ptr,
                        int                          batch_size,
                        int                          max_batch_size,
                        int                          n_heads,
                        int                          hidden_dim,
                        int                          rank,
                        int                          world_size,
                        int*                         device_semaphore_ptr,
                        bool                         is_transpose,
                        c10::optional<torch::Tensor> mask,
                        c10::optional<torch::Tensor> offsets)
{
    constexpr int num_warps = 16;

    TORCH_CHECK(x.is_cuda(), "x must be a CUDA tensor");
    TORCH_CHECK(x.is_contiguous(), "x must be contiguous");
    TORCH_CHECK(!(offsets.has_value() && is_transpose),
                "hao_basic offsets only support non-transpose all-to-all");

    size_t row_bytes = n_heads * hidden_dim * x.element_size();
    TORCH_CHECK(row_bytes % 16 == 0, "Data size per token must be divisible by 16 bytes");

    cudaStream_t stream = at::cuda::getCurrentCUDAStream();
    auto         kernel = intranode_alltoall_kernel<num_warps>;

    int blocks_per_rank = 16;
    if (blocks_per_rank > batch_size) {
        blocks_per_rank = std::max(1, batch_size);
    }

    int grid_size = blocks_per_rank * world_size;
    int block_dim = num_warps * 32;

    const int* mask_ptr = nullptr;
    if (mask.has_value()) {
        mask_ptr = mask.value().data_ptr<int>();
    }
    const int* offsets_ptr = nullptr;
    if (offsets.has_value()) {
        offsets_ptr = offsets.value().data_ptr<int>();
    }

    cudaMemsetAsync(device_semaphore_ptr, 0, world_size * sizeof(int), stream);
    kernel<<<grid_size, block_dim, 0, stream>>>(
        x.data_ptr(),
        buffer_ptr,
        signal_ptr,
        rank,
        world_size,
        batch_size,
        max_batch_size,
        n_heads,
        hidden_dim,
        x.element_size(),
        device_semaphore_ptr,
        is_transpose,
        mask_ptr,
        offsets_ptr);

    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        TORCH_CHECK(false, "Kernel Launch Error: ", cudaGetErrorString(err));
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
                         c10::optional<torch::Tensor> mask,
                         c10::optional<torch::Tensor> offsets)
{
    int8_t* x_ptr = reinterpret_cast<int8_t*>(x.data_ptr());

    auto shape = x.sizes();

    int32_t bs = is_transpose ? (x.size(0) / world_size) : x.size(0);

    int32_t msg_size = x.size(1);
    auto    itemsize = x.itemsize();

    int32_t* mask_ptr    = mask.has_value() ? (*mask).data_ptr<int32_t>() : nullptr;
    int32_t* offsets_ptr = offsets.has_value() ? (*offsets).data_ptr<int32_t>() : nullptr;

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
                  mask_ptr,
                  offsets_ptr);

    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        throw std::runtime_error(cudaGetErrorString(err));
    }
}

}  // namespace dlslime
