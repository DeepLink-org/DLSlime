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

#include "ops/launch.cuh"
#include "ops/utils.cuh"

namespace slime {

#define MAX_SMS 128

__global__ __launch_bounds__(1024, 1) void all_gather_inter_ll_kernel(int8_t*  q_ptr,
                                                                      int8_t** ipc_buffer_ptr,
                                                                      int**    ipc_signal_ptr,
                                                                      int32_t  max_bs,
                                                                      int32_t  msg_size,
                                                                      int32_t  itemsize,
                                                                      int32_t  world_size,
                                                                      int32_t  rank)
{
}

void all_gather_inter_ll(torch::Tensor q,
                         int8_t*       sym_buffer_ptr,
                         int*          sym_signal_ptr,
                         int32_t       max_bs,
                         int32_t       msg_size,
                         int32_t       itemsize,
                         int32_t       world_size,
                         int32_t       rank)
{
    return;
}

}  // namespace slime
