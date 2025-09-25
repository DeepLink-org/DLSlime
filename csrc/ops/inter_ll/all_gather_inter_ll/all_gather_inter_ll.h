#pragma once

#include <cstdint>
#include <cstdlib>

#include <torch/torch.h>

namespace slime {

#define ALL_GATHER_LL_SEND_PHASE 0b01
#define ALL_GATHER_LL_RECV_PHASE 0b10

void all_gather_inter_ll(torch::Tensor q,
                         int8_t*       sym_buffer_ptr,
                         int*          sym_signal_ptr,
                         int32_t       max_bs,
                         int32_t       msg_size,
                         int32_t       itemsize,
                         int32_t       world_size,
                         int32_t       rank,
                         int           phase,
                         bool          rdma_only);

}  // namespace slime
