#pragma once

#include <torch/torch.h>

#include <cstdint>
#include <cstdlib>

namespace dlslime {

#define ALL_GATHER_LL_SEND_PHASE 0b01
#define ALL_GATHER_LL_RECV_PHASE 0b10

void all_gather_inter_ll(torch::Tensor q,
                         int8_t*       sym_buffer_ptr,
                         int*          sym_signal_ptr,
                         int64_t       max_bs,
                         int64_t       msg_size,
                         int64_t       itemsize,
                         int64_t       world_size,
                         int64_t       rank,
                         int           phase,
                         int64_t       tag,
                         bool          allow_nvlink);

}  // namespace dlslime
