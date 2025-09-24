#pragma once

#include <cstdint>
#include <cstdlib>

#include <torch/torch.h>

namespace slime {

void all_gather_inter_ll(torch::Tensor q,
                         int8_t*       sym_buffer_ptr,
                         int*          sym_signal_ptr,
                         int32_t       max_bs,
                         int32_t       msg_size,
                         int32_t       itemsize,
                         int32_t       world_size,
                         int32_t       rank);

}  // namespace slime
