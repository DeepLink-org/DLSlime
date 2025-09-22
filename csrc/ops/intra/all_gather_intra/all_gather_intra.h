#pragma once

#include <cstdint>
#include <cstdlib>

#include <torch/torch.h>

namespace slime {

void all_gather_ll(torch::Tensor q,
                   int8_t**      ipc_buffer_ptr,
                   int**         ipc_signal_ptr,
                   int32_t       max_bs,
                   int32_t       num_head,
                   int32_t       head_size,
                   int32_t       itemsize,
                   int32_t       world_size,
                   int32_t       rank);

}  // namespace slime
