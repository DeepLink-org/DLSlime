#pragma once

#include <torch/torch.h>

#include <cstdint>
#include <cstdlib>

namespace dlslime {

void all_to_all_intra_ll(torch::Tensor                buffer_ori,
                         int8_t**                     ipc_buffer_ptr,
                         int**                        ipc_signal_ptr,
                         int32_t                      max_dispatch_per_msg,
                         int32_t                      max_bs,
                         int32_t                      rank,
                         int32_t                      world_size,
                         bool                         is_transpose,
                         c10::optional<torch::Tensor> mask);

}  // namespace dlslime
