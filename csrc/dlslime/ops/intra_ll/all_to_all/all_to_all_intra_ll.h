#pragma once

#include <cstdint>
#include <cstdlib>

#include <torch/torch.h>

namespace dlslime {

void all_to_all_intra_ll(torch::Tensor                buffer_ori,
                         int8_t**                     ipc_buffer_ptr,
                         int**                        ipc_signal_ptr,
                         int32_t                      max_dispatch_per_msg,
                         int32_t                      max_bs,
                         int32_t                      rank,
                         int32_t                      world_size,
                         bool                         is_transpose,
                         c10::optional<torch::Tensor> mask,
                         c10::optional<torch::Tensor> offsets);

void intranode_alltoall(torch::Tensor                x,
                        void**                       buffer_ptr,
                        void**                       signal_ptr,
                        int                          batch_size,
                        int                          n_heads,
                        int                          hidden_dim,
                        int                          rank,
                        int                          world_size,
                        int*                         device_semaphore_ptr,
                        bool                         is_transpose,
                        c10::optional<torch::Tensor> mask = c10::nullopt);

}  // namespace dlslime
