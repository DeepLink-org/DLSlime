#pragma once

#include <cstdint>
#include <cstdlib>

void all_gather_ll(uintptr_t q,
                   uintptr_t ipc_buffer_ptr,
                   uintptr_t ipc_signal_ptr,
                   int       max_bs,
                   int       num_head,
                   int       head_size,
                   int       itemsize,
                   int       world_size,
                   int       rank);
