import os

import torch
import torch.distributed as dist

from dlslime.buffer.intra.all_to_all_intra_ll_buffer import AllToAllIntraLLBuffer


dist.init_process_group("cpu:gloo,cuda:nccl")

group = dist.group.WORLD
rank = dist.get_rank()
world_size = dist.get_world_size()

torch.cuda.set_device(rank)

max_bs = 2
max_msg_size = 32 * 128
max_dispatch_per_msg = world_size
dtype = torch.bfloat16

torch.cuda.set_device(rank)
buffer_size = AllToAllIntraLLBuffer.get_buffer_size_hint(
    max_dispatch_per_msg, max_bs, max_msg_size, dtype.itemsize
)

buffer = AllToAllIntraLLBuffer(
    max_dispatch_per_msg, max_bs, rank, world_size, buffer_size
)

buffer.connect_full_mesh(group=group)
x = (
    torch.tensor(
        [[1, 2, 3, 4, 5, 6, 7, 8], [8, 7, 6, 5, 4, 3, 2, 1]],
        dtype=torch.bfloat16,
        device=f"cuda:{rank}",
    )
    * rank
)
x = x.repeat(8, 1)
local_buffer = buffer.all_to_all_ll(x, is_transpose=True)
if rank == 0:
    print(x)
    print(local_buffer)

dist.destroy_process_group()
