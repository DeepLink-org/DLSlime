import os

import torch
import torch.distributed as dist
import torch.utils.dlpack

from dlslime import _slime_c

# import pycuda.driver as cuda

world_size = int(os.environ['WORLD_SIZE'])
rank = int(os.environ['RANK'])

dist.init_process_group('cuda:nccl,cpu:gloo')
gpu_group = dist.new_group([_ for _ in range(world_size)], backend='nccl')

max_bs = 32
num_head = 4
head_size = 128
itemsize = 2
dtype = torch.bfloat16

torch.cuda.set_device(rank)

buffer = _slime_c.AllGatherLLBuffer(max_bs, num_head, head_size, itemsize, world_size, rank)

ipc_info = buffer.ipc_info()
all_ipc_info = [None for _ in range(world_size)]
dist.all_gather_object(all_ipc_info, ipc_info)

buffer.connect_full_mesh(all_ipc_info)

# allocate q
q_shape = [max_bs, num_head, head_size]
buffer_shape = [world_size, max_bs, num_head, head_size]
signal_shape = [world_size]
q = torch.ones(q_shape, dtype=dtype, device=f'cuda:{rank}') * rank

with torch.profiler.profile(activities=[torch.profiler.ProfilerActivity.CUDA, torch.profiler.ProfilerActivity.CPU],
                            on_trace_ready=torch.profiler.tensorboard_trace_handler(f'./profiler_{rank}.json')) as prof:
    for i in range(10):
        buffer.all_gather_ll(q.data_ptr())
        # dist.barrier(group=gpu_group)
    prof.step()

local_buffer = buffer.dlpack_local_buffer()
local_buffer_tensor = torch.utils.dlpack.from_dlpack(local_buffer)
print(local_buffer_tensor.view(torch.bfloat16))

dist.barrier()
dist.destroy_process_group()
