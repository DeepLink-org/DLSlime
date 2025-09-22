import os

import torch
import torch.distributed as dist
import torch.utils.dlpack

from dlslime import _slime_c

# import pycuda.driver as cuda

world_size = int(os.environ['WORLD_SIZE'])
rank = int(os.environ['RANK'])

dist.init_process_group('cuda:nccl,cpu:gloo')

max_bs = 2
num_head = 8
head_size = 1152 * 2
itemsize = 2
dtype = torch.bfloat16

torch.cuda.set_device(rank)

buffer = _slime_c.AllGatherLLBuffer(max_bs * num_head, head_size, itemsize, world_size, rank)

ipc_info = buffer.ipc_info()
all_ipc_info = [None for _ in range(world_size)]
dist.all_gather_object(all_ipc_info, ipc_info)
buffer.connect_full_mesh(all_ipc_info)

# allocate q
q_shape = [max_bs, num_head, head_size]
buffer_shape = [world_size, max_bs, num_head, head_size]
signal_shape = [world_size]
q = torch.ones(q_shape, dtype=dtype, device=f'cuda:{rank}') * rank

for i in range(10):
    buffer.all_gather_ll(q.data_ptr())
torch.cuda.synchronize()

with torch.profiler.profile(activities=[torch.profiler.ProfilerActivity.CUDA, torch.profiler.ProfilerActivity.CPU],
                            on_trace_ready=torch.profiler.tensorboard_trace_handler(f'./profiler_{rank}.json'),
                            schedule=torch.profiler.schedule(
                                skip_first=2,
                                wait=1,
                                warmup=1,
                                active=8,
                                repeat=1,
                            )) as prof:
    for i in range(10):
        buffer.all_gather_ll(q.data_ptr())
        torch.cuda.synchronize()
        prof.step()

local_buffer = buffer.dlpack_local_buffer()
local_buffer_tensor = torch.utils.dlpack.from_dlpack(local_buffer)
print(local_buffer_tensor.view(torch.bfloat16))
print(local_buffer_tensor.shape)
# print(q)

dist.barrier()
dist.destroy_process_group()
