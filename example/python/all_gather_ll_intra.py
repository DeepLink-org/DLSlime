import os

import torch
import torch.distributed as dist

from dlslime import _slime_c

# import pycuda.driver as cuda

dist.init_process_group('cuda:nccl,cpu:gloo')

max_bs = 8
num_head = 4
head_size = 8
itemsize = 2
dtype = torch.bfloat16

world_size = int(os.environ['WORLD_SIZE'])
rank = int(os.environ['RANK'])

torch.cuda.set_device(rank)

gpu_group = dist.new_group([_ for _ in range(world_size)], backend='nccl')

q_shape = [max_bs, num_head, head_size]
buffer_shape = [world_size, max_bs, num_head, head_size]
signal_shape = [world_size]

# allocate q
q = torch.ones(q_shape, dtype=dtype, device='cuda') * rank

# allocate buffer
buffer = torch.empty(buffer_shape, dtype=dtype, device='cuda')
signal = torch.zeros(signal_shape, dtype=torch.int8, device='cuda')

local_buffer_ipc_handle = buffer.untyped_storage()._share_cuda_()
local_signal_ipc_handle = signal.untyped_storage()._share_cuda_()

all_buffer_ipc_handles = [None for _ in range(world_size)]
all_signal_ipc_handles = [None for _ in range(world_size)]

dist.all_gather_object(all_buffer_ipc_handles, local_buffer_ipc_handle)
dist.all_gather_object(all_signal_ipc_handles, local_signal_ipc_handle)

buffer_ipc_ptrs = []
signal_ipc_ptrs = []
all_buffers = []
all_signals = []
for i in range(world_size):
    if i == rank:
        all_buffers.append(buffer)
        all_signals.append(signal)
        buffer_ipc_ptrs.append(buffer.data_ptr())
        signal_ipc_ptrs.append(signal.data_ptr())
    else:
        storage_buffer = torch.UntypedStorage._new_shared_cuda(*all_buffer_ipc_handles[i])
        remote_buffer = torch.tensor([], dtype=dtype, device=f'cuda:{i}').set_(storage_buffer, 0, buffer_shape)
        buffer_ipc_ptrs.append(remote_buffer.data_ptr())

        storage_signal = torch.UntypedStorage._new_shared_cuda(*all_signal_ipc_handles[i])
        remote_signal = torch.tensor([], dtype=torch.int8, device=f'cuda:{i}').set_(storage_signal, 0, signal_shape)
        signal_ipc_ptrs.append(remote_signal.data_ptr())

        remote_buffer.copy_(buffer)

        all_buffers.append(remote_buffer)
        all_signals.append(remote_signal)

print(all_buffers)

buffer_ipc_ptrs_tensor = torch.tensor(buffer_ipc_ptrs, dtype=torch.int64, device='cuda')
signal_ipc_ptrs_tensor = torch.tensor(signal_ipc_ptrs, dtype=torch.int64, device='cuda')
torch.cuda.synchronize()
dist.barrier()

with torch.profiler.profile(activities=[torch.profiler.ProfilerActivity.CUDA, torch.profiler.ProfilerActivity.CPU],
                            on_trace_ready=torch.profiler.tensorboard_trace_handler(f'./profiler_{rank}.json')) as prof:
    for i in range(1):
        _slime_c.all_gather_ll(q.data_ptr(), buffer_ipc_ptrs_tensor.data_ptr(), signal_ipc_ptrs_tensor.data_ptr(),
                               max_bs, num_head, head_size, itemsize, world_size, rank)
        # print(signal)
        # dist.barrier()
        dist.all_reduce(q)
        # dist.all_reduce(signal)
    prof.step()

torch.cuda.synchronize()
dist.barrier()

if rank == 1:
    print(buffer)

dist.destroy_process_group()
