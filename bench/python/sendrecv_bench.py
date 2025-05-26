import argparse
import os

import torch
import torch.distributed as dist

# add dlslime backend
try:
    from dlslime import _slime_torch  # noqa: F401
except ImportError as e:
    print(e, 'please install dlslime backend first')
    exit()

parser = argparse.ArgumentParser()
parser.add_argument('--size', nargs='+', type=int, default=[2 << n for n in range(8, 27)])

args = parser.parse_args()

rank = int(os.environ['RANK'])
world_size = int(os.environ['WORLD_SIZE'])
master_addr = os.environ['MASTER_ADDR']
master_port = os.environ['MASTER_PORT']

assert world_size == 2

print('initializing process group')
dist.init_process_group('cuda:dlslime', rank=rank, world_size=2)
print('initializing process group done')

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = 100
total_time = 0.0

for size in args.size:
    ttensor = torch.ones([size]).cuda()
    for _ in range(n_runs):
        start_event.record()
        if rank == 0:
            dist.send(ttensor, dst=1)
        elif rank == 1:
            dist.recv(ttensor, src=0)
        end_event.record()

        torch.cuda.synchronize()
        elapsed_time = start_event.elapsed_time(end_event)  # 毫秒

        total_time += elapsed_time

    if rank == 1:
        print(f'size: {size}')
        print(f'total transport: {n_runs * size * ttensor.itemsize}')
        print(f'average latency: {total_time / n_runs}ms')
        print(f'bw: {n_runs * size * ttensor.itemsize / total_time / 1e3} MBps')
