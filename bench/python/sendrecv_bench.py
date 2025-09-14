"""# Send/Recv Benchmark

# One Node
torchrun \
    --nproc-per-node 2 \
    --nnodes=1 \
    --node_rank=0 \
    --master_port=$MASTER_PORT \
    bench/python/sendrecv_bench.py

# Cross Node
## Node 0
torchrun \
    --nproc-per-node 1 \
    --nnodes=2 \
    --node_rank=0 \
    --master-addr=$MASTER_ADDR \
    --master_port=$MASTER_PORT \
    bench/python/sendrecv_bench.py

## Node 1
torchrun \
    --nproc-per-node 1 \
    --nnodes=2 \
    --node_rank=1 \
    --master-addr=$MASTER_ADDR \
    --master_port=$MASTER_PORT \
    bench/python/sendrecv_bench.py
"""

import os

import torch
import torch.distributed as dist
from tabulate import tabulate

# add dlslime backend
try:
    from dlslime import _slime_torch  # noqa: F401
except ImportError as e:
    print(e, 'please install dlslime backend first')
    exit()

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--size', nargs='+', type=int, default=[2 << n for n in range(8, 25)])
parser.add_argument('--num-concurrency', type=int, default=8)

args = parser.parse_args()

print('mode: PyTorch SendRecv')
print(f'num concurrency: {args.num_concurrency}')

# Prepare table data
benchmark_data = []

rank = int(os.environ['RANK'])
world_size = int(os.environ['WORLD_SIZE'])
master_addr = os.environ['MASTER_ADDR']
master_port = os.environ['MASTER_PORT']

assert world_size == 2

torch.cuda.set_device(rank % world_size)

print('initializing process group')
dist.init_process_group('cpu:gloo', rank=rank, world_size=world_size)
slime_group = dist.new_group(ranks=[0, 1], backend='cuda:dlslime')
print('initializing process group done')

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = args.num_concurrency

ttensors = [torch.ones([size]).cuda() for size in args.size]

for ttensor, size in zip(ttensors, args.size):
    print(f'{size=}')
    total_time = 0.0

    start_event.record()
    for _ in range(100):
        futures = []
        for _ in range(n_runs):
            if rank == 0:
                future = dist.isend(ttensor, dst=1, group=slime_group)
            elif rank == 1:
                future = dist.irecv(ttensor, src=0, group=slime_group)
            futures.append(future)

        [future.wait() for future in futures]
    end_event.record()
    torch.cuda.synchronize()
    elapsed_time = start_event.elapsed_time(end_event)

    total_time += elapsed_time

    size_bytes = ttensor.numel() * ttensor.itemsize
    total_transport = n_runs * size * ttensor.itemsize
    avg_latency = total_time / n_runs
    bandwidth = n_runs * size * ttensor.itemsize * 100 / total_time / 1e3

    benchmark_data.append([
        f'{size_bytes}',
        f'{total_transport}',
        f'{avg_latency:.2f} ms',  # noqa: E231
        f'{bandwidth:.2f} MB/s'  # noqa: E231
    ])

# Print table
if rank == 1 and benchmark_data:
    headers = ['Message Size (bytes)', 'Total Transport (bytes)', 'Avg Latency', 'Bandwidth']
    print('\nBenchmark Results:')
    print(tabulate(benchmark_data, headers=headers, tablefmt='grid'))
