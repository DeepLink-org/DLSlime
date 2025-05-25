import argparse
import os

import torch
import torch.distributed as dist

from dlslime import _slime_torch  # noqa: F401

default_sizes = [
    256,
    512,
    1024,
    2048,
    4096,
    8192,
    16384,
    32768,
    65536,
    131072,
    262144,
    524288,
    1048576,
    2097152,
    4194304,
    8388608,
    16777216,
    33554432,
    67108864,
    134217728,
]

parser = argparse.ArgumentParser()
parser.add_argument('--mode', type=str, choices=['send', 'recv'])

parser.add_argument('--master-addr', type=str, default='localhost')
parser.add_argument('--master-port', type=str, default='6006')

parser.add_argument('--size', nargs='+', type=int, default=default_sizes)

args = parser.parse_args()

os.environ['MASTER_ADDR'] = args.master_addr
os.environ['MASTER_PORT'] = args.master_port

print('initializing process group')
if args.mode == 'send':
    dist.init_process_group('cuda:dlslime', rank=0, world_size=2)
if args.mode == 'recv':
    dist.init_process_group('cuda:dlslime', rank=1, world_size=2)
print('initializing process group done')

# 创建CUDA事件对象
start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

# 正式测试循环
n_runs = 100
total_time = 0.0

for size in args.size:
    ttensor = torch.ones([size]).cuda()
    for _ in range(n_runs):
        # 记录CUDA事件
        start_event.record()
        if args.mode == 'send':
            dist.send(ttensor, dst=1)
        elif args.mode == 'recv':
            dist.recv(ttensor, src=0)
        end_event.record()

        # 等待事件完成并计算时间
        torch.cuda.synchronize()
        elapsed_time = start_event.elapsed_time(end_event)  # 毫秒

        total_time += elapsed_time

    print(f'size: {size}')
    print(f'total transport: {n_runs * size * ttensor.itemsize}')
    print(f'average latency: {total_time / n_runs}ms')
    print(f'bw: {n_runs * size * ttensor.itemsize / total_time / 1e3}MBps')
