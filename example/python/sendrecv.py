import argparse
import os

import torch
import torch.distributed as dist
from torch.distributed import distributed_c10d

from dlslime import _slime_torch  # noqa: F401

parser = argparse.ArgumentParser()
parser.add_argument('--mode', type=str)

parser.add_argument('--master-addr', type=str, default='localhost')
parser.add_argument('--master-port', type=str, default='6006')

args = parser.parse_args()

os.environ['MASTER_ADDR'] = args.master_addr
os.environ['MASTER_PORT'] = args.master_port

rank = 0 if args.mode == 'send' else 1
dist.init_process_group('cuda:dlslime', rank=rank, world_size=2)
torch.cuda.set_device(rank)

# 准备批量化数据
device = 'cuda'
send_batch = [torch.ones(3, device=device) * i for i in range(5)]
recv_batch = [torch.zeros(3, device=device) for _ in range(5)]

# 创建批量化通信请求
reqs = []
for i in range(5):
    dst = (rank + 1) % dist.get_world_size()
    src = (rank - 1) % dist.get_world_size()

    send_op = distributed_c10d.P2POp(dist.isend, send_batch[i], dst, tag=i)
    recv_op = distributed_c10d.P2POp(dist.irecv, recv_batch[i], src, tag=i)

    reqs.extend([send_op, recv_op])

# 批量执行所有通信操作
work = distributed_c10d.batch_isend_irecv(reqs)
print(work)
[w.wait() for w in work]

print(f'rtensor after send: {recv_batch}')

dist.destroy_process_group()

# if args.mode == 'send':
#     stensor = torch.ones([1600]).cuda()
#     dist.init_process_group('cuda:dlslime', rank=0, world_size=2)
#     dist.send(stensor, dst=1)
# if args.mode == 'recv':
#     rtensor = torch.zeros([1600]).cuda()
#     dist.init_process_group('cuda:dlslime', rank=1, world_size=2)
#     print(f'rtensor befors recv: {rtensor}')
#     dist.recv(rtensor, src=0)
#     print(f'rtensor after send: {rtensor}')
