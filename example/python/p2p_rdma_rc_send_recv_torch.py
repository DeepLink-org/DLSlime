import argparse
import os

import torch
import torch.distributed as dist
from dlslime import _slime_torch  # noqa: F401
from torch.distributed import distributed_c10d

parser = argparse.ArgumentParser()
parser.add_argument("--rank", type=int)

parser.add_argument("--master-addr", type=str, default="localhost")
parser.add_argument("--master-port", type=str, default="6006")

args = parser.parse_args()

os.environ["MASTER_ADDR"] = args.master_addr
os.environ["MASTER_PORT"] = args.master_port

rank = int(os.environ.get("RANK", None) or args.rank)
dist.init_process_group("cuda:dlslime", rank=rank, world_size=2)
torch.cuda.set_device(rank)

device = "cuda"
send_batch = [torch.ones(3, device=device) * i for i in range(5)]
recv_batch = [torch.zeros(3, device=device) for _ in range(5)]

print(f"rtensor before sendrecv: {recv_batch}")

reqs = []
for i in range(5):
    dst = (rank + 1) % dist.get_world_size()
    src = (rank - 1) % dist.get_world_size()

    send_op = distributed_c10d.P2POp(dist.isend, send_batch[i], dst, tag=i)
    recv_op = distributed_c10d.P2POp(dist.irecv, recv_batch[i], src, tag=i)

    reqs.extend([send_op, recv_op])

work = distributed_c10d.batch_isend_irecv(reqs)
[w.wait() for w in work]

print(f"rtensor after sendrecv: {recv_batch}")

dist.destroy_process_group()
