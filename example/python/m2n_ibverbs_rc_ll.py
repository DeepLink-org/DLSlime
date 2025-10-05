import argparse
import os

import torch
import torch.distributed as dist

from dlslime.buffer.ibverbs.m2n_ibverbs_rc_ll_buffer import M2NIBVerbsRCLLBuffer


# distributed config
rank = int(os.environ["RANK"])
world_size = int(os.environ["WORLD_SIZE"])
local_rank = int(os.environ["LOCAL_RANK"])
local_world_size = int(os.environ["LOCAL_WORLD_SIZE"])
if rank == 0:
    print(f"{rank=}, {world_size=}, {local_rank=}, {local_world_size=}")

torch.cuda.set_device(local_rank)

parser = argparse.ArgumentParser()
parser.add_argument("--m-size", type=int, default=None)
parser.add_argument("--n-size", type=int, default=None)
args = parser.parse_args()

# Peer Size
m_size = args.m_size or world_size // 2
n_size = args.n_size or world_size // 2

assert (
    m_size + n_size == world_size
), f"m_size({m_size}) + n_size({n_size}) != world_size({world_size})"

max_bs = 128
msg_size = 2048
device = f"cuda:{rank}"
role = 0 if rank < m_size else 1
num_concurrency = 1
qp_nums_per_rank = 2

num_topk = 8

m2n_rank = rank if role == 0 else rank - m_size


def feed_buffer():
    raise NotImplementedError


if __name__ == "__main__":
    dist.init_process_group("cuda:nccl,cpu:gloo")

    num_topk = 2
    x = torch.empty([max_bs, msg_size], device=device)
    top_k_idx = torch.rand(16, 8, device="cuda").argsort(dim=1)[:, :num_topk]

    # initialize buffer
    buffer = M2NIBVerbsRCLLBuffer(
        max_bs,
        msg_size,
        device,
        role,
        m_size,
        n_size,
        m2n_rank,
        num_concurrency,
        qp_nums_per_rank,
    )
    buffer_info = buffer.buffer_info

    all_buffer_info = [None for _ in range(world_size)]
    dist.all_gather_object(all_buffer_info, buffer_info)
    peer_buffer_info = (
        all_buffer_info[:m_size] if role == 1 else all_buffer_info[m_size:]
    )

    buffer.connect_full_mesh(peer_buffer_info)

    dist.barrier()
    dist.destroy_process_group()
