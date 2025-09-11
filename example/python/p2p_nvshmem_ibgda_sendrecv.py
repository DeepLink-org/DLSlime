import os

import time

import torch
import torch.distributed as dist
import torch.utils.dlpack

from dlslime import _slime_c

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--rank", type=int, required=True)
parser.add_argument("--world-size", type=int, required=True)
args = parser.parse_args()

os.environ["MASTER_ADDR"] = "127.0.0.1"
os.environ["MASTER_PORT"] = "6006"

dist.init_process_group("cpu:gloo", rank=args.rank, world_size=args.world_size)

os.environ['NVSHMEM_DISABLE_P2P'] = '1'
os.environ['NVSHMEM_IB_ENABLE_IBGDA'] = '1'
os.environ['NVSHMEM_IBGDA_NUM_RC_PER_PE'] = '1'
# Make sure QP depth is always larger than the number of on-flight WRs, so that we can skip WQ slot check
os.environ['NVSHMEM_QP_DEPTH'] = os.environ.get('NVSHMEM_QP_DEPTH', '1024')

# Reduce gpu memory usage
# 6 default teams + 1 extra team
os.environ['NVSHMEM_MAX_TEAMS'] = '7'
# Disable NVLink SHArP
os.environ['NVSHMEM_DISABLE_NVLS'] = '1'
# NOTES: NVSHMEM initialization requires at least 256 MiB
os.environ['NVSHMEM_CUMEM_GRANULARITY'] = f'{2 ** 29}'

nvshmem_ctx = _slime_c.NVShmemContext(args.rank, 2, args.rank)
nvshmem_edpt_info = nvshmem_ctx.get_local_nvshmem_unique_id()

object_list = [None] * args.world_size

dist.all_gather_object(object_list, nvshmem_edpt_info)

nvshmem_ctx.connect_full_mesh(object_list, 0)

local_dlpack_tensor = nvshmem_ctx.alloc_dlpack_tensor(2048, 2048)
local_tensor = torch.from_dlpack(local_dlpack_tensor)
if args.rank == 0:
    local_tensor.add_(1)
print(local_tensor)

nvshmem_ctx.register_memory_region(
    "buffer",
    local_tensor.data_ptr(),
    local_tensor.storage_offset(),
    local_tensor.numel() * local_tensor.itemsize
)

print(f"{args.rank=}, {sum(local_tensor)}")

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

start_event.record()
for i in range(10):
    if args.rank == 0:
        print("Data Sending")
        nvshmem_ctx.send("buffer", 1)
        print(f"{args.rank=}, {sum(local_tensor.to(torch.float32))}")
    else:
        print("Data Recving")
        nvshmem_ctx.recv("buffer", 0)
        print(local_tensor)
        print(f"{args.rank=}, {sum(local_tensor.to(torch.float32))}")
torch.cuda.synchronize()    
end_event.record()

print(start_event.elapsed_time(end_event))
torch.cuda.synchronize()
time.sleep(1)

print("done")
dist.barrier()
