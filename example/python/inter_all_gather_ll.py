import argparse
import os
import time
from pathlib import Path
from typing import List, Optional

import torch
import torch.distributed as dist
from dlslime.buffer.inter.all_gather_inter_ll_buffer import AllGatherInterLLBuffer
from dlslime.utils.json_merger import _merge_json

# Get SPMD Info
rank = int(os.environ["RANK"])
local_rank = int(os.environ["LOCAL_RANK"])
world_size = int(os.environ["WORLD_SIZE"])
local_world_size = int(os.environ["LOCAL_WORLD_SIZE"])
master_addr = os.environ["MASTER_ADDR"]
master_port = os.environ["MASTER_PORT"]


bs = 128
msg_size = 16384


valid_bs = 2


shape = [bs, msg_size]
dtype = torch.bfloat16
device = f"cuda:{local_rank}"
torch.cuda.set_device(local_rank)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eager-mode", action="store_true", help="--eager-mode")
    parser.add_argument("--hook-mode", action="store_true", help="--hook-mode")
    parser.add_argument("--allow-nvlink", action="store_true", help="--allow-nvlink")
    args = parser.parse_args()

    dist.init_process_group(backend="cpu:gloo,cuda:nccl")
    gpu_group = dist.new_group(ranks=list(range(world_size)), backend="nccl")
    torch.cuda.set_device(rank % 8)

    if rank == 0:
        root_time = torch.tensor(int(time.perf_counter()), dtype=torch.int)
    else:
        root_time = torch.zeros(1, dtype=torch.int)

    dist.broadcast(root_time, src=0)
    root_time = int(root_time)

    output_dir = "./"
    os.makedirs(output_dir, exist_ok=True)

    gather_buffer = AllGatherInterLLBuffer(
        bs,
        msg_size,
        dtype,
        world_size,
        rank,
        num_concurrency=1,
        allow_nvlink=args.allow_nvlink,
    )

    buffer_info = gather_buffer.buffer_info
    all_buffer_info = [None for _ in range(world_size)]
    dist.all_gather_object(all_buffer_info, buffer_info)
    gather_buffer.connect_full_mesh(all_buffer_info)

    input_tensor = torch.zeros(bs, msg_size, dtype=dtype, device=device)

    print("warmup begin")
    for _ in range(10):
        output = gather_buffer.all_gather_ll(input_tensor, tag=0)
        dist.barrier(group=gpu_group, device_ids=[local_rank])
        torch.cuda.synchronize()
    print("warmup done.")

    def forward(x: torch.Tensor):
        if args.hook_mode:
            output, hook = gather_buffer.all_gather_ll_hook(x, tag=0)
            hook()
        else:
            output = gather_buffer.all_gather_ll(x, tag=0)
        output.add_(0)
        return output

    # CUDA Graph
    if not args.eager_mode:
        cuda_graph = torch.cuda.CUDAGraph()
        stream = torch.cuda.Stream(device="cuda")
        print("cuda graph capture begin")
        with torch.cuda.stream(stream):
            with torch.cuda.graph(cuda_graph, stream=stream):
                forward(input_tensor)
        dist.barrier(group=gpu_group, device_ids=[local_rank])
        torch.cuda.synchronize()
        print("cuda graph capture done")

    if args.op_type == "all-gather":
        input_tensor.copy_(torch.ones(bs, msg_size, dtype=dtype, device=device) * rank)
    else:
        input_tensor.copy_(
            torch.ones(bs * world_size, msg_size, dtype=dtype, device=device) * rank
        )

    # profiling
    output_dir = "./"
    os.makedirs(output_dir, exist_ok=True)
    profiler_per_rank_output = Path(
        os.path.join(output_dir, f"profiler_{root_time}", "per_rank")
    )
    profiler_merged_output = Path(
        os.path.join(
            output_dir, f"profiler_{root_time}", f"profiler_{root_time}_merged.json"
        )
    )

    with torch.profiler.profile(
        activities=[
            torch.profiler.ProfilerActivity.CPU,
            torch.profiler.ProfilerActivity.CUDA,
        ],
        schedule=torch.profiler.schedule(wait=1, warmup=3, active=100, repeat=1),
        on_trace_ready=torch.profiler.tensorboard_trace_handler(
            dir_name=profiler_per_rank_output, worker_name=f"trace_rank_{rank}"
        ),
        record_shapes=True,
        profile_memory=True,
        with_stack=True,
        with_flops=True,
        with_modules=True,
    ) as prof:
        for i in range(100):
            torch.profiler.record_function(f"start_{i}")
            if args.eager_mode:
                output = forward(input_tensor)
            else:
                cuda_graph.replay()
            torch.profiler.record_function(f"end_{i}")

            dist.barrier(group=gpu_group, device_ids=[local_rank])
            torch.cuda.synchronize()

            if i % 10 == 0:
                prof.step()

    print(output, output.shape)
    dist.barrier(group=gpu_group, device_ids=[local_rank])
    dist.barrier()
    json_files_to_merge: List[Path] = list(profiler_per_rank_output.rglob("*.json"))
    _merge_json(
        to_merge_files=json_files_to_merge,
        output_json=profiler_merged_output,
        compress=False,
        version=2,
    )
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
