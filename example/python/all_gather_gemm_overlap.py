import argparse
import os

import torch
import torch.distributed as dist

from dlslime.buffer.inter.all_gather_inter_ll_buffer import AllGatherInterLLBuffer


# Get SPMD Info
rank = int(os.environ["RANK"])
local_rank = int(os.environ["LOCAL_RANK"])
world_size = int(os.environ["WORLD_SIZE"])
local_world_size = int(os.environ["LOCAL_WORLD_SIZE"])
master_addr = os.environ["MASTER_ADDR"]
master_port = os.environ["MASTER_PORT"]

os.environ["NVSHMEM_SYMMETRIC_SIZE "] = "1g"


bs = 16
msg_size = 16384


shape = [bs, msg_size]
dtype = torch.bfloat16
device = f"cuda:{local_rank}"
torch.cuda.set_device(local_rank)


class AllGatherInterLLGemm(torch.nn.Module):
    def __init__(self, bs, msg_size, dtype, rank, world_size, allow_nvlink):
        super().__init__()

        # set num_concurrency to 2 for ag gemm overlapping
        self.gather_buffer = AllGatherInterLLBuffer(
            bs,
            msg_size,
            dtype,
            world_size,
            rank,
            num_concurrency=2,
            allow_nvlink=allow_nvlink,
        )

        # connect full mesh
        buffer_info = self.gather_buffer.buffer_info
        all_buffer_info = [None for _ in range(world_size)]
        dist.all_gather_object(all_buffer_info, buffer_info)
        self.gather_buffer.connect_full_mesh(all_buffer_info)

        self.linear = torch.nn.Linear(msg_size, msg_size, dtype=dtype)

    def forward(self, x0: torch.Tensor, x1: torch.Tensor):
        t0, hook0 = self.gather_buffer.all_gather_ll_hook(x0, tag=0)
        hook0()

        t1, hook1 = self.gather_buffer.all_gather_ll_hook(x1, tag=1)
        y0 = self.linear(t0)
        hook1()

        y1 = self.linear(t1)

        return y0, y1


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser()
    parser.add_argument("--eager-mode", action="store_true", help="--eager-mode")
    parser.add_argument("--allow-nvlink", action="store_true", help="rdma-only")
    args = parser.parse_args()

    dist.init_process_group(backend="nccl")
    gpu_group = dist.new_group(ranks=list(range(world_size)), backend="nccl")
    torch.cuda.set_device(rank % 8)

    output_dir = "./"
    os.makedirs(output_dir, exist_ok=True)

    ag_gemm = (
        AllGatherInterLLGemm(
            bs, msg_size, dtype, rank, world_size, allow_nvlink=args.allow_nvlink
        )
        .cuda()
        .eval()
    )

    x0 = torch.zeros(bs, msg_size, dtype=dtype, device=device)
    x1 = torch.zeros(bs, msg_size, dtype=dtype, device=device)

    print("warmup begin")
    for _ in range(10):
        output = ag_gemm(x0, x1)
        dist.barrier(group=gpu_group, device_ids=[local_rank])
        torch.cuda.synchronize()
    print("warmup done.")

    # CUDA Graph
    if not args.eager_mode:
        cuda_graph = torch.cuda.CUDAGraph()
        stream = torch.cuda.Stream(device="cuda")
        print("cuda graph capture begin")
        with torch.cuda.stream(stream):
            with torch.cuda.graph(cuda_graph, stream=stream):
                output = ag_gemm(x0, x1)
        dist.barrier(group=gpu_group, device_ids=[local_rank])
        torch.cuda.synchronize()
        print("cuda graph capture done")

    x0.copy_(torch.ones(bs, msg_size, dtype=dtype, device=device) * rank)
    x1.copy_(torch.ones(bs, msg_size, dtype=dtype, device=device) * rank)

    # profiling
    output_dir = "./"
    os.makedirs(output_dir, exist_ok=True)
    profiler_output = os.path.join(output_dir, "profiler")

    with torch.profiler.profile(
        activities=[
            torch.profiler.ProfilerActivity.CPU,
            torch.profiler.ProfilerActivity.CUDA,
        ],
        schedule=torch.profiler.schedule(wait=1, warmup=3, active=100, repeat=1),
        on_trace_ready=torch.profiler.tensorboard_trace_handler(
            dir_name=profiler_output, worker_name=f"trace_rank_{rank}"
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
                output = ag_gemm(x0, x1)
            else:
                cuda_graph.replay()
            torch.profiler.record_function(f"end_{i}")

            dist.barrier(group=gpu_group, device_ids=[local_rank])
            torch.cuda.synchronize()

            if i % 10 == 0:
                prof.step()

    print(output)
    dist.barrier(group=gpu_group, device_ids=[local_rank])
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
