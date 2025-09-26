import argparse
import os

import torch
import torch.distributed as dist

from dlslime import _slime_c


bs = 64


class DLSlimeQGather:

    def __init__(self, world_size, rank: int, mode: str, rdma_only: False):
        self.rank = rank
        # 根据模式选择不同的Buffer类
        if mode == "inter":
            self.buffer = _slime_c.AllGatherInterLLBuffer(
                bs, 576, torch.bfloat16, world_size, self.rank, rdma_only
            )
        elif mode == "intra":
            self.buffer = _slime_c.AllGatherIntraLLBuffer(
                bs, 576, torch.bfloat16, world_size, self.rank
            )
        else:
            raise ValueError(f"Unsupported Mode: {mode}，please use 'inter' or 'intra'")

        buffer_info = self.buffer.buffer_info()
        all_buffer_info = [None for _ in range(world_size)]
        dist.all_gather_object(all_buffer_info, buffer_info)
        self.buffer.connect_full_mesh(all_buffer_info)

    def forward(self, input_tensor: torch.Tensor, hook_mode=False) -> torch.Tensor:
        if hook_mode:
            t, hook = self.buffer.all_gather_ll_hook(input_tensor)
            hook()
        else:
            t = self.buffer.all_gather_ll(input_tensor)
        return t


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", type=str, default="intra", choices=["inter", "intra"], help="--mode"
    )
    parser.add_argument("--eager-mode", action="store_true", help="--eager-mode")
    parser.add_argument("--hook-mode", action="store_true", help="--hook-mode")
    parser.add_argument("--rdma-only", action="store_true", help="rdma-only")
    args = parser.parse_args()

    assert not (
        args.hook_mode and args.mode == "intra"
    ), "Only Inter Mode can support recv hook"

    if args.mode == "inter":
        # NVSHMEM ENVS
        os.environ["NVSHMEM_DISABLE_P2P"] = "0"
        os.environ["NVSHMEM_IB_ENABLE_IBGDA"] = "1"
        os.environ["NVSHMEM_IBGDA_NUM_RC_PER_PE"] = "8"
        # Make sure QP depth is always larger than the number of on-flight WRs, so that we can skip WQ slot check
        os.environ["NVSHMEM_QP_DEPTH"] = os.environ.get("NVSHMEM_QP_DEPTH", "1024")

        # Reduce gpu memory usage
        # 6 default teams + 1 extra team
        os.environ["NVSHMEM_MAX_TEAMS"] = "7"
        # Disable NVLink SHArP
        os.environ["NVSHMEM_DISABLE_NVLS"] = "1"
        # NOTES: NVSHMEM initialization requires at least 256 MiB
        os.environ["NVSHMEM_CUMEM_GRANULARITY"] = f"{2 **29}"

    # Get SPMD Info
    rank = int(os.environ["RANK"])
    local_rank = int(os.environ["LOCAL_RANK"])
    world_size = int(os.environ["WORLD_SIZE"])
    local_world_size = nnodes = int(os.environ["LOCAL_WORLD_SIZE"])
    master_addr = os.environ["MASTER_ADDR"]
    master_port = os.environ["MASTER_PORT"]

    dist.init_process_group(backend="gloo")
    torch.cuda.set_device(rank % 8)

    output_dir = "./"
    os.makedirs(output_dir, exist_ok=True)

    # 根据选择的模式初始化gather
    gather = DLSlimeQGather(world_size, rank, args.mode, args.rdma_only)

    input_tensor = (
        torch.ones(bs, 576, dtype=torch.bfloat16, device=f"cuda:{local_rank}")
        * rank
        * 0
    )

    # 预热
    for _ in range(10):
        output = gather.forward(input_tensor, args.hook_mode)
        dist.barrier()
        torch.cuda.synchronize()

    print("warmup done.")

    profiler_output = os.path.join(output_dir, f"rank_{rank}_profile")
    if args.eager_mode:
        # Eager mode
        with torch.profiler.profile(
            activities=[
                torch.profiler.ProfilerActivity.CPU,
                torch.profiler.ProfilerActivity.CUDA,
            ],
            schedule=torch.profiler.schedule(wait=1, warmup=3, active=100, repeat=1),
            on_trace_ready=torch.profiler.tensorboard_trace_handler(profiler_output),
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
            with_flops=True,
            with_modules=True,
        ) as prof:
            input_tensor.copy_(
                torch.ones(bs, 576, dtype=torch.bfloat16, device=f"cuda:{local_rank}")
                * rank
            )
            for i in range(500):
                torch.profiler.record_function(f"forward_start_{i}")
                dist.barrier()
                output = gather.forward(input_tensor, args.hook_mode)
                torch.profiler.record_function(f"forward_end_{i}")
                torch.cuda.synchronize()
                prof.step()
    else:
        # 默认模式：使用CUDA Graph
        graph = torch.cuda.CUDAGraph()
        device = torch.device(f"cuda:{local_rank}")
        stream = torch.cuda.Stream(device=device)

        with torch.cuda.stream(stream):
            with torch.cuda.graph(graph, stream=stream):
                output = gather.forward(input_tensor, args.hook_mode)
                output.add_(0)

        torch.cuda.synchronize()
        dist.barrier()
        input_tensor.copy_(
            torch.ones(bs, 576, dtype=torch.bfloat16, device=f"cuda:{local_rank}")
            * rank
        )
        with torch.profiler.profile(
            activities=[
                torch.profiler.ProfilerActivity.CPU,
                torch.profiler.ProfilerActivity.CUDA,
            ],
            schedule=torch.profiler.schedule(wait=1, warmup=3, active=100, repeat=1),
            on_trace_ready=torch.profiler.tensorboard_trace_handler(profiler_output),
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
            with_flops=True,
            with_modules=True,
        ) as prof:
            for i in range(500):
                torch.profiler.record_function(f"replay_start_{i}")
                graph.replay()
                torch.profiler.record_function(f"replay_end_{i}")
                dist.barrier()
                torch.cuda.synchronize()
                prof.step()

    print(output)
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
