import argparse
import os

import torch
import torch.distributed as dist

from dlslime import _slime_c


class DLSlimeQGather:

    def __init__(self, rank: int, mode: str):
        self.rank = rank
        # 根据模式选择不同的Buffer类
        if mode == "inter":
            self.buffer = _slime_c.AllGatherInterLLBuffer(
                64, 576, torch.bfloat16, 8, self.rank, False
            )
        elif mode == "intra":
            self.buffer = _slime_c.AllGatherIntraLLBuffer(
                64, 576, torch.bfloat16, 8, self.rank
            )
        else:
            raise ValueError(f"Unsupported Mode: {mode}，please use 'inter' or 'intra'")

        buffer_info = self.buffer.buffer_info()
        all_buffer_info = [None for _ in range(8)]
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
    args = parser.parse_args()

    assert not (
        args.hook_mode and args.mode == "intra"
    ), "Only Inter Mode can support recv hook"

    if args.mode == "inter":
        # NVSHMEM ENVS
        os.environ["NVSHMEM_DISABLE_P2P"] = "1"
        os.environ["NVSHMEM_IB_ENABLE_IBGDA"] = "1"
        os.environ["NVSHMEM_IBGDA_NUM_RC_PER_PE"] = "1"
        # Make sure QP depth is always larger than the number of on-flight WRs, so that we can skip WQ slot check
        os.environ["NVSHMEM_QP_DEPTH"] = os.environ.get("NVSHMEM_QP_DEPTH", "1024")

        # Reduce gpu memory usage
        # 6 default teams + 1 extra team
        os.environ["NVSHMEM_MAX_TEAMS"] = "7"
        # Disable NVLink SHArP
        os.environ["NVSHMEM_DISABLE_NVLS"] = "1"
        # NOTES: NVSHMEM initialization requires at least 256 MiB
        os.environ["NVSHMEM_CUMEM_GRANULARITY"] = f"{2 **29}"

    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    torch.cuda.set_device(rank)

    output_dir = "./"
    os.makedirs(output_dir, exist_ok=True)

    # 根据选择的模式初始化gather
    gather = DLSlimeQGather(rank, args.mode)

    input_tensor = (
        torch.ones(2, 8, 1152 * 2, dtype=torch.bfloat16, device=f"cuda:{rank}")
        * rank
        * 0
    )

    # 预热
    for _ in range(10):
        output = gather.forward(input_tensor, args.hook_mode)
        dist.barrier()
        torch.cuda.synchronize()

    profiler_output = os.path.join(output_dir, f"rank_{rank}_profile")
    if args.eager_mode:
        # Eager mode
        with torch.profiler.profile(
            activities=[
                torch.profiler.ProfilerActivity.CPU,
                torch.profiler.ProfilerActivity.CUDA,
            ],
            schedule=torch.profiler.schedule(wait=1, warmup=3, active=5, repeat=1),
            on_trace_ready=torch.profiler.tensorboard_trace_handler(profiler_output),
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
            with_flops=True,
            with_modules=True,
        ) as prof:
            input_tensor.copy_(
                torch.ones(2, 8, 1152 * 2, dtype=torch.bfloat16, device=f"cuda:{rank}")
                * rank
            )
            for i in range(10):
                torch.profiler.record_function(f"forward_start_{i}")
                dist.barrier()
                output = gather.forward(input_tensor, args.hook_mode)
                torch.profiler.record_function(f"forward_end_{i}")
                torch.cuda.synchronize()
                prof.step()
    else:
        # 默认模式：使用CUDA Graph
        graph = torch.cuda.CUDAGraph()
        device = torch.device(f"cuda:{rank}")
        stream = torch.cuda.Stream(device=device)

        with torch.cuda.stream(stream):
            with torch.cuda.graph(graph, stream=stream):
                output = gather.forward(input_tensor, args.hook_mode)
                output.add_(0)

        torch.cuda.synchronize()
        input_tensor.copy_(
            torch.ones(2, 8, 1152 * 2, dtype=torch.bfloat16, device=f"cuda:{rank}")
            * rank
        )
        with torch.profiler.profile(
            activities=[
                torch.profiler.ProfilerActivity.CPU,
                torch.profiler.ProfilerActivity.CUDA,
            ],
            schedule=torch.profiler.schedule(wait=1, warmup=3, active=5, repeat=1),
            on_trace_ready=torch.profiler.tensorboard_trace_handler(profiler_output),
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
            with_flops=True,
            with_modules=True,
        ) as prof:
            for i in range(10):
                torch.profiler.record_function(f"replay_start_{i}")
                dist.barrier()
                graph.replay()
                torch.profiler.record_function(f"replay_end_{i}")
                torch.cuda.synchronize()
                prof.step()

    print(output)
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
