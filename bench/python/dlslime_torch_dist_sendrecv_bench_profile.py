import argparse
import os
import time

import torch
import torch.distributed as dist
import torch.profiler
from tabulate import tabulate

# 尝试导入 dlslime backend
try:
    from dlslime import _slime_torch  # noqa: F401

    print("DLSlime backend registered:", "dlslime" in dist.Backend.backend_list)
except ImportError as e:
    print(f"{e}, please install dlslime backend first")
    exit()


def benchmark_send_recv(args):
    # 1. 初始化进程组
    rank = 0 if args.mode == "send" else 1
    os.environ["MASTER_ADDR"] = args.master_addr
    os.environ["MASTER_PORT"] = args.master_port
    os.environ["NCCL_P2P_DISABLE"] = "1"
    os.environ["NCCL_SHM_DISABLE"] = "1"

    backend_S = "dlslime" if args.use_gpu else "gloo"

    print(f"Rank {rank} initializing process group with backend: {backend_S}...")
    dist.init_process_group(backend_S, rank=rank, world_size=2)

    # 这里的 group 其实对于 P2P 来说不是必须的，但在某些版本下是个好习惯
    # slime_group = dist.new_group(ranks=[0, 1], backend=backend_S)

    if args.use_gpu:
        torch.cuda.set_device(rank)
        device = torch.device(f"cuda:{rank}")
    else:
        device = torch.device("cpu")

    # 2. 准备数据大小
    if args.sizes:
        sizes = [int(s) for s in args.sizes]
    else:
        sizes = [1024 * 1024 * 10]  # 10MB

    print(f"Rank {rank} prepared sizes: {sizes}")
    benchmark_data = []

    for size in sizes:
        num_elements = max(1, size // 4)  # float32 = 4 bytes

        send_tensor = torch.ones(num_elements, device=device, dtype=torch.float32)
        recv_tensor = torch.zeros(num_elements, device=device, dtype=torch.float32)

        print(f"Rank {rank} warming up...")
        for _ in range(5):
            if rank == 0:
                dist.send(send_tensor, dst=1)
            else:
                dist.recv(recv_tensor, src=0)

        if args.use_gpu:
            torch.cuda.synchronize()

        print(f"Rank {rank} starting profiler loop for size {size}...")

        with torch.profiler.profile(
            activities=[
                torch.profiler.ProfilerActivity.CPU,
                torch.profiler.ProfilerActivity.CUDA,
            ],
            schedule=torch.profiler.schedule(wait=1, warmup=1, active=3, repeat=1),
            on_trace_ready=torch.profiler.tensorboard_trace_handler(
                f"./log/rank_{rank}"
            ),
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
        ) as prof:

            total_steps = 1 + 1 + 32

            for step in range(total_steps):
                torch.cuda.nvtx.range_push(f"Step {step}")

                if args.use_gpu:
                    torch.cuda.nvtx.range_push("Compute Simulator")
                    temp = torch.matmul(
                        send_tensor.view(1, -1), send_tensor.view(-1, 1)
                    )
                    torch.cuda.nvtx.range_pop()

                torch.cuda.nvtx.range_push("Communication")

                if rank == 0:
                    # Sender
                    x = dist.isend(send_tensor, dst=1)
                else:
                    # Receiver
                    x = dist.irecv(recv_tensor, src=0)

                x.wait()

                torch.cuda.nvtx.range_pop()  # End Communication
                torch.cuda.nvtx.range_pop()  # End Step

                prof.step()

        if args.use_gpu:
            torch.cuda.synchronize()

        trace_path = f"trace_rank{rank}_size{size}.json"
        print(f"Rank {rank} trace saved to {trace_path}")

    dist.destroy_process_group()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DLSlime Profiler Benchmark")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["send", "recv"],
        help="Operation mode: 'send' or 'recv'",
    )
    parser.add_argument("--master-addr", type=str, default="127.0.0.1")
    parser.add_argument("--master-port", type=str, default="29500")
    parser.add_argument("--sizes", nargs="+", default=None, help="List of sizes")
    parser.add_argument("--use-gpu", action="store_true", default=True)

    args = parser.parse_args()

    benchmark_send_recv(args)
