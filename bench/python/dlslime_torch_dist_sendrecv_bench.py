import argparse
import os
import time
from tabulate import tabulate
import cProfile

import torch
import torch.distributed as dist
from torch.distributed import distributed_c10d

# add dlslime backend
try:
    from dlslime import _slime_torch  # noqa: F401

    print("DLSlime backend registered:", "dlslime" in dist.Backend.backend_list)
except ImportError as e:
    print(e, "please install dlslime backend first")
    exit()

def benchmark_send_recv(args):
    # Initialize process group
    print("Initialize process group")
    rank = 0 if args.mode == "send" else 1
    backend_S = "cuda:dlslime" if args.use_gpu else "cpu:dlslime"
    dist.init_process_group(backend_S, rank=rank, world_size=2)
    slime_group = dist.new_group(ranks=[0, 1], backend=backend_S)
    print(backend_S)
    print("rank: ", rank)
    if args.use_gpu:
        torch.cuda.set_device(rank)
        device = "cuda"
        print("Device: cuda")
    else:
        device = "cpu"
        print("Device: cpu")

    # Prepare data sizes to test (in bytes)
    if args.sizes:
        sizes = [int(s) for s in args.sizes]
    else:
        sizes = [2**n for n in range(11, 26)]  # 256B to 256MB

    print("Prepare data sizes: ", sizes)
    benchmark_data = []
    num = 2
    print("Start to test the bench")
    for size in sizes:
        num_elements = max(1, size // 4)
        send_batch = [
            torch.ones(num_elements, device=device, dtype=torch.float32)
            for _ in range(num)
        ]
        recv_batch = [
            torch.zeros(num_elements, device=device, dtype=torch.float32)
            for _ in range(num)
        ]

        if args.use_gpu:
            torch.cuda.synchronize()
            
        for _ in range(25):
            all_work = []
            reqs = []
            for i in range(num):
                if rank == 0:
                    send_op = dist.isend(send_batch[i], dst=1, group=slime_group)
                    reqs.extend([send_op])
                else:
                    recv_op = dist.irecv(recv_batch[i], src=0, group=slime_group)
                    reqs.extend([recv_op])
            work = reqs
            all_work.extend(work)

            [w.wait() for w in all_work]

        if args.use_gpu:
            torch.cuda.synchronize()
        start_time = time.time()
        
        for _ in range(args.iterations):
            all_work = []
            for i in range(num):
                if rank == 0:
                    send_op = dist.isend(send_batch[i], dst=1, group=slime_group)
                    all_work.extend([send_op])
                else:
                    recv_op = dist.irecv(recv_batch[i], src=0 ,group=slime_group)
                    all_work.extend([recv_op])

            [w.wait() for w in all_work]

        if args.use_gpu:
            torch.cuda.synchronize()
        elapsed_time = time.time() - start_time

        total_data = size * num * args.iterations
        avg_latency = elapsed_time / (num * args.iterations) * 1000  # in ms
        bandwidth = total_data / elapsed_time / 1e6  # MB/s

        if rank == 1:
            benchmark_data.append(
                [
                    f"{size:,}",
                    f"{avg_latency:.3f} ms",
                    f"{bandwidth:.2f} MB/s",
                    "GPU" if args.use_gpu else "CPU",
                ]
            )

    # Print results
    if rank == 1 and benchmark_data:
        headers = ["Message Size (bytes)", "Avg Latency", "Bandwidth", "Device"]
        print("\nBenchmark Results:")
        print(tabulate(benchmark_data, headers=headers, tablefmt="grid"))

    dist.destroy_process_group()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send/Recv Benchmark")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["send", "recv"],
        help="Operation mode: 'send' or 'recv'",
    )
    parser.add_argument(
        "--master-addr",
        type=str,
        default="localhost",
        help="Master address for distributed training",
    )
    parser.add_argument(
        "--master-port",
        type=str,
        default="6008",
        help="Master port for distributed training",
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=None,
        help="List of message sizes in bytes to test",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of iterations for benchmarking",
    )

    parser.add_argument(
        "--use-gpu", action="store_true", help="Use CUDA (Default: use CPU)"
    )

    args = parser.parse_args()

    # Set environment variables
    os.environ["MASTER_ADDR"] = args.master_addr
    os.environ["MASTER_PORT"] = args.master_port

    benchmark_send_recv(args)
