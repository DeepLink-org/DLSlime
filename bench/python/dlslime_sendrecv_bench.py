import argparse
import os
import time
from tabulate import tabulate

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
    backend = "cuda:dlslime" if args.use_gpu else "cpu:dlslime"
    dist.init_process_group(backend, rank=rank, world_size=2)
    print(backend)
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
        sizes = [2**n for n in range(8, 16)]  # 256B to 256MB

    print("Prepare data sizes: ", sizes)
    benchmark_data = []
    print("Start to test the bench")
    for size in sizes:
        num_elements = max(1, size // 4)
        send_batch = [
            torch.ones(num_elements, device=device, dtype=torch.float32)
            for _ in range(5)
        ]
        recv_batch = [
            torch.zeros(num_elements, device=device, dtype=torch.float32)
            for _ in range(5)
        ]

        # Warmup
        reqs = []
        for i in range(5):
            send_op = distributed_c10d.P2POp(dist.isend, send_batch[i], 1, tag=i)
            recv_op = distributed_c10d.P2POp(dist.irecv, recv_batch[i], 0, tag=i)
            reqs.extend([send_op, recv_op])
        work = distributed_c10d.batch_isend_irecv(reqs)
        [w.wait() for w in work]

        if args.use_gpu:
            torch.cuda.synchronize()
        start_time = time.time()

        all_work = []
        for _ in range(args.iterations):
            reqs = []
            for i in range(5):
                send_op = distributed_c10d.P2POp(dist.isend, send_batch[i], 1, tag=i)
                recv_op = distributed_c10d.P2POp(dist.irecv, recv_batch[i], 0, tag=i)
                reqs.extend([send_op, recv_op])
            work = distributed_c10d.batch_isend_irecv(reqs)
            all_work.append(work)

        for work in all_work:
            [w.wait() for w in work]

        if args.use_gpu:
            torch.cuda.synchronize()
        elapsed_time = time.time() - start_time

        total_data = size * 5 * 2 * args.iterations
        avg_latency = elapsed_time / (5 * args.iterations) * 1000  # in ms
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
        default="6006",
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
        default=10,
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
