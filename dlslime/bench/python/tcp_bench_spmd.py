"""# Remote Read Benchmark

## Node 0
torchrun --master-addr 10.130.8.145 --master-port 6006 \
    --nnodes 2 --nproc-per-node 1 --node-rank 0 bench/python/tcp_spmd.py \
    --transfer-engine dlslime --batch-size 94 --num-iteration 10 --num-concurrency 8

## Node 1
torchrun --master-addr 10.130.8.145 --master-port 6006 \
    --nnodes 2 --nproc-per-node 1 --node-rank 1 bench/python/tcp_spmd.py \
    --transfer-engine dlslime --batch-size 94 --num-iteration 10 --num-concurrency 8
"""

import argparse
import csv
import os
import socket

import torch
import torch.distributed as dist
from tabulate import tabulate
from torch.distributed import distributed_c10d

parser = argparse.ArgumentParser()
parser.add_argument("--batch-size", type=int, default=1)
parser.add_argument("--size", nargs="+", type=int, default=[n for n in range(8, 20)])
parser.add_argument("--num-concurrency", type=int, default=16)
parser.add_argument("--num-iteration", type=int, default=100)
parser.add_argument("--num-warmup-iteration", type=int, default=10)
parser.add_argument("--opcode", type=str, choices=["read", "write"], default="write")
parser.add_argument(
    "--save-csv", action="store_true", help="Save benchmark results to CSV file"
)
parser.add_argument(
    "--csv-filename", type=str, default="./output.csv", help="Filename for CSV output"
)
parser.add_argument(
    "--transfer-engine",
    choices=["dlslime", "mooncake"],
    type=str,
    default="dlslime",
)

args = parser.parse_args()


def set_env_when_no_default(env_name, value):
    env_value = os.environ.get(env_name, value) or value
    os.environ[env_name] = env_value
    return env_value


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()
    return local_ip


local_ip = get_local_ip()

# Get SPMD Info
rank = int(os.environ["RANK"])
local_rank = int(os.environ["LOCAL_RANK"])
world_size = int(os.environ["WORLD_SIZE"])
local_world_size = nnodes = int(os.environ["LOCAL_WORLD_SIZE"])
master_addr = os.environ["MASTER_ADDR"]
master_port = os.environ["MASTER_PORT"]
npros_per_rank = local_world_size
assert world_size % 2 == 0
num_channels = world_size // 2
# target rank for initiator rank
peer_rank = (rank + num_channels) % world_size

if rank < num_channels:
    role = "initiator"
else:
    role = "target"


def rank_0_print(*args):
    if rank == 0:
        print(*args)


rank_0_print(
    f"rank [{0}, {world_size // 2}) for initiator, "
    f"rank [{world_size // 2}, {world_size}) for target."
)
rank_0_print(
    f"{rank=}, {peer_rank=}, {world_size=}, {npros_per_rank=}, {master_addr=}, {master_port=}"
)
rank_0_print(f"Local_ip: {local_ip}")
rank_0_print(f"mode: {args.opcode}")
rank_0_print(f"batch size: {args.batch_size}")
rank_0_print(f"num concurrency: {args.num_concurrency}")

rank_0_print(f"benchmarking transfer engine: {args.transfer_engine}")
# import Python Package
if args.transfer_engine == "dlslime":
    import dlslime
    from dlslime import TcpEndpoint

elif args.transfer_engine == "mooncake":
    from mooncake.engine import TransferEngine as MooncakeTransferEngine

dist.init_process_group("cpu:gloo,cuda:nccl")
transfer_group = dist.new_group(list(range(world_size)), backend="cuda:nccl")

# TODO: for AFD Benchmark
initiator_group = dist.new_group(list(range(num_channels)), backend="cpu:gloo")
target_group = dist.new_group(list(range(num_channels, world_size)), backend="cpu:gloo")

# Setting Info
if args.transfer_engine == "mooncake":
    mooncake_endpoint_info = {"local_ip": local_ip, "kv_table": {}, "endpoint": []}

if args.opcode != "write":
    raise ValueError("Immediate data can only be used with write operations.")

if args.transfer_engine == "dlslime":
    tcp_endpoint = TcpEndpoint(f"{local_ip}", 22500 + local_rank)
elif args.transfer_engine == "mooncake":
    engine = MooncakeTransferEngine()
    result = engine.initialize(
        f"{local_ip}:{22500+local_rank}", "P2PHANDSHAKE", "tcp", None
    )
    mooncake_endpoint_info = {
        "local_ip": local_ip,
        "kv_table": {},
        "endpoint": engine.get_rpc_port(),
    }
    tcp_endpoint = engine

torch.cuda.set_device(local_rank)

max_numel = 2 << max(args.size)
max_ttensor = torch.ones([max_numel], device=f"cuda:{local_rank}")
ttensors = [max_ttensor[: 2 << rawsize] for rawsize in args.size]
max_mr_key = 0
dlslime_mr_name = "bench_mr"  # named MR for the DLSlime handle model
dlslime_local_handle = None
dlslime_remote_handle = None
print(local_rank)
torch.cuda.synchronize()

if args.transfer_engine == "dlslime":
    dlslime_local_handle = tcp_endpoint.register_memory_region(
        dlslime_mr_name,
        max_ttensor.data_ptr(),
        int(max_ttensor.storage_offset()),
        max_ttensor.numel() * max_ttensor.itemsize,
    )
elif args.transfer_engine == "mooncake":
    result = tcp_endpoint.register_memory(
        max_ttensor.data_ptr() + max_ttensor.storage_offset(),
        max_ttensor.numel() * max_ttensor.itemsize,
    )
    if result != 0:
        raise RuntimeError(f"Failed to register memory region: {result}")
    mooncake_endpoint_info["kv_table"][max_mr_key] = (
        max_ttensor.data_ptr() + max_ttensor.storage_offset(),
        max_ttensor.numel() * max_ttensor.itemsize,
    )


if rank == 0:
    print("exchanging endpoint info... ")
all_endpoint_info = [{} for _ in range(world_size)]
if args.transfer_engine == "dlslime":
    dist.all_gather_object(all_endpoint_info, tcp_endpoint.endpoint_info())
elif args.transfer_engine == "mooncake":
    dist.all_gather_object(all_endpoint_info, mooncake_endpoint_info)

if rank == 0:
    print("endpoint exchanged")

if args.transfer_engine == "dlslime":
    # endpoint connect
    tcp_endpoint.connect(all_endpoint_info[(rank + num_channels) % world_size])
    # Resolve the peer's published MR to a local remote-handle. Both sides
    # register so either can initiate read/write; only the initiator role
    # actually uses the handle in this benchmark.
    peer_mr_info = all_endpoint_info[peer_rank]["mr_info"][dlslime_mr_name]
    dlslime_remote_handle = tcp_endpoint.register_remote_memory_region(
        dlslime_mr_name, peer_mr_info
    )
elif args.transfer_engine == "mooncake":
    # construct connect lazily
    pass

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)


def transfer_batch_concurrency_dlslime(
    role, opcode, local_handle, remote_handle, tensor, batch_size, num_concurrency
):
    fn = tcp_endpoint.read if opcode == "read" else tcp_endpoint.write
    if role == "initiator":
        slots = []
        for concurrent_id in range(num_concurrency):
            assign = [
                fn(
                    [
                        (
                            local_handle,
                            remote_handle,
                            0,
                            0,
                            tensor.numel() * tensor.itemsize,
                        )
                        for _ in range(batch_size)
                    ],
                )
            ]
            slots.extend(assign)

        for slot in slots:
            slot.wait()


def transfer_batch_concurrency_mooncake(
    role, opcode, mr_key, tensor, batch_size, num_concurrency
):
    print(f"tcp_endpoint:{tcp_endpoint}")
    # assert opcode == 'read'
    if role == "initiator":
        all_batch_ids_to_wait = []
        for concurrent_id in range(num_concurrency):
            batch_id = tcp_endpoint.batch_transfer_async_write(
                f"{all_endpoint_info[peer_rank]['local_ip']}:{all_endpoint_info[peer_rank]['endpoint']}",
                [
                    all_endpoint_info[rank]["kv_table"][mr_key][0]
                    for _ in range(batch_size)
                ],
                [
                    all_endpoint_info[peer_rank]["kv_table"][mr_key][0]
                    for _ in range(batch_size)
                ],
                [tensor.numel() * tensor.itemsize for _ in range(batch_size)],
            )
            if batch_id == 0:
                print("error for transport")
            all_batch_ids_to_wait.append(batch_id)
        result = tcp_endpoint.get_batch_transfer_status(all_batch_ids_to_wait)
        if result != 0:
            print(f"transport failure, batch IDs: {all_batch_ids_to_wait}")


n_runs = args.num_concurrency
benchmark_data = []
for idx, (rawsize, ttensor) in enumerate(zip(args.size, ttensors)):
    rank_0_print(f"benchmark s={ttensor.numel() * ttensor.itemsize / 1024}K")
    size = 2 << rawsize
    total_time = 0.0

    def _run_one_iteration():
        if args.transfer_engine == "dlslime":
            transfer_batch_concurrency_dlslime(
                role,
                args.opcode,
                dlslime_local_handle,
                dlslime_remote_handle,
                ttensor,
                args.batch_size,
                args.num_concurrency,
            )
        elif args.transfer_engine == "mooncake":
            transfer_batch_concurrency_mooncake(
                role,
                args.opcode,
                max_mr_key,
                ttensor,
                args.batch_size,
                args.num_concurrency,
            )
        torch.cuda.synchronize()

    # Warmup (excluded from timing). Barrier after so all ranks start the
    # timed region together and the first measured iteration isn't paying
    # cold-start cost.
    for _ in range(args.num_warmup_iteration):
        _run_one_iteration()
    torch.cuda.synchronize()
    dist.barrier()

    start_event.record()
    for iter_id in range(args.num_iteration):
        _run_one_iteration()
    end_event.record()
    torch.cuda.synchronize()
    dist.barrier()
    elapsed_time = start_event.elapsed_time(end_event)
    total_time += elapsed_time

    if rank < num_channels:
        size_bytes = ttensor.numel() * ttensor.itemsize
        total_transport = (
            n_runs * size * ttensor.itemsize * args.num_iteration * args.batch_size
        )
        avg_latency = total_time / args.num_iteration / n_runs

        bandwidth = torch.tensor(total_transport / total_time / 1e3)
        dist.all_reduce(bandwidth, group=initiator_group)
        bandwidth = int(bandwidth)

        benchmark_data.append(
            [
                args.transfer_engine,
                num_channels,
                f"{size_bytes:,}",  # noqa: E231
                f"{args.batch_size}",  # noqa: E231
                f"{args.num_concurrency}",  # noqa: E231
                f"{total_transport:,}",  # noqa: E231
                f"{avg_latency:.3f}",  # noqa: E231
                f"{bandwidth:.3f}",  # noqa: E231
            ]
        )

        rank_0_print(
            [
                args.transfer_engine,
                num_channels,
                f"{size_bytes:,}",  # noqa: E231
                f"{args.batch_size}",  # noqa: E231
                f"{args.num_concurrency}",  # noqa: E231
                f"{total_transport:,}",  # noqa: E231
                f"{avg_latency:.3f}",  # noqa: E231
                f"{bandwidth:.3f}",  # noqa: E231
            ]
        )

dist.barrier()

if rank == 0:
    headers = [
        "Transfer Engine",
        "#Channels",
        "Message Size (bytes)",
        "Batch Size",
        "Num Concurrency",
        "Total Transport (bytes)",
        "Avg Latency(ms)",
        "Bandwidth(MB/s)",
    ]
    print("\nBenchmark Results:")
    print(tabulate(benchmark_data, headers=headers, tablefmt="github"))
    if args.save_csv:
        with open(args.csv_filename, "w", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(headers)
            writer.writerows(benchmark_data)
        print(f"CSV saved to {args.csv_filename}")

dist.destroy_process_group()
