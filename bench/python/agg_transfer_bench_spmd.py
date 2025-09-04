import os
from typing import List

import socket

import csv
from tabulate import tabulate

import torch
import torch.distributed as dist

from dlslime import Assignment, RDMAEndpoint, available_nic

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--size', nargs='+', type=int, default=[n for n in range(8, 27)])
parser.add_argument('--num-concurrency', type=int, default=16)
parser.add_argument('--opcode', type=str, choices=['read', 'write'], default='read')
parser.add_argument('--with-imm-data', action='store_true', help='with-imm-data')
parser.add_argument('--save-csv', action='store_true', help='Save benchmark results to CSV file')
parser.add_argument('--csv-filename', type=str, default='./output.csv', help='Filename for CSV output')
parser.add_argument('--qp-num', type=int, default=None, help='Queue Pair number for RDMA operations')
parser.add_argument('--transfer-engine', choices=['dlslime', 'mooncake'], type=str, default='dlslime')


args = parser.parse_args()


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))  # 连接 Google DNS
    local_ip = s.getsockname()[0]
    s.close()
    return local_ip


local_ip = get_local_ip()


rank = int(os.environ["RANK"])
world_size = int(os.environ["WORLD_SIZE"])
master_addr = os.environ["MASTER_ADDR"]
master_port = os.environ["MASTER_PORT"]

assert world_size % 2 == 0
num_channels = world_size // 2

if rank == 0:
    print(rank, world_size, master_addr, master_port)

peer_rank = (rank + num_channels) % world_size

dist.init_process_group("cpu:gloo,cuda:nccl")

initiator_group = dist.new_group(list(range(num_channels)), backend="cpu:gloo,cuda:nccl")
target_group = dist.new_group(list(range(num_channels, world_size)), backend="cpu:gloo,cuda:nccl")

if args.transfer_engine == 'mooncake':
    from mooncake.engine import TransferEngine as MooncakeTransferEngine

if args.with_imm_data and args.opcode != 'write':
    raise ValueError('Immediate data can only be used with write operations.')

qp_num = args.qp_num if args.qp_num is not None else int(os.getenv('SLIME_QP_NUM', 1))

if rank == 0:
    print(f'Local_ip: {local_ip}')
    print(f'mode: RDMA RC {args.opcode}')
    print(f'num concurrency: {args.num_concurrency}')
    print(f'qp num: {args.qp_num}')

benchmark_data = []

rdma_devices = available_nic()

mooncake_endpoint_info = {
    "local_ip": local_ip,
    "kv_table": {},
    "endpoint": []
}

rdma_device = rdma_devices[rank%len(rdma_devices)]
if args.transfer_engine == 'mooncake':
    engine = MooncakeTransferEngine()
    result = engine.initialize(f"{local_ip}:12001", 'P2PHANDSHAKE', 'rdma', rdma_device)
    mooncake_endpoint_info["endpoint"] = engine.get_rpc_port()
    rdma_endpoint = engine
else:
    rdma_endpoint = RDMAEndpoint(rdma_device, ib_port=1, link_type='RoCE', qp_num=qp_num)

torch.cuda.set_device(rank % 8)

ttensors = [torch.ones([2 << rawsize], device=f"cuda") for rawsize in args.size]
torch.cuda.synchronize()

for idx, ttensor in enumerate(ttensors):
    if args.transfer_engine == 'dlslime':
        rdma_endpoint.register_memory_region(
            f"buffer_{idx}",
            ttensor.data_ptr(),
            ttensor.storage_offset(),
            ttensor.numel() * ttensor.itemsize
        )
    else:
        result = rdma_endpoint.register_memory(
            ttensor.data_ptr() + ttensor.storage_offset(),
            ttensor.numel() * ttensor.itemsize
        )
        mooncake_endpoint_info["kv_table"][f"buffer_{idx}"] = (
            ttensor.data_ptr() + ttensor.storage_offset(),
            ttensor.numel() * ttensor.itemsize
        )
        if result != 0:
            raise RuntimeError(f'Failed to register memory region: {result}')

all_endpoint_info = [{} for _ in range(world_size)]

if args.transfer_engine == 'dlslime':
    dist.all_gather_object(all_endpoint_info, rdma_endpoint.endpoint_info)
else:
    dist.all_gather_object(all_endpoint_info, mooncake_endpoint_info)

if args.transfer_engine == 'dlslime':
    # endpoint connect
    rdma_endpoint.connect(all_endpoint_info[(rank + num_channels) % world_size])

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = args.num_concurrency

for idx, (rawsize, ttensor) in enumerate(zip(args.size, ttensors)):
    size = 2 << rawsize
    total_time = 0.0
    start_event.record()
    for _ in range(100):
        assigns = []
        all_batch_ids_to_wait = []
        for assign_id in range(n_runs):
            if rank < num_channels:
                if args.with_imm_data:
                    assign = rdma_endpoint.write_batch_with_imm_data(
                        batch=[
                            Assignment(
                                mr_key=f"buffer_{idx}",
                                target_offset=0,
                                source_offset=0,
                                length=ttensor.numel() * ttensor.itemsize,
                        )
                        ],
                        qpi=assign_id % qp_num,
                        imm_data=1,
                        async_op=True
                    )
                else:
                    if args.transfer_engine == "dlslime":
                        fn = rdma_endpoint.read_batch if args.opcode == "read" else rdma_endpoint.write_batch
                        if n_runs == 1:
                            # split to 2 for multi_qp
                            assign = [
                                fn(
                                    batch=[
                                        Assignment(
                                            mr_key=f"buffer_{idx}",
                                            target_offset=0,
                                            source_offset=0,
                                            length=ttensor.numel() * ttensor.itemsize // 2,
                                        )
                                    ],
                                    async_op=True
                                ),
                                fn(
                                    batch=[
                                        Assignment(
                                            mr_key=f"buffer_{idx}",
                                            target_offset=ttensor.numel() * ttensor.itemsize // 2,
                                            source_offset=ttensor.numel() * ttensor.itemsize // 2,
                                            length=ttensor.numel() * ttensor.itemsize // 2,
                                        )
                                    ],
                                    async_op=True
                                )
                            ]
                        else:
                            assign = [
                                fn(
                                    batch=[
                                        Assignment(
                                            mr_key=f"buffer_{idx}",
                                            target_offset=0,
                                            source_offset=0,
                                            length=ttensor.numel() * ttensor.itemsize,
                                        )
                                    ],
                                    async_op=True
                                )
                            ]
                    else:
                        batch_id = rdma_endpoint.batch_transfer_async_read(
                            f"{all_endpoint_info[peer_rank]['local_ip']}:{all_endpoint_info[peer_rank]['endpoint']}",
                            [all_endpoint_info[rank]['kv_table'][f"buffer_{idx}"][0]],
                            [all_endpoint_info[peer_rank]['kv_table'][f"buffer_{idx}"][0]],
                            [ttensor.numel() * ttensor.itemsize,]
                        )
                        if batch_id == 0:
                            print(f"error for transport")
                if args.transfer_engine == 'dlslime':
                    assigns.extend(assign)
                elif args.transfer_engine == 'mooncake':
                    all_batch_ids_to_wait.append(batch_id)

            else:
                if args.with_imm_data:
                    assign = rdma_endpoint.recv_batch(
                        batch=[
                            Assignment(
                                mr_key=f"buffer_{idx}",
                                target_offset=0,
                                source_offset=0,
                                length=ttensor.numel() * ttensor.itemsize,
                            )
                        ],
                        qpi=assign_id % qp_num,
                        async_op=True
                    )
                    assigns.append(assign)
        if args.transfer_engine == 'dlslime':
            [assign.wait() for assign in assigns]
        elif args.transfer_engine == 'mooncake':
            result = rdma_endpoint.get_batch_transfer_status(all_batch_ids_to_wait)
            if result != 0:
                print(f"transport failure, batch IDs: {all_batch_ids_to_wait}")

    end_event.record()
    torch.cuda.synchronize()
    dist.barrier()
    elapsed_time = start_event.elapsed_time(end_event)
    total_time += elapsed_time

    if rank < num_channels:
        size_bytes = ttensor.numel() * ttensor.itemsize
        total_transport = n_runs * size * ttensor.itemsize * 100
        avg_latency = total_time / 100 / n_runs

        bandwidth = torch.tensor(total_transport / total_time / 1e3)
        dist.all_reduce(bandwidth, group=initiator_group)
        bandwidth = int(bandwidth)

        benchmark_data.append([
            rank,
            f'{size_bytes:,}',  # noqa: E231
            f'{total_transport:,}',  # noqa: E231
            str(0),
            str(0),
            f'{avg_latency:.3f}',  # noqa: E231
            f'{bandwidth:.3f}'  # noqa: E231
        ])
        if rank == 0:
            print(
                [
                    rank,
                    f'{size_bytes:,}',  # noqa: E231
                    f'{total_transport:,}',  # noqa: E231
                    str(0),
                    str(0),
                    f'{avg_latency:.3f}',  # noqa: E231
                    f'{bandwidth:.3f}'  # noqa: E231
                ]
            )

dist.barrier()

if rank == 0:
    headers = [
        'rank', 'Message Size (bytes)', 'Total Transport (bytes)', 'Target Affinity', 'Initiator Affinity', 'Avg Latency(ms)',
        'Bandwidth(MB/s)'
    ]
    print('\nBenchmark Results:')
    print(tabulate(benchmark_data, headers=headers, tablefmt='grid'))
    if args.save_csv:
        with open(args.csv_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(headers)
            writer.writerows(benchmark_data)
        print(f'CSV saved to {args.csv_filename}')


dist.destroy_process_group()