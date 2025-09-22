"""# Remote Read Benchmark

## Node 0
torchrun --master-addr 10.130.8.145 --master-port 6006 \
    --nnodes 2 --nproc-per-node 1 --node-rank 1 bench/python/agg_transfer_bench_spmd.py \
    --qp-num 8 --transfer-engine dlslime --batch-size 94 --num-iteration 10 --num-concurrency 8

## Node 1
torchrun --master-addr 10.130.8.145 --master-port 6006 \
    --nnodes 2 --nproc-per-node 1 --node-rank 0 bench/python/agg_transfer_bench_spmd.py \
    --qp-num 8 --transfer-engine dlslime --batch-size 94 --num-iteration 10 --num-concurrency 8
"""

import argparse
import csv
import os

from typing import List

import numpy as np
import torch
import zmq
from tabulate import tabulate

from dlslime import Assignment, RDMAEndpoint, available_nic

parser = argparse.ArgumentParser()
parser.add_argument('--size', nargs='+', type=int, default=[n for n in range(8, 30)])
parser.add_argument('--target-endpoint', type=str, default='127.0.0.1:6006')
parser.add_argument('--initiator-endpoint', type=str, default='127.0.0.1:6007')
parser.add_argument('--num-concurrency', type=int, default=16)
parser.add_argument('--opcode', type=str, choices=['read', 'write'], default='read')
parser.add_argument('--role', type=str, choices=['initiator', 'target'], default='read')
parser.add_argument('--save-csv', action='store_true', help='Save benchmark results to CSV file')
parser.add_argument('--csv-filename', type=str, default='./output.csv', help='Filename for CSV output')
parser.add_argument('--qp-num', type=int, default=None, help='Queue Pair number for RDMA operations')
parser.add_argument('--num-devices', type=int, default=8, help='num devices')
parser.add_argument('--with-imm-data',
                    action='store_true',
                    help='Use immediate data for write operations (only applicable for write operations)')
parser.add_argument('--full-mesh', action='store_true',  help='full mesh')
parser.add_argument('--transfer-engine', choices=['dlslime', 'mooncake'], type=str)

parser.add_argument('--mooncake-endpoint', type=str, default='127.0.0.1:12001')


args = parser.parse_args()


if args.transfer_engine == 'mooncake':
    from mooncake.engine import TransferEngine as MooncakeTransferEngine


if args.with_imm_data and args.opcode != 'write':
    raise ValueError('Immediate data can only be used with write operations.')

qp_num = args.qp_num if args.qp_num is not None else int(os.getenv('SLIME_QP_NUM', 1))

print(f'mode: RDMA RC {args.opcode}')
print(f'num concurrency: {args.num_concurrency}')

benchmark_data = []

num_devices = args.num_devices

rdma_devices = available_nic()

rdma_endpoint: List[RDMAEndpoint] = []

mooncake_endpoint_info = {
    "kv_table": {},
    "endpoint": []
}

if args.transfer_engine == 'mooncake':
    for i in range(num_devices):
        engine = MooncakeTransferEngine()
        result = engine.initialize(args.mooncake_endpoint, 'P2PHANDSHAKE', 'rdma', rdma_devices[i % len(rdma_devices)])
        mooncake_endpoint_info["endpoint"].append(engine.get_rpc_port())
        rdma_endpoint.append(engine)
else:
    for i in range(num_devices):
        rdma_endpoint.append(RDMAEndpoint(rdma_devices[i % len(rdma_devices)], ib_port=1, link_type='RoCE', qp_num=qp_num))
zmq_ctx = zmq.Context(2)

zmq_recv = zmq_ctx.socket(zmq.PULL)
zmq_send = zmq_ctx.socket(zmq.PUSH)

if args.role == 'target':
    # target endpoint
    zmq_send.bind(f'tcp://{args.target_endpoint}')  # noqa: E231
    zmq_recv.connect(f'tcp://{args.initiator_endpoint}')  # noqa: E231
else:
    # initiator endpoint
    zmq_send.bind(f'tcp://{args.initiator_endpoint}')  # noqa: E231
    zmq_recv.connect(f'tcp://{args.target_endpoint}')  # noqa: E231

ttensors = [[torch.ones([2 << rawsize], device=f"cuda:{i}") for i in range(num_devices)] for rawsize in args.size]
torch.cuda.synchronize()

for i in range(num_devices):
    for idx, ttensor in enumerate(ttensors):
        if not args.full_mesh:
            reg_edpt = [i]
        else:
            reg_edpt = list(range(args.num_devices))
        for j in reg_edpt:
            if args.transfer_engine == 'dlslime':
                rdma_endpoint[j].register_memory_region(
                    f"device_{i}_buffer_{idx}",
                    ttensor[i].data_ptr(),
                    ttensor[i].storage_offset(),
                    ttensor[i].numel() * ttensor[i].itemsize
                )
            else:
                result = rdma_endpoint[j].register_memory(
                    ttensor[i].data_ptr() + ttensor[i].storage_offset(),
                    ttensor[i].numel() * ttensor[i].itemsize
                )
                mooncake_endpoint_info["kv_table"][f"device_{i}_buffer_{idx}"] = (
                    ttensor[i].data_ptr() + ttensor[i].storage_offset(),
                    ttensor[i].numel() * ttensor[i].itemsize
                )
                if result != 0:
                    raise RuntimeError(f'Failed to register memory region: {i, result}')


if args.transfer_engine == 'dlslime':
    zmq_send.send_pyobj([edpt.endpoint_info for edpt in rdma_endpoint])
else:
    zmq_send.send_pyobj(mooncake_endpoint_info)

if args.transfer_engine == 'dlslime':
    remote_info = zmq_recv.recv_pyobj()
    for i, edpt in enumerate(rdma_endpoint):
        edpt.connect(remote_info[i])
else:
    mooncake_remote_endpoint_info = zmq_recv.recv_pyobj()

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = args.num_concurrency

# if args.opcode == 'read':
#     fn = rdma_endpoint.read_batch
# elif args.opcode == 'write' and args.with_imm_data:
#     fn = rdma_endpoint.write_batch_with_imm_data
# elif args.opcode == 'write':
#     fn = rdma_endpoint.write_batch
# else:
#     raise ValueError

for idx, (rawsize, ttensor) in enumerate(zip(args.size, ttensors)):
    size = 2 << rawsize
    total_time = 0.0
    start_event.record()
    for _ in range(100):
        assigns = []
        for assign_id in range(n_runs):
            if args.role == "initiator":
                for i in range(args.num_devices):
                    if not args.full_mesh:
                        trans_device = [i]
                    else:
                        trans_device = list(range(args.num_devices))
                    for tran in trans_device:
                        if args.with_imm_data:
                            assign = rdma_endpoint[i].write_batch_with_imm_data(
                                batch=[
                                    Assignment(
                                        mr_key=f"device_{tran}_buffer_{idx}",
                                        target_offset=0,
                                        source_offset=0,
                                        length=ttensor[i].numel() * ttensor[i].itemsize,
                                )
                                ],
                                qpi=assign_id % qp_num,
                                imm_data=1,
                                async_op=True
                            )
                        else:
                            if args.transfer_engine == "dlslime":
                                fn = rdma_endpoint[i].read_batch if args.opcode == "read" else rdma_endpoint[i].write_batch
                                assign = fn(
                                    batch=[
                                        Assignment(
                                            mr_key=f"device_{tran}_buffer_{idx}",
                                            target_offset=0,
                                            source_offset=0,
                                            length=ttensor[i].numel() * ttensor[i].itemsize,
                                        )
                                    ],
                                    async_op=True)
                            else:
                                result = rdma_endpoint[i].transfer_sync_read(
                                    f"10.130.8.143:{mooncake_remote_endpoint_info['endpoint'][i]}",
                                    mooncake_endpoint_info['kv_table'][f"device_{tran}_buffer_{idx}"][0],
                                    mooncake_remote_endpoint_info['kv_table'][f"device_{tran}_buffer_{idx}"][0],
                                    ttensor[i].numel() * ttensor[i].itemsize,
                                )
                    if args.transfer_engine == 'dlslime':
                        assigns.append(assign)
            else:
                if args.with_imm_data:
                    for i in range(args.num_devices):
                        if not args.full_mesh:
                            trans_device = [i]
                        else:
                            trans_device = list(range(args.num_devices))
                        for tran in trans_device:
                            assign = rdma_endpoint[i].recv_batch(
                                batch=[
                                    Assignment(
                                        mr_key=f"device_{tran}_buffer_{idx}",
                                        target_offset=0,
                                        source_offset=0,
                                        length=ttensor[i].numel() * ttensor[i].itemsize,
                                    )
                                ],
                                qpi=assign_id % qp_num,
                                async_op=True
                            )
                    assigns.append(assign)
        [assign.wait() for assign in assigns]
    end_event.record()
    torch.cuda.synchronize()
    elapsed_time = start_event.elapsed_time(end_event)
    total_time += elapsed_time

    if args.role == "initiator":
        size_bytes = ttensor[0].numel() * ttensor[0].itemsize
        total_transport = n_runs * size * ttensor[0].itemsize * args.num_devices * 100
        if args.full_mesh:
            total_transport *= args.num_devices
        avg_latency = np.mean([assign.latency() for assign in assigns])
        bandwidth = total_transport / total_time / 1e3

        benchmark_data.append([
            f'{size_bytes:,}',  # noqa: E231
            f'{total_transport:,}',  # noqa: E231
            f'{0 * 1000:.2f}',  # noqa: E231
            f'{bandwidth:.2f}'  # noqa: E231
        ])
        print(
            [
                f'{size_bytes:,}',  # noqa: E231
                f'{total_transport:,}',  # noqa: E231
                f'{0 * 1000:.2f}',  # noqa: E231
                f'{bandwidth:.2f}'  # noqa: E231
            ]
        )

if args.role == "target":
    _ = zmq_recv.recv_pyobj()
else:
    headers = [
        'Message Size (bytes)', 'Total Transport (bytes)', 'Target Affinity', 'Initiator Affinity', 'Avg Latency(ms)',
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
    zmq_send.send_pyobj('TERMINATE')

zmq_send.close()
zmq_recv.close()
zmq_ctx.term()
