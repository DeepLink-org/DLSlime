"""# Remote Read Benchmark

# One Node: LocalHost Test
## Process 0:
python3 bench/python/transfer_bench.py --rank 0

## Process 1:
python3 bench/python/transfer_bench.py --rank 1

# Cross Node
## Node 0
python3 bench/python/transfer_bench.py --rank 0 \
    --target-endpoint ${NODE_0_IP:PORT} \
    --initiator-endpoint ${NODE_1_IP:PORT}

## Node 1
python3 bench/python/transfer_bench.py --rank 1 \
    --target-endpoint ${NODE_0_IP:PORT} \
    --initiator-endpoint ${NODE_1_IP:PORT}
"""

import argparse
import csv

import numpy as np
import torch
import zmq
from tabulate import tabulate

from dlslime import Assignment, RDMAEndpoint, available_nic

parser = argparse.ArgumentParser()
parser.add_argument('--rank', type=int, required=True, help='Rank of the current process (0 or 1)')
parser.add_argument('--world-size', type=int, choices=[2], default=2, help='World size (must be 2)')
parser.add_argument('--size', nargs='+', type=int, default=[n for n in range(8, 25)])
parser.add_argument('--target-endpoint', type=str, default='127.0.0.1:6006')
parser.add_argument('--initiator-endpoint', type=str, default='127.0.0.1:6007')
parser.add_argument('--target-affi', type=int, default=0, help='CUDA device ID for target (rank 0)')
parser.add_argument('--initiator-affi', type=int, default=1, help='CUDA device ID for initiator (rank 1)')
parser.add_argument('--num-concurrency', type=int, default=16)
parser.add_argument('--opcode', type=str, choices=['read', 'write'], default='read')
parser.add_argument('--save-csv', action='store_true', help='Save benchmark results to CSV file')
parser.add_argument('--csv-filename', type=str, default='./output.csv', help='Filename for CSV output')

args = parser.parse_args()

print(f'mode: RDMA RC {args.opcode}')
print(f'num concurrency: {args.num_concurrency}')

benchmark_data = []
rank = args.rank
world_size = args.world_size
assert world_size == 2

rdma_devices = available_nic()
rdma_endpoint = RDMAEndpoint(rdma_devices[rank % len(rdma_devices)], ib_port=1, link_type='RoCE')
zmq_ctx = zmq.Context(2)

zmq_recv = zmq_ctx.socket(zmq.PULL)
zmq_send = zmq_ctx.socket(zmq.PUSH)

if rank == 0:
    # target endpoint
    zmq_send.bind(f'tcp://{args.target_endpoint}')
    zmq_recv.connect(f'tcp://{args.initiator_endpoint}')
else:
    # initiator endpoint
    zmq_send.bind(f'tcp://{args.initiator_endpoint}')
    zmq_recv.connect(f'tcp://{args.target_endpoint}')

if rank == 0:
    target_device = args.target_affi
    torch.cuda.set_device(target_device)
else:
    initiator_device = args.initiator_affi
    torch.cuda.set_device(initiator_device)
ttensors = [torch.ones([2 << rawsize]).cuda() for rawsize in args.size]
torch.cuda.synchronize()

for idx, ttensor in enumerate(ttensors):
    rdma_endpoint.register_memory_region(str(idx), ttensor.data_ptr(), ttensor.storage_offset(),
                                         ttensor.numel() * ttensor.itemsize)

zmq_send.send_pyobj(rdma_endpoint.endpoint_info)
remote_info = zmq_recv.recv_pyobj()
rdma_endpoint.connect(remote_info)

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = args.num_concurrency

if args.opcode == 'read':
    fn = rdma_endpoint.read_batch
elif args.opcode == 'write':
    fn = rdma_endpoint.write_batch
else:
    raise ValueError

for idx, (rawsize, ttensor) in enumerate(zip(args.size, ttensors)):
    size = 2 << rawsize
    total_time = 0.0
    start_event.record()
    for _ in range(100):
        assigns = []
        for _ in range(n_runs):
            if rank == 1:
                assign = fn([
                    Assignment(
                        mr_key=str(idx), target_offset=0, source_offset=0, length=ttensor.numel() * ttensor.itemsize)
                ],
                            async_op=True)
                assigns.append(assign)
        [assign.wait() for assign in assigns]
    end_event.record()
    torch.cuda.synchronize()
    elapsed_time = start_event.elapsed_time(end_event)
    total_time += elapsed_time

    if rank == 1:
        size_bytes = ttensor.numel() * ttensor.itemsize
        total_transport = n_runs * size * ttensor.itemsize
        avg_latency = np.mean([assign.latency() for assign in assigns])
        bandwidth = n_runs * size * ttensor.itemsize * 100 / total_time / 1e3

        benchmark_data.append([
            f'{size_bytes:,}', f'{total_transport:,}',
            str(args.target_affi),
            str(args.initiator_affi), f'{avg_latency.total_seconds() * 1000:.2f}', f'{bandwidth:.2f}'
        ])

if rank == 0:
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
            writer.writerow(headers)
            writer.writerows(benchmark_data)
        print(f'CSV saved to {args.csv_filename}')
    zmq_send.send_pyobj('TERMINATE')

zmq_send.close()
zmq_recv.close()
zmq_ctx.term()
