"""# Send/Recv Benchmark

# One Node
torchrun \
    --nproc-per-node 2 \
    --nnodes=1 \
    --node_rank=0 \
    --master_port=$MASTER_PORT \
    bench/python/transfer_bench.py

# Cross Node
## Node 0
torchrun \
    --nproc-per-node 1 \
    --nnodes=2 \
    --node_rank=0 \
    --master-addr=$MASTER_ADDR \
    --master_port=$MASTER_PORT \
    bench/python/transfer_bench.py

## Node 1
torchrun \
    --nproc-per-node 1 \
    --nnodes=2 \
    --node_rank=1 \
    --master-addr=$MASTER_ADDR \
    --master_port=$MASTER_PORT \
    bench/python/transfer_bench.py
"""

import argparse
import os

import torch
import zmq

from dlslime import Assignment, RDMAEndpoint, available_nic

parser = argparse.ArgumentParser()
parser.add_argument('--size', nargs='+', type=int, default=[2 << n for n in range(8, 27)])
parser.add_argument('--target-endpoint', type=str, default='127.0.0.1:6006')
parser.add_argument('--initiator-endpoint', type=str, default='127.0.0.1:6007')
parser.add_argument('--num-concurrency', type=int, default=80)

args = parser.parse_args()

rank = int(os.environ['RANK'])
world_size = int(os.environ['WORLD_SIZE'])
master_addr = os.environ['MASTER_ADDR']
master_port = os.environ['MASTER_PORT']

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

torch.cuda.set_device(rank % world_size)
ttensors = [torch.ones([size]) for size in args.size]

for idx, ttensor in enumerate(ttensors):
    rdma_endpoint.register_memory_region(str(idx), ttensor.data_ptr(), ttensor.storage_offset(),
                                         ttensor.numel() * ttensor.itemsize)

zmq_send.send_pyobj(rdma_endpoint.endpoint_info)
remote_info = zmq_recv.recv_pyobj()
rdma_endpoint.connect(remote_info)

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = args.num_concurrency

for idx, ttensor in enumerate(ttensors):
    total_time = 0.0
    assigns = []
    start_event.record()
    for _ in range(n_runs):
        if rank == 1:
            assign = rdma_endpoint.read_batch([
                Assignment(mr_key=str(idx), target_offset=0, source_offset=0, length=ttensor.numel() * ttensor.itemsize)
            ],
                                              async_op=True)
            assigns.append(assign)
    [assign.wait() for assign in assigns]
    end_event.record()
    torch.cuda.synchronize()
    elapsed_time = start_event.elapsed_time(end_event)
    total_time += elapsed_time

    if rank == 1:
        print(f'size: {ttensor.numel() * ttensor.itemsize}')
        print(f'total transport: {n_runs * ttensor.numel() * ttensor.itemsize}')
        print(f'average latency: {total_time / n_runs}ms')
        print(f'bw: {n_runs * ttensor.numel() * ttensor.itemsize / total_time / 1e3} MBps')

if rank == 0:
    _ = zmq_recv.recv_pyobj()
else:
    zmq_send.send_pyobj('TERMINATE')
