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
import socket

import torch
import torch.distributed as dist
from tabulate import tabulate
from torch.distributed import distributed_c10d

from dlslime import _slime_torch

parser = argparse.ArgumentParser()
parser.add_argument('--batch-size', type=int, default=1)
parser.add_argument('--size', nargs='+', type=int, default=[n for n in range(8, 20)])
parser.add_argument('--num-concurrency', type=int, default=16)
parser.add_argument('--num-iteration', type=int, default=100)
parser.add_argument('--opcode', type=str, choices=['read', 'write'], default='write')
parser.add_argument('--with-imm-data', action='store_true', help='with-imm-data')
parser.add_argument('--save-csv', action='store_true', help='Save benchmark results to CSV file')
parser.add_argument('--csv-filename', type=str, default='./output.csv', help='Filename for CSV output')
parser.add_argument('--qp-num', type=int, default=None, help='Queue Pair number for RDMA operations')
parser.add_argument('--transfer-engine', choices=['dlslime', 'mooncake', 'nixl', 'nccl'], type=str, default='dlslime')
parser.add_argument('--nixl-port', default=5555, type=int)

args = parser.parse_args()


def set_env_when_no_default(env_name, value):
    env_value = os.environ.get(env_name, value) or value
    os.environ[env_name] = env_value
    return env_value


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    local_ip = s.getsockname()[0]
    s.close()
    return local_ip


local_ip = get_local_ip()

# Get SPMD Info
rank = int(os.environ['RANK'])
local_rank = int(os.environ['LOCAL_RANK'])
world_size = int(os.environ['WORLD_SIZE'])
local_world_size = nnodes = int(os.environ['LOCAL_WORLD_SIZE'])
master_addr = os.environ['MASTER_ADDR']
master_port = os.environ['MASTER_PORT']
npros_per_rank = local_world_size
assert world_size % 2 == 0
num_channels = world_size // 2
# target rank for initiator rank
peer_rank = (rank + num_channels) % world_size

if rank < num_channels:
    role = 'initiator'
else:
    role = 'target'


def rank_0_print(*args):
    if rank == 0:
        print(*args)


rank_0_print(f'rank [{0}, {world_size // 2}) for initiator, '
             f'rank [{world_size // 2}, {world_size}) for target.')
rank_0_print(f'{rank=}, {peer_rank=}, {world_size=}, {npros_per_rank=}, {master_addr=}, {master_port=}')
rank_0_print(f'Local_ip: {local_ip}')
rank_0_print(f'mode: RDMA RC {args.opcode}')
rank_0_print(f'batch size: {args.batch_size}')
rank_0_print(f'num concurrency: {args.num_concurrency}')

rank_0_print(f'benchmarking transfer engine: {args.transfer_engine}')
# import Python Package
if args.transfer_engine == 'dlslime':
    if args.num_concurrency == 1:
        SLIME_AGG_QP_NUM = set_env_when_no_default('SLIME_AGG_QP_NUM', str(1))
        rank_0_print('[hint] SLIME_AGG_QP_NUM is set to None, '
                     'for high performance when args.num_concurrency=1, '
                     f'set {SLIME_AGG_QP_NUM=}.')
    qp_num = args.qp_num if args.qp_num is not None else int(os.getenv('SLIME_QP_NUM', 1))
    rank_0_print(f'slime qp num: {args.qp_num}')

    import dlslime
    from dlslime import RDMAEndpoint, available_nic

elif args.transfer_engine == 'nccl':
    NCCL_P2P_DISABLE = set_env_when_no_default('NCCL_P2P_DISABLE', str(1))
    NCCL_P2P_NET_CHUNKSIZE = set_env_when_no_default('NCCL_P2P_NET_CHUNKSIZE', str(8))
    NCCL_SHM_DISABLE = set_env_when_no_default('NCCL_SHM_DISABLE', str(1))
    NCCL_BUFFSIZE = set_env_when_no_default('NCCL_BUFFSIZE', str(8388606))
    NCCL_IB_QPS_PER_CONNECTION = set_env_when_no_default('NCCL_IB_QPS_PER_CONNECTION', str(8))

    print('To use RDMA Transport, when transfer engine is NCCL, we set '
          f'{NCCL_P2P_DISABLE=}, {NCCL_P2P_NET_CHUNKSIZE=}, '
          f'{NCCL_SHM_DISABLE=}, {NCCL_BUFFSIZE=}, '
          f'{NCCL_IB_QPS_PER_CONNECTION=}.')
elif args.transfer_engine == 'mooncake':
    # TODO: remove dlslime dependency
    from mooncake.engine import TransferEngine as MooncakeTransferEngine

    from dlslime import available_nic

dist.init_process_group('cpu:gloo,cuda:nccl')
transfer_group = dist.new_group(list(range(world_size)), backend='cuda:nccl')

# TODO: for AFD Benchmark
initiator_group = dist.new_group(list(range(num_channels)), backend='cpu:gloo,cuda:dlslime')
target_group = dist.new_group(list(range(num_channels, world_size)), backend='cpu:gloo,cuda:dlslime')

# Setting Info
if args.transfer_engine == 'mooncake':
    mooncake_endpoint_info = {'local_ip': local_ip, 'kv_table': {}, 'endpoint': []}
elif args.transfer_engine == 'nixl':
    from nixl._api import nixl_agent, nixl_agent_config
    nixl_endpoint_info = {'local_ip': local_ip, 'kv_table': {}}

if args.with_imm_data and args.opcode != 'write':
    raise ValueError('Immediate data can only be used with write operations.')

if args.transfer_engine in ['dlslime', 'mooncake']:
    rdma_devices = available_nic()
    rdma_device = rdma_devices[local_rank % len(rdma_devices)]
    print(f'setting NIC for {rdma_device}')

if args.transfer_engine == 'dlslime':
    rdma_endpoint = RDMAEndpoint(rdma_device, ib_port=1, link_type='RoCE', qp_num=qp_num)
elif args.transfer_engine == 'mooncake':
    engine = MooncakeTransferEngine()
    result = engine.initialize(f'{local_ip}:12001', 'P2PHANDSHAKE', 'rdma', rdma_device)
    mooncake_endpoint_info['endpoint'] = engine.get_rpc_port()
    rdma_endpoint = engine
elif args.transfer_engine == 'nixl':
    if role == 'initiator':
        config = nixl_agent_config(True, True, args.nixl_port + rank)
    else:
        config = nixl_agent_config(True, True, args.nixl_port + rank)
    agent = nixl_agent(role, config)

torch.cuda.set_device(local_rank)

ttensors = [torch.ones([2 << rawsize], device=f'cuda:{local_rank}') for rawsize in args.size]
print(local_rank)
torch.cuda.synchronize()

if args.transfer_engine == 'dlslime':
    for idx, ttensor in enumerate(ttensors):
        rdma_endpoint.register_memory_region(idx, ttensor.data_ptr(), ttensor.storage_offset(),
                                             ttensor.numel() * ttensor.itemsize)
elif args.transfer_engine == 'mooncake':
    for idx, ttensor in enumerate(ttensors):
        result = rdma_endpoint.register_memory(ttensor.data_ptr() + ttensor.storage_offset(),
                                               ttensor.numel() * ttensor.itemsize)
        mooncake_endpoint_info['kv_table'][idx] = (ttensor.data_ptr() + ttensor.storage_offset(),
                                                               ttensor.numel() * ttensor.itemsize)
        if result != 0:
            raise RuntimeError(f'Failed to register memory region: {result}')
elif args.transfer_engine == 'nixl':
    # adapted from: https://github.com/ai-dynamo/nixl/blob/main/examples/python/blocking_send_recv_example.py
    nixl_memory_info = []
    for idx, ttensor in enumerate(ttensors):
        nixl_memory_info.append(
            (ttensor.data_ptr() + ttensor.storage_offset(), ttensor.numel() * ttensor.itemsize, local_rank, ''))
        nixl_endpoint_info['kv_table'][idx] = (ttensor.data_ptr() + ttensor.storage_offset(),
                                                           ttensor.numel() * ttensor.itemsize)
    reg_descs = agent.register_memory(nixl_memory_info, 'VRAM', is_sorted=False)
    if not reg_descs:  # Same as reg_descs if successful
        print('Memory registration failed.')
        exit()

if rank == 0:
    print('exchanging endpoint info... ')
all_endpoint_info = [{} for _ in range(world_size)]
if args.transfer_engine == 'dlslime':
    dist.all_gather_object(all_endpoint_info, rdma_endpoint.endpoint_info)
elif args.transfer_engine == 'mooncake':
    dist.all_gather_object(all_endpoint_info, mooncake_endpoint_info)
elif args.transfer_engine == 'nixl':
    dist.all_gather_object(all_endpoint_info, nixl_endpoint_info)
if rank == 0:
    print('endpoint exchanged')

if args.transfer_engine == 'dlslime':
    # endpoint connect
    rdma_endpoint.connect(all_endpoint_info[(rank + num_channels) % world_size])
elif args.transfer_engine == 'nccl':
    # construction by torch.distributed
    pass
elif args.transfer_engine == 'mooncake':
    # construct connect lazily
    pass
elif args.transfer_engine == 'nixl':
    if rank < num_channels:
        agent.fetch_remote_metadata('target', all_endpoint_info[peer_rank]['local_ip'], args.nixl_port + peer_rank)

        agent.send_local_metadata(all_endpoint_info[peer_rank]['local_ip'], args.nixl_port + peer_rank)
        notifs = agent.get_new_notifs()
        while len(notifs) == 0:
            notifs = agent.get_new_notifs()

        ready = False
        while not ready:
            ready = agent.check_remote_metadata('target')

        print('Ready for transfer')
    else:
        ready = False

        target_descs = reg_descs.trim()
        target_desc_str = agent.get_serialized_descs(target_descs)

        while not ready:
            ready = agent.check_remote_metadata('initiator')

        agent.send_notif('initiator', target_desc_str)

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)


def transfer_batch_concurrency_dlslime(role, opcode, mr_key, tensor, batch_size, num_concurrency):
    slime_opcode = dlslime.OpCode.READ if opcode == 'read' else dlslime.OpCode.WRITE
    if role == 'initiator':
        assigns = []
        for concurrent_id in range(num_concurrency):
            assign = [
                rdma_endpoint.post_oneside_raw_v(slime_opcode, [mr_key for _ in range(batch_size)],
                                                 [0 for _ in range(batch_size)], [0 for _ in range(batch_size)],
                                                 [tensor.numel() * tensor.itemsize for _ in range(batch_size)],
                                                 async_op=True)
            ]
            assigns.extend(assign)

        [assign.wait() for assign in assigns]


def transfer_batch_concurrency_mooncake(role, opcode, mr_key, tensor, batch_size, num_concurrency):
    # assert opcode == 'read'
    if role == 'initiator':
        all_batch_ids_to_wait = []
        for concurrent_id in range(num_concurrency):
            batch_id = rdma_endpoint.batch_transfer_async_write(
                f"{all_endpoint_info[peer_rank]['local_ip']}:{all_endpoint_info[peer_rank]['endpoint']}",
                [all_endpoint_info[rank]['kv_table'][mr_key][0] for _ in range(batch_size)],
                [all_endpoint_info[peer_rank]['kv_table'][mr_key][0] for _ in range(batch_size)],
                [tensor.numel() * tensor.itemsize for _ in range(batch_size)],
            )
            if batch_id == 0:
                print('error for transport')
            all_batch_ids_to_wait.append(batch_id)
        result = rdma_endpoint.get_batch_transfer_status(all_batch_ids_to_wait)
        if result != 0:
            print(f'transport failure, batch IDs: {all_batch_ids_to_wait}')


def transfer_batch_concurrency_nixl(role, opcode, mr_key, tensor, batch_size, num_concurrency):
    if role == 'initiator':
        xfer_handles = []
        target_mr_info = all_endpoint_info[peer_rank]['kv_table'][mr_key][0]
        initiator_mr_info = all_endpoint_info[rank]['kv_table'][mr_key][0]
        for concurrent_id in range(num_concurrency):
            target_desc_addrs = [(target_mr_info, tensor.numel() * tensor.itemsize, peer_rank % npros_per_rank)
                                 for _ in range(batch_size)]
            initiator_desc_addrs = [(initiator_mr_info, tensor.numel() * tensor.itemsize, rank % npros_per_rank)
                                    for _ in range(batch_size)]
            target_descs = agent.get_xfer_descs(target_desc_addrs, 'VRAM', is_sorted=False)
            initiator_descs = agent.get_xfer_descs(initiator_desc_addrs, 'VRAM', is_sorted=False)

            xfer_handle = agent.initialize_xfer(opcode.upper(), initiator_descs, target_descs, 'target', 'UUID')

            if not xfer_handle:
                print('Creating transfer failed.')
                exit()
            state = agent.transfer(xfer_handle)
            xfer_handles.append(xfer_handle)
        for xfer_handle in xfer_handles:
            while True:
                state = agent.check_xfer_state(xfer_handle)
                if state == 'ERR':
                    print('Transfer got to Error state.')
                    exit()
                elif state == 'DONE':
                    break
            agent.release_xfer_handle(xfer_handle)


def transfer_batch_concurrency_nccl(role, opcode, mr_key, tensor, batch_size, num_concurrency):
    futures = []
    for concurrent_id in range(num_concurrency):
        if role == 'target':
            reqs = [
                distributed_c10d.P2POp(dist.isend, tensor, peer_rank, tag=iter_id * batch_size + batch_id, group=transfer_group)
                for batch_id in range(batch_size)
            ]
            futures.extend(distributed_c10d.batch_isend_irecv(reqs))
        else:
            reqs = [
                distributed_c10d.P2POp(dist.irecv, tensor, peer_rank, tag=iter_id * batch_size + batch_id, group=transfer_group)
                for batch_id in range(batch_size)
            ]
            futures.extend(distributed_c10d.batch_isend_irecv(reqs))
    [future.wait() for future in futures]


n_runs = args.num_concurrency
benchmark_data = []
for idx, (rawsize, ttensor) in enumerate(zip(args.size, ttensors)):
    rank_0_print(f'benchmark s={ttensor.numel() * ttensor.itemsize / 1024}K')
    size = 2 << rawsize
    total_time = 0.0
    start_event.record()
    for iter_id in range(args.num_iteration):
        if args.transfer_engine == 'dlslime':
            transfer_batch_concurrency_dlslime(role, args.opcode, idx, ttensor, args.batch_size,
                                               args.num_concurrency)
        elif args.transfer_engine == 'mooncake':
            transfer_batch_concurrency_mooncake(role, args.opcode, idx, ttensor, args.batch_size,
                                                args.num_concurrency)
        elif args.transfer_engine == 'nixl':
            transfer_batch_concurrency_nixl(role, args.opcode, idx, ttensor, args.batch_size,
                                            args.num_concurrency)
        elif args.transfer_engine == 'nccl':
            transfer_batch_concurrency_nccl(role, args.opcode, idx, ttensor, args.batch_size,
                                            args.num_concurrency)
        torch.cuda.synchronize()
    end_event.record()
    torch.cuda.synchronize()
    dist.barrier()
    elapsed_time = start_event.elapsed_time(end_event)
    total_time += elapsed_time

    if rank < num_channels:
        size_bytes = ttensor.numel() * ttensor.itemsize
        total_transport = n_runs * size * ttensor.itemsize * args.num_iteration * args.batch_size
        avg_latency = total_time / args.num_iteration / n_runs

        bandwidth = torch.tensor(total_transport / total_time / 1e3)
        dist.all_reduce(bandwidth, group=initiator_group)
        bandwidth = int(bandwidth)

        benchmark_data.append([
            args.transfer_engine,
            num_channels,
            f'{size_bytes:,}',  # noqa: E231
            f'{args.batch_size}',  # noqa: E231
            f'{args.num_concurrency}',  # noqa: E231
            f'{total_transport:,}',  # noqa: E231
            f'{avg_latency:.3f}',  # noqa: E231
            f'{bandwidth:.3f}'  # noqa: E231
        ])

        rank_0_print([
            args.transfer_engine,
            num_channels,
            f'{size_bytes:,}',  # noqa: E231
            f'{args.batch_size}',  # noqa: E231
            f'{args.num_concurrency}',  # noqa: E231
            f'{total_transport:,}',  # noqa: E231
            f'{avg_latency:.3f}',  # noqa: E231
            f'{bandwidth:.3f}'  # noqa: E231
        ])

dist.barrier()

if rank == 0:
    headers = [
        'Transfer Engine', '#Channels', 'Message Size (bytes)', 'Batch Size', 'Num Concurrency',
        'Total Transport (bytes)', 'Avg Latency(ms)', 'Bandwidth(MB/s)'
    ]
    print('\nBenchmark Results:')
    print(tabulate(benchmark_data, headers=headers, tablefmt='github'))
    if args.save_csv:
        with open(args.csv_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(headers)
            writer.writerows(benchmark_data)
        print(f'CSV saved to {args.csv_filename}')

if args.transfer_engine == 'nixl':
    if role == 'target':
        agent.remove_remote_agent('target')
        agent.invalidate_local_metadata(local_ip, args.nixl_port + rank)

    agent.deregister_memory(reg_descs)

dist.destroy_process_group()
