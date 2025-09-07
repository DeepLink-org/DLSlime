import os
from typing import List

import socket

import csv
from tabulate import tabulate

import torch
import torch.distributed as dist
from torch.distributed import distributed_c10d

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--batch-size', type=int, default=1)
parser.add_argument('--size', nargs='+', type=int, default=[n for n in range(8, 25)])
parser.add_argument('--num-concurrency', type=int, default=16)
parser.add_argument('--num-iteration', type=int, default=100)
parser.add_argument('--opcode', type=str, choices=['read', 'write'], default='read')
parser.add_argument('--with-imm-data', action='store_true', help='with-imm-data')
parser.add_argument('--save-csv', action='store_true', help='Save benchmark results to CSV file')
parser.add_argument('--csv-filename', type=str, default='./output.csv', help='Filename for CSV output')
parser.add_argument('--qp-num', type=int, default=None, help='Queue Pair number for RDMA operations')
parser.add_argument('--transfer-engine', choices=['dlslime', 'mooncake', 'nixl', 'nccl'], type=str, default='dlslime')
parser.add_argument('--nixl-port', default=5555, type=int)


args = parser.parse_args()


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()
    return local_ip


local_ip = get_local_ip()


rank = int(os.environ["RANK"])
local_rank = int(os.environ["LOCAL_RANK"])
world_size = int(os.environ["WORLD_SIZE"])
local_world_size = nnodes = int(os.environ["LOCAL_WORLD_SIZE"])
master_addr = os.environ["MASTER_ADDR"]
master_port = os.environ["MASTER_PORT"]
npros_per_rank = local_world_size

assert world_size % 2 == 0
num_channels = world_size // 2

if rank == 0:
    print(f"{rank=}, {world_size=}, {npros_per_rank=}, {master_addr=}, {master_port=}")

peer_rank = (rank + num_channels) % world_size

dist.init_process_group("cpu:gloo,cuda:nccl")

initiator_group = dist.new_group(list(range(num_channels)), backend="cpu:gloo,cuda:nccl")
target_group = dist.new_group(list(range(num_channels, world_size)), backend="cpu:gloo,cuda:nccl")

if args.transfer_engine == 'dlslime' or args.transfer_engine == 'mooncake':
    from dlslime import Assignment, RDMAEndpoint, available_nic

if args.transfer_engine == 'mooncake':
    from mooncake.engine import TransferEngine as MooncakeTransferEngine
    mooncake_endpoint_info = {
        "local_ip": local_ip,
        "kv_table": {},
        "endpoint": []
    }
elif args.transfer_engine == 'nixl':
    from nixl._api import nixl_agent, nixl_agent_config
    nixl_endpoint_info = {
        "local_ip": local_ip,
        "kv_table": {},
    }
    if rank < num_channels:
        nixl_mode = "initiator"
    else:
        nixl_mode = "target"

if args.with_imm_data and args.opcode != 'write':
    raise ValueError('Immediate data can only be used with write operations.')

qp_num = args.qp_num if args.qp_num is not None else int(os.getenv('SLIME_QP_NUM', 1))

if rank == 0:
    print(f'Local_ip: {local_ip}')
    print(f'mode: RDMA RC {args.opcode}')
    print(f'batch size: {args.batch_size}')
    print(f'num concurrency: {args.num_concurrency}')
    print(f'qp num: {args.qp_num}')

benchmark_data = []

if args.transfer_engine in ['dlslime', 'mooncake']:
    rdma_devices = available_nic()
    rdma_device = rdma_devices[local_rank%len(rdma_devices)]

if args.transfer_engine == 'dlslime':
    rdma_endpoint = RDMAEndpoint(rdma_device, ib_port=1, link_type='RoCE', qp_num=qp_num)
elif args.transfer_engine == 'mooncake':
    engine = MooncakeTransferEngine()
    result = engine.initialize(f"{local_ip}:12001", 'P2PHANDSHAKE', 'rdma', rdma_device)
    mooncake_endpoint_info["endpoint"] = engine.get_rpc_port()
    rdma_endpoint = engine
elif args.transfer_engine == 'nixl':
    if rank < num_channels:
        config = nixl_agent_config(True, True, args.nixl_port + rank)
    else:
        config = nixl_agent_config(True, True, args.nixl_port + rank)
        # print(args.nixl_port + rank)
    agent = nixl_agent(nixl_mode, config)

torch.cuda.set_device(local_rank)

ttensors = [torch.ones([2 << rawsize], device=f"cuda") for rawsize in args.size]
torch.cuda.synchronize()

print("initiate start")
nixl_memory_info = []
for idx, ttensor in enumerate(ttensors):
    if args.transfer_engine == 'dlslime':
        rdma_endpoint.register_memory_region(
            f"buffer_{idx}",
            ttensor.data_ptr(),
            ttensor.storage_offset(),
            ttensor.numel() * ttensor.itemsize
        )
    elif args.transfer_engine == 'mooncake':
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
    elif args.transfer_engine == 'nixl':
        nixl_memory_info.append((ttensor.data_ptr() + ttensor.storage_offset(), ttensor.numel() * ttensor.itemsize, local_rank, ""))
        nixl_endpoint_info["kv_table"][f"buffer_{idx}"] = (
            ttensor.data_ptr() + ttensor.storage_offset(),
            ttensor.numel() * ttensor.itemsize
        )
    elif args.transfer_engine == 'nccl':
        os.environ["NCCL_P2P_DISABLE"]="1"
        os.environ["NCCL_SHM_DISABLE"]="1"
        os.environ["NCCL_P2P_NET_CHUNKSIZE"]="524288"
        os.environ["NCCL_BUFFSIZE"]="8388608"
        os.environ["NCCL_IB_QPS_PER_CONNECTION"]="8" 

if args.transfer_engine == 'nixl':
    reg_descs = agent.register_memory(
        nixl_memory_info,
        "VRAM",
        is_sorted=False
    )
    
    if not reg_descs:  # Same as reg_descs if successful
        print("Memory registration failed.")
        exit()

all_endpoint_info = [{} for _ in range(world_size)]

if args.transfer_engine == 'dlslime':
    dist.all_gather_object(all_endpoint_info, rdma_endpoint.endpoint_info)
elif args.transfer_engine == 'mooncake':
    dist.all_gather_object(all_endpoint_info, mooncake_endpoint_info)
elif args.transfer_engine == 'nixl':
    dist.all_gather_object(all_endpoint_info, nixl_endpoint_info)

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
        # initiator
        agent.fetch_remote_metadata("target", all_endpoint_info[peer_rank]['local_ip'], args.nixl_port + peer_rank)
        
        agent.send_local_metadata(all_endpoint_info[peer_rank]['local_ip'], args.nixl_port + peer_rank)
        notifs = agent.get_new_notifs()
        while len(notifs) == 0:
            notifs = agent.get_new_notifs()

        # Ensure remote metadata has arrived from fetch
        ready = False
        while not ready:
            ready = agent.check_remote_metadata("target")

        print("Ready for transfer")
    else:
        # target
        ready = False

        target_descs = reg_descs.trim()
        target_desc_str = agent.get_serialized_descs(target_descs)

        # Send desc list to initiator when metadata is ready
        while not ready:
            ready = agent.check_remote_metadata("initiator")

        agent.send_notif("initiator", target_desc_str)

        # # Waiting for transfer
        # # For now the notification is just UUID, could be any python bytes.
        # # Also can have more than UUID, and check_remote_xfer_done returns
        # # the full python bytes, here it would be just UUID.
        # while not agent.check_remote_xfer_done("initiator", b"UUID"):
        #     continue

start_event = torch.cuda.Event(enable_timing=True)
end_event = torch.cuda.Event(enable_timing=True)

n_runs = args.num_concurrency
print(f"initiate done, benchmark start")
for idx, (rawsize, ttensor) in enumerate(zip(args.size, ttensors)):
    print(f"benchmark s={ttensor.numel() * ttensor.itemsize}")
    size = 2 << rawsize
    total_time = 0.0
    start_event.record()
    for iter_id in range(args.num_iteration):
        assigns = []
        all_batch_ids_to_wait = []
        target_desc_addrs = []
        initiator_desc_addrs = []
        xfer_handles = []
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
                        ] * args.batch_size,
                        qpi=assign_id % qp_num,
                        imm_data=1,
                        async_op=True
                    )
                else:
                    if args.transfer_engine == "dlslime":
                        fn = rdma_endpoint.read_batch if args.opcode == "read" else rdma_endpoint.write_batch
                        assign = [
                            fn(
                                batch=[
                                    Assignment(
                                        mr_key=f"buffer_{idx}",
                                        target_offset=0,
                                        source_offset=0,
                                        length=ttensor.numel() * ttensor.itemsize,
                                    )
                                ]  * args.batch_size,
                                async_op=True
                            )
                        ]
                    elif args.transfer_engine == "mooncake":
                        batch_id = rdma_endpoint.batch_transfer_async_read(
                            f"{all_endpoint_info[peer_rank]['local_ip']}:{all_endpoint_info[peer_rank]['endpoint']}",
                            [all_endpoint_info[rank]['kv_table'][f"buffer_{idx}"][0]] * args.batch_size,
                            [all_endpoint_info[peer_rank]['kv_table'][f"buffer_{idx}"][0]] * args.batch_size,
                            [ttensor.numel() * ttensor.itemsize,] * args.batch_size
                        )
                        if batch_id == 0:
                            print(f"error for transport")
                    elif args.transfer_engine == "nixl":
                        xfer_handle_batch = []
                        for batch in range(args.batch_size):
                            target_desc_addrs.append((all_endpoint_info[peer_rank]['kv_table'][f'buffer_{idx}'][0], ttensor.numel() * ttensor.itemsize, peer_rank % npros_per_rank))
                            initiator_desc_addrs.append((all_endpoint_info[rank]['kv_table'][f'buffer_{idx}'][0], ttensor.numel() * ttensor.itemsize, rank % npros_per_rank))
                            target_descs = agent.get_xfer_descs(target_desc_addrs, "VRAM", is_sorted=False)
                            initiator_descs = agent.get_xfer_descs(initiator_desc_addrs, "VRAM", is_sorted=False)
                            xfer_handle = agent.initialize_xfer(
                                "READ", initiator_descs, target_descs, "target", "UUID"
                            )
                            if not xfer_handle:
                                print("Creating transfer failed.")
                                exit()
                            state = agent.transfer(xfer_handle)
                            xfer_handle_batch.append(xfer_handle)
                    elif args.transfer_engine == "nccl":
                        reqs = []
                        for batch_id in range(args.batch_size):
                            send_op = distributed_c10d.P2POp(dist.isend, ttensor, peer_rank, tag=iter_id * args.batch_size + batch_id)
                            reqs.extend([send_op])
                        assigns.extend(distributed_c10d.batch_isend_irecv(reqs))

                if args.transfer_engine == 'dlslime':
                    assigns.extend(assign)
                elif args.transfer_engine == 'mooncake':
                    all_batch_ids_to_wait.append(batch_id)
                elif args.transfer_engine == 'nixl':
                    xfer_handles.extend(xfer_handle_batch)
            else:
                if args.transfer_engine == 'dlslime':
                    if args.with_imm_data:
                        assign = rdma_endpoint.recv_batch(
                            batch=[
                                Assignment(
                                    mr_key=f"buffer_{idx}",
                                    target_offset=0,
                                    source_offset=0,
                                    length=ttensor.numel() * ttensor.itemsize,
                                )
                            ] * args.batch_size,
                            qpi=assign_id % qp_num,
                            async_op=True
                        )
                        assigns.append(assign)
                elif args.transfer_engine == 'nccl':
                    reqs = []
                    for batch_id in range(args.batch_size):
                        recv_op = distributed_c10d.P2POp(dist.irecv, ttensor, peer_rank, tag=iter_id * args.batch_size + batch_id)
                        reqs.extend([recv_op])
                    assigns.extend(distributed_c10d.batch_isend_irecv(reqs))

        if args.transfer_engine in ['dlslime', 'nccl']:
            [assign.wait() for assign in assigns]
        elif args.transfer_engine == 'mooncake':
            result = rdma_endpoint.get_batch_transfer_status(all_batch_ids_to_wait)
            if result != 0:
                print(f"transport failure, batch IDs: {all_batch_ids_to_wait}")
        elif args.transfer_engine == 'nixl':
            if nixl_mode == "initiator":
                for xfer_handle in xfer_handles:
                    while True:
                        state = agent.check_xfer_state(xfer_handle)
                        if state == "ERR":
                            print("Transfer got to Error state.")
                            exit()
                        elif state == "DONE":
                            break
                    agent.release_xfer_handle(xfer_handle)
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
            rank,
            f'{size_bytes:,}',  # noqa: E231
            f'{args.batch_size}',  # noqa: E231
            f'{args.num_concurrency}',  # noqa: E231
            f'{total_transport:,}',  # noqa: E231
            f'{avg_latency:.3f}',  # noqa: E231
            f'{bandwidth:.3f}'  # noqa: E231
        ])
        if rank == 0:
            print(
                [
                    args.transfer_engine,
                    rank,
                    f'{size_bytes:,}',  # noqa: E231
                    f'{args.batch_size}',  # noqa: E231
                    f'{args.num_concurrency}',  # noqa: E231
                    f'{total_transport:,}',  # noqa: E231
                    f'{avg_latency:.3f}',  # noqa: E231
                    f'{bandwidth:.3f}'  # noqa: E231
                ]
            )

dist.barrier()

if rank == 0:
    headers = [
        'Transfer Engine', 'rank', 'Message Size (bytes)', 'Batch Size', 'Num Concurrency', 'Total Transport (bytes)', 'Avg Latency(ms)',
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

if args.transfer_engine == 'nixl':
    if nixl_mode == "target":
        agent.remove_remote_agent("target")
        agent.invalidate_local_metadata(local_ip, args.nixl_port + rank)

    agent.deregister_memory(reg_descs)


dist.destroy_process_group()
