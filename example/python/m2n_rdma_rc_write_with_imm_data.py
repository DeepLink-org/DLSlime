"""
M for attention engine
N for expert engine
RDMA RC Write with Immediate Data Example
"""
import argparse
import time

import torch
import torch.distributed as dist
from torch.distributed import batch_isend_irecv

import dlslime
from dlslime import _slime_c, available_nic

parser = argparse.ArgumentParser(description='RDMA RC Write with Immediate Data Example')
parser.add_argument('--max-bs', type=int, default=256, help='max batch size per rank')
parser.add_argument('--m-size', type=int, default=4, help='M size')
parser.add_argument('--n-size', type=int, default=4, help='N size')
parser.add_argument('--hidden-size', type=int, default=4096, help='hidden size')
parser.add_argument('--num-experts', type=int, default=128, help='number of experts')
parser.add_argument('--topk', type=int, default=8, help='top k for dispatch')
parser.add_argument('--qp-per-rank', type=int, default=2, help='number of QPs per rank')
parser.add_argument('--comm-backend', choices=['dlslime', 'nccl'], default='dlslime')

args = parser.parse_args()

print('initializing torch.dist ...')
dist.init_process_group(backend='cpu:gloo,cuda:nccl')
rank = dist.get_rank()
print(f"torch.dist initialized, {rank=} ...")

if __name__ == '__main__':
    for max_bs in [2**i for i in range(12, 11, -1)]:
        # SLIME_CPU_AFFINITY = [
        #     '0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46',
        #     '48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94',
        #     '96,98,100,102,104,106,108,110,112,114,116,118,120,122,124,126,128,130,132,134,136,138,140',
        #     '142,144,146,148,150,152,154,156,158,160,162,164,166,168,170,172,174,176,178,180,182,184,186,188,190',
        # ]

        devices = available_nic()
        endpoint: dlslime.RDMAEndpoint = None
        torch.cuda.set_device(rank % 8)
        # os.environ['SLIME_CPU_AFFINITY'] = SLIME_CPU_AFFINITY[int(rank)]
        if rank < args.m_size:
            endpoint = _slime_c.ep_m2n(_slime_c.EPM2NRole.M, rank, args.m_size, args.n_size, 128, 128, 2,
                                       devices[rank % len(devices)], 'RoCE')
        else:
            endpoint = _slime_c.ep_m2n(_slime_c.EPM2NRole.N, rank - args.m_size, args.m_size, args.n_size, 128, 128, 2,
                                       devices[rank % len(devices)], 'RoCE')

        assert args.m_size + args.n_size == dist.get_world_size()

        send_buffer: torch.Tensor
        recv_buffer: torch.Tensor
        # Only support M=>N for now
        if rank < args.m_size:
            send_buffer = [
                torch.zeros((max_bs, args.hidden_size), dtype=torch.int8, device='cuda') for _ in range(args.n_size)
            ]
            endpoint.register_buffer([(x.data_ptr(), x.storage_offset(), x.numel()) for x in send_buffer])
        else:
            recv_buffer = [
                torch.zeros((max_bs, args.hidden_size), dtype=torch.int8, device='cuda') for _ in range(args.m_size)
            ]
            endpoint.register_buffer([(x.data_ptr(), x.storage_offset(), x.numel()) for x in recv_buffer])

        def initialize():
            info = endpoint.endpoint_info()
            all_info = [{} for _ in range(dist.get_world_size())]
            dist.all_gather_object(all_info, info)
            print(f"<{rank}> connecting")
            if rank < args.m_size:
                endpoint.connect(all_info[args.m_size:])
            else:
                endpoint.connect(all_info[:args.m_size])
            print(f"<{rank}> connected")
            dist.barrier()

        def m2n():
            with torch.cuda.nvtx.range(f"m2n_rank_{rank}"):
                futures = []
                op_list = []
                if rank < args.m_size:
                    if args.comm_backend == 'dlslime':
                        futures.append(endpoint.m2n_send([args.max_bs * args.hidden_size for _ in range(args.n_size)]))
                    elif args.comm_backend == 'nccl':
                        for dst in range(args.n_size):
                            op_list.append(dist.P2POp(dist.isend, send_buffer[dst], dst + args.m_size))
                        futures.extend(batch_isend_irecv(op_list))
                else:
                    if args.comm_backend == 'dlslime':
                        futures.append(endpoint.m2n_recv())
                    elif args.comm_backend == 'nccl':
                        for i, src in enumerate(range(args.m_size)):
                            op_list.append(dist.P2POp(
                                dist.irecv,
                                recv_buffer[i],
                                src,
                            ))
                        futures.extend(batch_isend_irecv(op_list))
                [future.wait() for future in futures]
                dist.barrier()

        def n2m(x: torch.Tensor):
            pass

        start_event = torch.cuda.Event(enable_timing=True)
        end_event = torch.cuda.Event(enable_timing=True)
        torch.cuda.synchronize()
        initialize()
        torch.cuda.synchronize()
        # warmup
        time.sleep(3)
        for i in range(10):
            m2n()
        torch.cuda.synchronize()
        start_event.record()
        for i in range(10):
            m2n()
        end_event.record()
        torch.cuda.synchronize()

        if rank < args.m_size:
            total_send_time = start_event.elapsed_time(end_event) / 1e3
            print(f"{total_send_time=}")
            send_message_size = args.n_size * max_bs * args.hidden_size
            print(f"{send_message_size=}")
            print(f"send bw = {args.n_size * max_bs * args.hidden_size * 10 / (total_send_time) / 1024 / 1024} MiB/s")
        else:
            total_recv_time = start_event.elapsed_time(end_event) / 1e3
            print(f"{total_recv_time=}")
            recv_message_size = args.m_size * max_bs * args.hidden_size
            print(f"{recv_message_size=}")
            print(f"recv bw = {args.m_size * max_bs * args.hidden_size * 10 / (total_recv_time) / 1024 / 1024} MiB/s")

        time.sleep(1)
    dist.destroy_process_group()
