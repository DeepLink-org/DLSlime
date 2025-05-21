import argparse
import os

import torch
import torch.distributed as dist

from dlslime import _slime_torch  # noqa: F401

parser = argparse.ArgumentParser()
parser.add_argument('--mode', type=str)

parser.add_argument('--mater-addr', type=str, default="localhost")
parser.add_argument('--mater-port', type=str, default="6006")

args = parser.parse_args()

os.environ['MASTER_ADDR'] = args.master_addr
os.environ['MASTER_PORT'] = args.master_port

if args.mode == 'send':
    stensor = torch.ones([1600]).cuda()
    dist.init_process_group('cuda:dlslime', rank=0, world_size=2)
    dist.send(stensor, dst=1)
if args.mode == 'recv':
    rtensor = torch.zeros([1600]).cuda()
    dist.init_process_group('cuda:dlslime', rank=1, world_size=2)
    print(f'rtensor befors recv: {rtensor}')
    dist.recv(rtensor, src=0)
    print(f'rtensor after send: {rtensor}')
