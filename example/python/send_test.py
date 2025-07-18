import argparse
import json
import time

import torch
import zmq

from dlslime import _slime_c


def parse_args():
    parser = argparse.ArgumentParser(description='RDMA Python SEND Test')
    parser.add_argument('--device', type=str, default='rxe_0', help='RDMA device name')
    parser.add_argument('--ib_port', type=int, default=1, help='RDMA port number')
    parser.add_argument('--link_type', type=str, default='RoCE', help='IB or RoCE')
    parser.add_argument('--port_data', type=int, default=5557, help='ZMQ data port')
    parser.add_argument('--port_mrcn', type=int, default=5558, help='ZMQ metadata port')
    return parser.parse_args()


def main():

    args = parse_args()

    print('Init the RDMA ENDPOINT OF SEND...')
    end_point = _slime_c.rdma_endpoint(args.device, args.ib_port, args.link_type, 16)

    print('Establishing control plane via ZMQ...')

    zmq_ctx_data = zmq.Context()
    zmq_ctx_mmrg = zmq.Context()
    sock_data = zmq_ctx_data.socket(zmq.REP)
    sock_mmrg = zmq_ctx_mmrg.socket(zmq.REP)

    sock_data.bind(f'tcp://*:{args.port_data}')
    sock_mmrg.bind(f'tcp://*:{args.port_mrcn}')

    print('Receive the RDMA Info to other side...')
    data_channel_info = sock_data.recv_string()
    mmrg_channel_info = sock_mmrg.recv_string()

    sock_data.send_string(json.dumps(end_point.get_data_context_info()))
    sock_mmrg.send_string(json.dumps(end_point.get_meta_context_info()))

    end_point.context_connect(json.loads(data_channel_info), json.loads(mmrg_channel_info))
    print('Endpoint Connection established successfully')
    print('Finish the connection of QP, start to SEND of buf_0 and buf_1...')

    batch_size_buf_0 = 1
    data_buf_0 = torch.full((1024, ), ord('0'), dtype=torch.uint8)
    ptrs_buf_0 = [data_buf_0.data_ptr()]
    data_sizes_buf_0 = [data_buf_0.numel() * data_buf_0.element_size()]

    batch_size_buf_1 = 2
    data_buf_1_0 = torch.full((1024, ), ord('1'), dtype=torch.uint8)
    data_buf_1_1 = torch.full((2048, ), ord('2'), dtype=torch.uint8)
    ptrs_buf_1 = [data_buf_1_0.data_ptr(), data_buf_1_1.data_ptr()]
    data_sizes_buf_1 = [
        data_buf_1_0.numel() * data_buf_1_0.element_size(),
        data_buf_1_1.numel() * data_buf_1_1.element_size()
    ]

    buf_0 = _slime_c.rdma_buffer(end_point, ptrs_buf_0, data_sizes_buf_0, batch_size_buf_0)
    buf_1 = _slime_c.rdma_buffer(end_point, ptrs_buf_1, data_sizes_buf_1, batch_size_buf_1)

    buf_0.send()
    buf_1.send()

    for _ in range(5):
        print('Main thread working Test...')
        time.sleep(0.2)

    print('Wait SEND Complete...')
    buf_0.wait_send()
    buf_1.wait_send()

    print('The SEND test completed.')
    return 0


if __name__ == '__main__':
    main()
