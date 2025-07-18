import argparse
import json
import time

import torch
import zmq

from dlslime import _slime_c


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--device', default='rxe_0', help='RDMA device name')
    parser.add_argument('--ib_port', type=int, default=1, help='IB port number')
    parser.add_argument('--link_type', default='RoCE', choices=['IB', 'RoCE'])
    parser.add_argument('--peer_addr', default='192.168.254.128', help='Peer IP address')
    parser.add_argument('--port_data', type=int, default=5557, help='Data plane port')
    parser.add_argument('--port_mrcn', type=int, default=5558, help='Metadata port')
    return parser.parse_args()


def main():
    args = parse_args()

    print('Init the RDMA ENDPOINT OF RECV...')
    end_point = _slime_c.rdma_endpoint(args.device, args.ib_port, args.link_type, 16)

    print('Establishing control plane via ZMQ...')
    zmq_ctx_data = zmq.Context()
    zmq_ctx_mmrg = zmq.Context()
    sock_data = zmq_ctx_data.socket(zmq.REQ)
    sock_mmrg = zmq_ctx_mmrg.socket(zmq.REQ)

    sock_data.connect(f'tcp://{args.peer_addr}:{args.port_data}')
    sock_mmrg.connect(f'tcp://{args.peer_addr}:{args.port_mrcn}')
    local_data_channel_info = json.dumps(end_point.get_data_context_info())
    local_meta_channel_info = json.dumps(end_point.get_meta_context_info())

    sock_data.send_string(local_data_channel_info)
    sock_mmrg.send_string(local_meta_channel_info)

    print('Receive the RDMA Info to other side...')
    data_channel_info = sock_data.recv_string()
    meta_channel_info = sock_mmrg.recv_string()

    end_point.context_connect(json.loads(data_channel_info), json.loads(meta_channel_info))
    print('Endpoint Connection established successfully')
    print('Finish the connection of QP, start to RECV of buf_0 and buf_1...')

    batch_size_buf_0 = 1
    data_buf_0_0 = torch.full((1024, ), ord('A'), dtype=torch.uint8)
    ptrs_buf_0 = [data_buf_0_0.data_ptr()]
    data_sizes_buf_0 = [data_buf_0_0.numel() * data_buf_0_0.element_size()]

    batch_size_buf_1 = 2
    data_buf_1_0 = torch.full((1024, ), ord('B'), dtype=torch.uint8)
    data_buf_1_1 = torch.full((2048, ), ord('C'), dtype=torch.uint8)
    ptrs_buf_1 = [data_buf_1_0.data_ptr(), data_buf_1_1.data_ptr()]
    data_sizes_buf_1 = [
        data_buf_1_0.numel() * data_buf_1_0.element_size(),
        data_buf_1_1.numel() * data_buf_1_1.element_size()
    ]

    buf_0 = _slime_c.rdma_buffer(end_point, ptrs_buf_0, data_sizes_buf_0, batch_size_buf_0)
    buf_1 = _slime_c.rdma_buffer(end_point, ptrs_buf_1, data_sizes_buf_1, batch_size_buf_1)

    buf_0.recv()
    buf_1.recv()

    for _ in range(5):
        print('Main thread working Test...')
        time.sleep(0.2)

    print('Wait RECV Complete...')
    buf_0.wait_recv()
    buf_1.wait_recv()

    # Verify the received data
    data_buf_0_0_correct = torch.all(data_buf_0_0 == ord('0')).item()
    data_buf_1_0_correct = torch.all(data_buf_1_0 == ord('1')).item()
    data_buf_1_1_correct = torch.all(data_buf_1_1 == ord('2')).item()

    assert data_buf_0_0_correct, "Data_0_0 should contain '0'"
    assert data_buf_1_0_correct, "Data_1_0 should contain '1'"
    assert data_buf_1_1_correct, "Data_1_1 should contain '2'"

    print('The RECV test completed and data verified.')

    return 0


if __name__ == '__main__':
    main()
