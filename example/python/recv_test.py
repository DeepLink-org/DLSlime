import argparse
import json
import time

import torch
import zmq

from dlslime import _slime_c


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--device', default='rxe_0', help='RDMA device name')
    parser.add_argument('--rdmaport', type=int, default=1, help='IB port number')
    parser.add_argument('--type', default='RoCE', choices=['IB', 'RoCE'])
    parser.add_argument('--addr', default='127.0.0.1', help='IP address')
    parser.add_argument('--port', type=int, default=5557, help='port')
    return parser.parse_args()


def main():
    args = parse_args()

    print('Init the RDMA ENDPOINT OF RECV...')
    num_qp = 4
    end_point = _slime_c.rdma_endpoint(args.device, args.rdmaport, args.type, num_qp)

    print('Establishing control plane via ZMQ...')
    zmq_ctx = zmq.Context()
    sock = zmq_ctx.socket(zmq.REQ)
    sock.connect(f'tcp://{args.addr}:{args.port}')

    local_info = {
        'data_channel': json.dumps(end_point.get_data_context_info()),
        'meta_channel': json.dumps(end_point.get_meta_context_info()),
    }
    sock.send_json(local_info)
    print('Receive the RDMA Info to other side...')

    peer_info = sock.recv_json()
    data_channel_info = peer_info['data_channel']
    meta_channel_info = peer_info['meta_channel']

    end_point.context_connect(json.loads(data_channel_info), json.loads(meta_channel_info))

    print('Endpoint Connection established successfully')
    print('Finish the connection of QP, start to RECV of buf_0 and buf_1...')

    data_buf_0_0 = torch.full((1024, ), ord('A'), dtype=torch.uint8)
    ptrs_buf_0 = [data_buf_0_0.data_ptr()]
    data_sizes_buf_0 = [data_buf_0_0.numel() * data_buf_0_0.element_size()]
    offset_buf_0 = [0]

    data_buf_1_0 = torch.full((1024, ), ord('B'), dtype=torch.uint8)
    data_buf_1_1 = torch.full((2048, ), ord('C'), dtype=torch.uint8)
    ptrs_buf_1 = [data_buf_1_0.data_ptr(), data_buf_1_1.data_ptr()]
    data_sizes_buf_1 = [
        data_buf_1_0.numel() * data_buf_1_0.element_size(),
        data_buf_1_1.numel() * data_buf_1_1.element_size(),
    ]
    offset_buf_1 = [0, 0]

    buf_0 = _slime_c.rdma_buffer(end_point, ptrs_buf_0, offset_buf_0, data_sizes_buf_0)
    buf_1 = _slime_c.rdma_buffer(end_point, ptrs_buf_1, offset_buf_1, data_sizes_buf_1)

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
