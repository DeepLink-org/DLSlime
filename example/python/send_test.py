import argparse
# import ctypes
import json
import time

import _slime_c
import numpy as np
import zmq


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--device', default='rxe_0', help='RDMA device name')
    parser.add_argument('--ib-port', type=int, default=1, help='IB port number')
    parser.add_argument('--link-type', default='RoCE', choices=['IB', 'RoCE'])
    parser.add_argument('--peer-addr', default='192.168.254.128', help='Peer IP address')
    parser.add_argument('--port-data', type=int, default=5557, help='Data plane port')
    parser.add_argument('--port-meta', type=int, default=5558, help='Metadata port')
    parser.add_argument('--block-size', type=int, default=4096, help='Block size')
    parser.add_argument('--batch-size', type=int, default=256, help='Batch size')
    parser.add_argument('--duration', type=int, default=10, help='Test duration (seconds)')
    return parser.parse_args()


def main():
    args = parse_args()

    print('Initializing RDMA sender endpoint...')
    sender = _slime_c.rdma_endpoint(args.device, args.ib_port, args.link_type, 16)

    print('Establishing control plane via ZMQ...')
    zmq_ctx = zmq.Context()
    sock_data = zmq_ctx.socket(zmq.REQ)
    sock_meta = zmq_ctx.socket(zmq.REQ)

    sock_data.connect(f'tcp://{args.peer_addr}:{args.port_data}')
    sock_meta.connect(f'tcp://{args.peer_addr}:{args.port_meta}')

    sock_data.send_string(json.dumps(sender.get_data_context_info()))
    sock_meta.send_string(json.dumps(sender.get_meta_context_info()))

    print('Connecting to receiver side...')
    data_info = json.loads(sock_data.recv_string())
    meta_info = json.loads(sock_meta.recv_string())

    sender.context_connect(data_info, meta_info)
    print('Connection established successfully')

    batch_size = 4
    buffers = [
        np.full(1024, ord('A'), dtype=np.uint8),
        np.full(1024, ord('B'), dtype=np.uint8),
        np.full(1024, ord('C'), dtype=np.uint8),
        np.full(1024, ord('D'), dtype=np.uint8)
    ]

    ptrs = [buf.ctypes.data for buf in buffers]
    sizes = [buf.nbytes for buf in buffers]

    print('Starting send operations...')
    sender.launch_send(1)

    try:
        sender.send(ptrs, sizes, batch_size)
        print(f'Successfully sent batch of size {batch_size}')
    except Exception as e:
        print(f'Send failed: {str(e)}')
        raise

    print('Main thread alive...')
    time.sleep(0.2)
    print('Main thread alive...')
    time.sleep(0.2)
    print('Main thread alive...')
    time.sleep(0.2)
    print('Main thread alive...')
    time.sleep(0.2)

    print('Stopping sender...')
    sender.stop()
    print('Sender test completed')


if __name__ == '__main__':
    main()
