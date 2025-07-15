import argparse
# import ctypes
import json
import time

import _slime_c
import numpy as np
import zmq


def main():

    parser = argparse.ArgumentParser(description='RDMA Python Test')
    parser.add_argument('--device', type=str, default='rxe_0', help='RDMA device name')
    parser.add_argument('--ib_port', type=int, default=1, help='RDMA port number')
    parser.add_argument('--link_type', type=str, default='RoCE', help='IB or RoCE')
    parser.add_argument('--port_data', type=int, default=5557, help='ZMQ data port')
    parser.add_argument('--port_mrcn', type=int, default=5558, help='ZMQ metadata port')
    args = parser.parse_args()

    print('Initializing RDMA receiver endpoint...')

    receiver = _slime_c.rdma_endpoint(args.device, args.ib_port, args.link_type, 16)

    print('RDMA QP INFO via TCP...')

    zmq_ctx_data = zmq.Context()
    zmq_ctx_mmrg = zmq.Context()

    sock_data = zmq_ctx_data.socket(zmq.REP)
    sock_mmrg = zmq_ctx_mmrg.socket(zmq.REP)

    sock_data.bind(f'tcp://*:{args.port_data}')
    sock_mmrg.bind(f'tcp://*:{args.port_mrcn}')

    data_channel_info = sock_data.recv_string()
    mmrg_channel_info = sock_mmrg.recv_string()

    print('Connect to the Tx side...')

    receiver.context_connect(json.loads(data_channel_info), json.loads(mmrg_channel_info))
    print('Connect Success...')

    print('Send the RDMA Info to Tx...')

    sock_data.send_string(json.dumps(receiver.get_data_context_info()))
    sock_mmrg.send_string(json.dumps(receiver.get_meta_context_info()))

    print('Finish the connection of QP, start to RECV...')

    batch_size = 4
    buffers = [
        np.empty(1024, dtype=np.uint8),
        np.empty(1024, dtype=np.uint8),
        np.empty(1024, dtype=np.uint8),
        np.empty(1024, dtype=np.uint8)
    ]

    ptrs = [buf.ctypes.data for buf in buffers]
    sizes = [buf.nbytes for buf in buffers]

    print('Launch Recv...')
    receiver.launch_recv(1)

    try:
        receiver.recv(ptrs, sizes, batch_size)
        print(f'Recv called successfully with batch size: {batch_size}')
    except Exception as e:
        print(f'Recv failed: {e}')
        raise

    for i in range(5):
        print('Main thread working Test...')
        time.sleep(0.2)

    print('Wait Recv Complete...')
    receiver.stop()

    for i, buf in enumerate(buffers):
        expected = chr(ord('A') + i).encode()[0]
        if not all(b == expected for b in buf):
            print(f'Data_{i} verification failed!')
            return 1
    print('RECV endpoint test completed. Data verified.')
    return 0


if __name__ == '__main__':
    main()
