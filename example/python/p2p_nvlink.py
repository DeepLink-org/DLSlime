import argparse

import torch
import zmq

from dlslime import Assignment, NVLinkEndpoint


def transfer_demo(mode, send_socket: zmq.Socket, recv_socket: zmq.Socket, value):
    nvl_endpoint = NVLinkEndpoint()
    tensor = torch.ones(16, dtype=torch.uint8, device='cuda') * value
    nvl_endpoint.register_memory_region('buffer', tensor.data_ptr(), tensor.storage_offset(), 16)
    send_socket.send_json(nvl_endpoint.endpoint_info)
    remote_endpoint_info = recv_socket.recv_json()
    nvl_endpoint.connect(remote_endpoint_info)

    if mode == 'target':
        terminate = recv_socket.recv_string()
        assert terminate == 'terminate'
    elif mode == 'initiator':
        print(f'before transfer: {tensor.cpu()}')
        nvl_endpoint.read_batch([Assignment(mr_key='buffer', target_offset=0, source_offset=8, length=8)])
        print(f'after transfer: {tensor.cpu()}')
        send_socket.send_string('terminate')
    else:
        raise ValueError


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--role', choices=['initiator', 'target'])
    parser.add_argument('--initiator-url', help='--initiator-endpoint')
    parser.add_argument('--target-url', help='--target-endpoint')

    args = parser.parse_args()
    role = args.role

    if role == 'initiator':
        send_url, recv_url = args.initiator_url, args.target_url
        value = 0
    elif role == 'target':
        send_url, recv_url = args.target_url, args.initiator_url
        value = 1
    else:
        raise ValueError

    tcp_ctx = zmq.Context()
    send_socket = tcp_ctx.socket(zmq.PUSH)
    send_socket.bind(f'tcp://{send_url}')
    recv_socket = tcp_ctx.socket(zmq.PULL)
    recv_socket.connect(f'tcp://{recv_url}')

    transfer_demo(role, send_socket, recv_socket, value)

    send_socket.close()
    recv_socket.close()
    tcp_ctx.destroy()
