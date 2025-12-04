import torch

from dlslime import _slime_c, available_nic

num_qp = 1
devices = available_nic()

if __name__ == "__main__":
    send_endpoint = _slime_c.rdma_endpoint_v0(devices[0], 1, "RoCE", num_qp)
    recv_endpoint = _slime_c.rdma_endpoint_v0(devices[1], 1, "RoCE", num_qp)

    send_endpoint.context_connect(
        recv_endpoint.get_data_context_info(), recv_endpoint.get_meta_context_info()
    )
    recv_endpoint.context_connect(
        send_endpoint.get_data_context_info(), send_endpoint.get_meta_context_info()
    )

    send_tensor = torch.ones([262144], dtype=torch.uint8, device="cuda") * 2

    recv_tensor = torch.zeros([262144], dtype=torch.uint8, device="cuda")

    send_buffer = _slime_c.rdma_buffer(
        send_endpoint,
        send_tensor.data_ptr(),
        send_tensor.storage_offset(),
        send_tensor.numel(),
    )

    recv_buffer = _slime_c.rdma_buffer(
        recv_endpoint,
        recv_tensor.data_ptr(),
        recv_tensor.storage_offset(),
        recv_tensor.numel(),
    )

    print(f"before recv, {recv_tensor=}")

    recv_buffer.recv()
    send_buffer.send()

    send_buffer.wait_send()
    recv_buffer.wait_recv()

    print(f"after recv, {recv_tensor=}")
