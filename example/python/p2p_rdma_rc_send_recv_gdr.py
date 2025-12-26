import torch
from dlslime import _slime_c, available_nic

num_qp = 1
devices = available_nic()

if __name__ == "__main__":
    send_endpoint = _slime_c.RDMAEndpoint(devices[0], 1, "RoCE", 1)
    recv_endpoint = _slime_c.RDMAEndpoint(devices[1], 1, "RoCE", 1)

    send_endpoint.connect(recv_endpoint.endpoint_info())
    recv_endpoint.connect(send_endpoint.endpoint_info())

    send_tensor = torch.ones([262144], dtype=torch.uint8, device="cuda") * 2

    recv_tensor = torch.zeros([262144], dtype=torch.uint8, device="cuda")

    print(f"before recv, {recv_tensor=}")

    send_slot = send_endpoint.send(
        send_tensor.data_ptr(), 0, send_tensor.numel() * send_tensor.itemsize
    )
    recv_slot = recv_endpoint.recv(
        recv_tensor.data_ptr(), 0, recv_tensor.numel() * recv_tensor.itemsize
    )

    send_slot.wait()
    recv_slot.wait()

    print(f"after recv, {recv_tensor=}")
