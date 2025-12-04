import asyncio

import xxhash

import torch

from dlslime import Assignment, RDMAEndpoint, available_nic

devices = available_nic()
assert devices, 'No RDMA devices.'

mr_key = xxhash.xxh64_intdigest("buffer")

initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type='RoCE')

local_tensor = torch.zeros([16], device='cuda:0', dtype=torch.uint8)

initiator.register_memory_region(
    mr_key=mr_key,
    addr=local_tensor.data_ptr(),
    offset=local_tensor.storage_offset(),
    length=local_tensor.numel() * local_tensor.itemsize,
)

target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type='RoCE')

remote_tensor = torch.ones([16], device='cuda', dtype=torch.uint8)

target.register_memory_region(
    mr_key=mr_key,
    addr=remote_tensor.data_ptr(),
    offset=remote_tensor.storage_offset(),
    length=remote_tensor.numel() * remote_tensor.itemsize,
)

target.connect(initiator.endpoint_info)
initiator.connect(target.endpoint_info)


# run with coroutine
async def read_batch_coroutine():
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def _completion_handler(status: int):
        loop.call_soon_threadsafe(future.set_result, status)

    initiator.read_batch_with_callback(
        [Assignment(mr_key=mr_key, target_offset=0, source_offset=8, length=8)],
        _completion_handler,
    )
    await future


asyncio.run(read_batch_coroutine())

assert torch.all(local_tensor[:8] == 0)
assert torch.all(local_tensor[8:] == 1)
print('Local tensor after RDMA read:', local_tensor)
