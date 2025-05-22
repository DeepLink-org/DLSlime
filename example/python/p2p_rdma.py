import asyncio

import torch  # For GPU tensor management

from dlslime import Assignment, RDMAEndpoint, available_nic  # RDMA endpoint management

devices = available_nic()
assert devices, 'No RDMA devices.'

# Initialize RDMA endpoint on NIC 'mlx5_bond_1' port 1 using RoCE transport
initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type='RoCE')
# Create a zero-initialized CUDA tensor on GPU 0 as local buffer
local_tensor = torch.zeros([16], device='cuda:0', dtype=torch.uint8)
# Register local GPU memory with RDMA subsystem
initiator.register_memory_region(
    mr_key='buffer',
    addr=local_tensor.data_ptr(),
    offset=local_tensor.storage_offset(),
    length=local_tensor.numel() * local_tensor.itemsize,
)

# Initialize target endpoint on different NIC
target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type='RoCE')

# Create a one-initialized CUDA tensor on GPU 1 as remote buffer
remote_tensor = torch.ones([16], device='cuda', dtype=torch.uint8)
# Register target's GPU memory
target.register_memory_region(
    mr_key='buffer',
    addr=remote_tensor.data_ptr(),
    offset=remote_tensor.storage_offset(),
    length=remote_tensor.numel() * remote_tensor.itemsize,
)

# Establish bidirectional RDMA connection:
# 1. Target connects to initiator's endpoint information
# 2. Initiator connects to target's endpoint information
# Note: Real-world scenarios typically use out-of-band exchange (e.g., via TCP)
target.connect(initiator.endpoint_info)
initiator.connect(target.endpoint_info)

# Execute asynchronous batch read operation:
# - Read 8 bytes from target's "buffer" at offset 0
# - Write to initiator's "buffer" at offset 0
# - asyncio.run() executes the async operation synchronously for demonstration

# run with async
x = initiator.read_batch(
    [Assignment(mr_key='buffer', target_offset=0, source_offset=8, length=8)],
    async_op=True,
)
x.wait()


# run with coroutine
async def read_batch_coroutine():
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def _completion_handler(status: int):
        loop.call_soon_threadsafe(future.set_result, status)

    initiator.read_batch_with_callback(
        [Assignment(mr_key='buffer', target_offset=0, source_offset=8, length=8)],
        _completion_handler,
    )
    await future


asyncio.run(read_batch_coroutine())

# Verify data transfer:
# - Local tensor should now contain data from remote tensor's first 8 elements
# - Remote tensor remains unchanged (RDMA read is non-destructive)

assert torch.all(local_tensor[:8] == 0)
assert torch.all(local_tensor[8:] == 1)
print('Local tensor after RDMA read:', local_tensor)
