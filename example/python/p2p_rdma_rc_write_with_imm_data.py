import torch

from dlslime import Assignment, RDMAEndpoint, available_nic

devices = available_nic()
assert devices, 'No RDMA devices.'

# Initialize RDMA endpoint
initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type='RoCE')
target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type='RoCE')

# Register local GPU memory with RDMA subsystem
local_tensor = torch.zeros([16], device='cuda:0', dtype=torch.uint8)
initiator.register_memory_region(
    mr_key='buffer',
    addr=local_tensor.data_ptr(),
    offset=local_tensor.storage_offset(),
    length=local_tensor.numel() * local_tensor.itemsize,
)
remote_tensor = torch.ones([16], device='cuda', dtype=torch.uint8)
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

print('Remote tensor after RDMA write:', remote_tensor)
future_write = initiator.write_batch_with_imm_data(
    [Assignment(mr_key='buffer', target_offset=0, source_offset=8, length=8)],
    qpi=0,
    imm_data=1,
    async_op=True,
)

future_recv = target.recv_batch(
    [Assignment(mr_key='buffer', target_offset=0, source_offset=0, length=8)],
    qpi=0,
    async_op=True,
)

future_write.wait()
future_recv.wait()

assert torch.all(remote_tensor[:8] == 0)
assert torch.all(remote_tensor[8:] == 1)
print('Remote tensor after RDMA write:', remote_tensor)

del target, initiator
print('run rdma rc write example successful')
