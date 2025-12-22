import torch
import xxhash

from dlslime import Assignment, available_nic, RDMAEndpoint

devices = available_nic()
assert devices, "No RDMA devices."

mr_key = xxhash.xxh64_intdigest("buffer")

# Initialize RDMA endpoint
initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type="RoCE")
target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type="RoCE")

# Register local GPU memory with RDMA subsystem
local_tensor = torch.zeros([16], device="cuda:0", dtype=torch.uint8)
initiator.register_memory_region(
    mr_key,
    local_tensor.data_ptr() + local_tensor.storage_offset(),
    local_tensor.numel() * local_tensor.itemsize,
)
remote_tensor = torch.ones([16], device="cuda", dtype=torch.uint8)
target.register_memory_region(
    mr_key,
    remote_tensor.data_ptr() + remote_tensor.storage_offset(),
    remote_tensor.numel() * remote_tensor.itemsize,
)

# Establish bidirectional RDMA connection:
# 1. Target connects to initiator's endpoint information
# 2. Initiator connects to target's endpoint information
# Note: Real-world scenarios typically use out-of-band exchange (e.g., via TCP)
target.connect(initiator.endpoint_info())
initiator.connect(target.endpoint_info())

print("Remote tensor after RDMA write:", remote_tensor)
x = initiator.write(
    [mr_key],
    [mr_key],
    [0],
    [8],
    [8],
    None
)

initiator.wait(x)

assert torch.all(remote_tensor[:8] == 0)
assert torch.all(remote_tensor[8:] == 1)
print("Remote tensor after RDMA write:", remote_tensor)

del target, initiator
print("run rdma rc write example successful")
