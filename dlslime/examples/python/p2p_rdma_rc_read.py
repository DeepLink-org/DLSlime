import torch
from dlslime import available_nic, RDMAEndpoint

devices = available_nic()
assert devices, "No RDMA devices."

mr_key = "buffer"

# Initialize RDMA endpoint
initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type="RoCE")
target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type="RoCE")

# Register local GPU memory with RDMA subsystem
local_tensor = torch.zeros([16], device="cpu", dtype=torch.uint8)
handler = initiator.register_memory_region(
    "kv",
    local_tensor.data_ptr(),
    int(local_tensor.storage_offset()),
    local_tensor.numel() * local_tensor.itemsize,
)
remote_tensor = torch.ones([16], device="cpu", dtype=torch.uint8)
hremote = target.register_memory_region(
    "kv",
    remote_tensor.data_ptr(),
    int(remote_tensor.storage_offset()),
    remote_tensor.numel() * remote_tensor.itemsize,
)

# Simulate OOB Exchange: Target -> Initiator
# mr_info = target.get_local_mr_info("kv")
info = target.endpoint_info()
kv_info = info["mr_info"]["kv"]
hremote_on_initiator = initiator.register_remote_memory_region("kv", kv_info)

# Establish bidirectional RDMA connection:
# 1. Target connects to initiator's endpoint information
# 2. Initiator connects to target's endpoint information
# Note: Real-world scenarios typically use out-of-band exchange (e.g., via TCP)
target.connect(initiator.endpoint_info())
initiator.connect(target.endpoint_info())

print("Remote tensor after RDMA write:", remote_tensor)
slot = initiator.read([(handler, hremote_on_initiator, 0, 8, 8)], None)

slot.wait()

assert torch.all(local_tensor[:8] == 0)
assert torch.all(local_tensor[8:] == 1)
print("Remote tensor after RDMA write:", local_tensor)

del target, initiator
print("run rdma rc write example successful")
