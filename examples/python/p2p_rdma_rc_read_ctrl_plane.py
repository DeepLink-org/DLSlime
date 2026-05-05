"""
Example: P2P RDMA RC Read using Control Plane (Declarative Topology).

Uses the declarative model:
- set_desired_topology(target_peers=[...]) to declare desired connections
- TopologyReconciler converges Actual State to Desired State (Symmetric Rendezvous)
- wait_for_peers() blocks until connections are established
"""

import torch
from dlslime import start_peer_agent

# Start two peer agents (NanoCtrl auto-generates unique names)
# In a real distributed scenario, these would run on different machines
initiator_agent = start_peer_agent(
    # alias=None (default) - NanoCtrl will auto-generate unique name
    server_url="http://127.0.0.1:3000",
)

target_agent = start_peer_agent(
    # alias=None (default) - NanoCtrl will auto-generate unique name
    server_url="http://127.0.0.1:3000",
)

# Get allocated names
initiator_name = initiator_agent.alias
target_name = target_agent.alias
print(f"Allocated names: initiator={initiator_name}, target={target_name}")

# Query available peers
print("Available peers:", initiator_agent.query())

# Declarative: set desired topology (both sides want to connect to each other)
print("Setting desired topology...")
conn = initiator_agent.set_desired_topology(target_name, ib_port=1, qp_num=1)

# Wait for reconciliation to establish connections
print("Waiting for connections...")
conn.wait()
target_agent.wait_for_peers([initiator_name])
print("Connections established.")

# Register local memory regions (each agent registers its own MR)
local_tensor = torch.zeros([16], device="cpu", dtype=torch.uint8)
handler = initiator_agent.register_memory_region(
    "kv",
    local_tensor.data_ptr(),
    int(local_tensor.storage_offset()),
    local_tensor.numel() * local_tensor.itemsize,
)

remote_tensor = torch.ones([16], device="cuda", dtype=torch.uint8)
target_agent.register_memory_region(
    "kv",
    remote_tensor.data_ptr(),
    int(remote_tensor.storage_offset()),
    remote_tensor.numel() * remote_tensor.itemsize,
)

# Perform RDMA read via the directed connection handle.
print("Performing RDMA read...")
slot = conn.read("kv", local_offset=8, remote_offset=0, length=8)
slot.wait()

# Verify results
assert torch.all(local_tensor[:8] == 0)
assert torch.all(local_tensor[8:] == 1)
print("Local tensor after RDMA read:", local_tensor)

# Cleanup
initiator_agent.shutdown()
target_agent.shutdown()
print("Control plane example completed successfully")
