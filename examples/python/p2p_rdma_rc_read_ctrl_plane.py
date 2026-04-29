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
    device=None,  # Auto-select
    ib_port=1,
    link_type="RoCE",
    qp_num=1,
)

target_agent = start_peer_agent(
    # alias=None (default) - NanoCtrl will auto-generate unique name
    server_url="http://127.0.0.1:3000",
    device=None,  # Auto-select (will use same device if only one available)
    ib_port=1,
    link_type="RoCE",
    qp_num=1,
)

# Get allocated names
initiator_name = initiator_agent.alias
target_name = target_agent.alias
print(f"Allocated names: initiator={initiator_name}, target={target_name}")

# Query available peers
print("Available peers:", initiator_agent.query())

# Declarative: set desired topology (both sides want to connect to each other)
print("Setting desired topology...")
initiator_agent.set_desired_topology(target_peers=[target_name])
target_agent.set_desired_topology(target_peers=[initiator_name])

# Wait for reconciliation to establish connections
print("Waiting for connections...")
initiator_agent.wait_for_peers([target_name])
target_agent.wait_for_peers([initiator_name])
print("Connections established.")

# Get endpoints
initiator = initiator_agent.get_endpoint(target_name)
target = target_agent.get_endpoint(initiator_name)

# Register local memory regions (each agent registers its own MR)
local_tensor = torch.zeros([16], device="cpu", dtype=torch.uint8)
handler = initiator_agent.register_memory_region(
    "kv",
    local_tensor.data_ptr(),
    int(local_tensor.storage_offset()),
    local_tensor.numel() * local_tensor.itemsize,
)

remote_tensor = torch.ones([16], device="cpu", dtype=torch.uint8)
target_agent.register_memory_region(
    "kv",
    remote_tensor.data_ptr(),
    int(remote_tensor.storage_offset()),
    remote_tensor.numel() * remote_tensor.itemsize,
)

# Get remote MR info through control plane
print("Getting remote MR info...")
remote_mr_info = initiator_agent.get_mr_info(target_name, "kv")
assert remote_mr_info is not None, "Failed to get remote MR info"

# Register remote memory region
hremote_on_initiator = initiator_agent.register_remote_memory_region(
    target_name,
    "kv",
    remote_mr_info,
)

# Perform RDMA read
print("Performing RDMA read...")
slot = initiator.read(
    [
        (handler, hremote_on_initiator, 0, 8, 8),
        (handler, hremote_on_initiator, 0, 0, 8),
    ],
    None,
)
slot.wait()

# Verify results
assert torch.all(local_tensor[:8] == 0)
assert torch.all(local_tensor[8:] == 1)
print("Local tensor after RDMA read:", local_tensor)

# Cleanup
initiator_agent.shutdown()
target_agent.shutdown()
print("Control plane example completed successfully")
