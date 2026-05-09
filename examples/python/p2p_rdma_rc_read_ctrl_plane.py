"""
Example: P2P RDMA RC Read using Control Plane (Declarative Topology).

Uses the PeerAgent connection model:
- connect_to(peer, ...) starts one peer connection
- conn.wait() blocks until the connection is established
- PeerAgent.read/write perform I/O through the established endpoint
"""

import json

import torch
from dlslime import start_peer_agent


EXAMPLE_SCOPE = "ctrl_plane_rc_read_ctrl_plane_example"


def print_topology_discovery(agent, label, peer_aliases):
    print(f"\nTopology discovery ({label} view):")
    print("Active agents:", agent.list_agents())
    for alias in peer_aliases:
        if alias == agent.alias:
            resource = agent.get_resource()
        else:
            resource = agent.get_resource(alias)
        print(f"Resource[{alias}]:")
        if resource is None:
            print("  <unavailable>")
        else:
            print(json.dumps(resource, indent=2, sort_keys=True))


# Start two peer agents (NanoCtrl auto-generates unique names)
# In a real distributed scenario, these would run on different machines
initiator_agent = start_peer_agent(
    # alias=None (default) - NanoCtrl will auto-generate unique name
    nanoctrl_url="http://127.0.0.1:3000",
    alias="dlslime0",
    scope=EXAMPLE_SCOPE,
)

target_agent = start_peer_agent(
    # alias=None (default) - NanoCtrl will auto-generate unique name
    nanoctrl_url="http://127.0.0.1:3000",
    alias="dlslime1",
    scope=EXAMPLE_SCOPE,
)

# Get allocated names
initiator_name = initiator_agent.alias
target_name = target_agent.alias
print(f"Allocated names: initiator={initiator_name}, target={target_name}")

# Query available peers
print("Available peers:", initiator_agent.list_agents())
print_topology_discovery(
    initiator_agent,
    "initiator",
    [initiator_name, target_name],
)
print_topology_discovery(
    target_agent,
    "target",
    [initiator_name, target_name],
)

if (
    target_name not in initiator_agent.list_agents()
    or initiator_name not in target_agent.list_agents()
):
    raise RuntimeError(
        "Both peer agents must use the same NanoCtrl scope so they can discover "
        f"each other. scope={EXAMPLE_SCOPE!r}"
    )

# Connect both sides so each agent has a connection handle to wait on.
print("Connecting peers...")
initiator_conn = initiator_agent.connect_to(target_name, ib_port=1, qp_num=1)
target_conn = target_agent.connect_to(initiator_name, ib_port=1, qp_num=1)

# Wait for reconciliation to establish connections
print("Waiting for connections...")
initiator_conn.wait()
target_conn.wait()
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

# Perform RDMA read via the PeerAgent I/O facade.
print("Performing RDMA read...")
slot = initiator_agent.read(target_name, [("kv", 8, 0, 8)])
slot.wait()

# Verify results
assert torch.all(local_tensor[:8] == 0)
assert torch.all(local_tensor[8:] == 1)
print("Local tensor after RDMA read:", local_tensor)

# Cleanup
initiator_agent.shutdown()
target_agent.shutdown()
print("Control plane example completed successfully")
