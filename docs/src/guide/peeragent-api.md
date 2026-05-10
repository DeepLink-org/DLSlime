# PeerAgent API

PeerAgent is the service-friendly facade over DLSlime endpoints. It registers
agents and memory metadata with NanoCtrl, exchanges connection metadata through
Redis, and exposes named I/O helpers so application code does not need to wire
RDMA endpoint metadata by hand.

Use PeerAgent when you want dynamic peer discovery, scoped multi-tenant tests,
named memory regions, and automatic cleanup around stale control-plane state.

## Starting Agents

```python
from dlslime import start_peer_agent

agent = start_peer_agent(
    nanoctrl_url="http://127.0.0.1:3000",
    alias="worker-a",       # optional; NanoCtrl can allocate one
    device="mlx5_0",        # optional preferred NIC
    scope="job-123",        # optional isolation prefix
)
```

`start_peer_agent(...)` returns a `PeerAgent`. You can also instantiate
`PeerAgent(...)` directly, but the factory is the normal entry point.

PeerAgent supports context-manager cleanup:

```python
with start_peer_agent(scope="example") as agent:
    print(agent.alias)
```

## Discovery

```python
peers = agent.list_agents()
self_resource = agent.get_resource()
peer_resource = agent.get_resource("worker-b")
memory_keys = agent.list_mem_keys("worker-b")
```

Common discovery calls:

| Method                              | Purpose                                                        |
| ----------------------------------- | -------------------------------------------------------------- |
| `list_agents()`                     | Return active aliases visible in the agent's NanoCtrl scope.   |
| `get_resource(peer_alias=None)`     | Return local or peer topology/resource metadata.               |
| `list_mem_keys(peer_alias=None)`    | Return registered memory-region names for local or peer agent. |
| `get_connections()`                 | Return connection handles grouped by peer alias.               |
| `query_connection(peer_alias, ...)` | Return a specific connection handle if it exists.              |

Use `scope` consistently across all agents that should discover each other.
Agents in different scopes intentionally cannot see each other.

## Connecting Peers

```python
conn = agent.connect_to(
    "worker-b",
    ib_port=1,
    qp_num=1,
)
conn.wait(timeout=60)
```

`connect_to` returns a `PeerConnection` handle.

| `PeerConnection` member    | Meaning                                                          |
| -------------------------- | ---------------------------------------------------------------- |
| `wait(timeout=60)`         | Block until the directed connection is ready.                    |
| `is_connected()`           | Return whether the local connection is established.              |
| `conn_id`                  | Stable directed connection id.                                   |
| `peer_alias`               | Remote PeerAgent alias.                                          |
| `local_nic` / `remote_nic` | Selected local and remote NICs.                                  |
| `state`                    | Connection state such as `connecting`, `connected`, or `failed`. |
| `endpoint`                 | Underlying `RDMAEndpoint` once created.                          |

For bidirectional flows, both agents normally call `connect_to` and wait on
their local handle:

```python
a_to_b = agent_a.connect_to(agent_b.alias, ib_port=1, qp_num=1)
b_to_a = agent_b.connect_to(agent_a.alias, ib_port=1, qp_num=1)

a_to_b.wait()
b_to_a.wait()
```

## Memory Regions

Register local buffers by name:

```python
handler = agent.register_memory_region(
    "kv",
    tensor.data_ptr(),
    int(tensor.storage_offset()),
    tensor.numel() * tensor.itemsize,
)
```

PeerAgent publishes memory keys through the control plane, so peers can resolve
handles by name:

```python
remote_handle = agent.get_handle("kv", peer_alias="worker-b")
local_info = agent.get_mr_info("kv")
peer_info = agent.get_mr_info("kv", peer_alias="worker-b")
```

Cleanup:

```python
agent.unregister_memory_region("kv")
agent.shutdown()
```

Keep the underlying tensor or buffer alive while the memory region is registered
and while any future using it is in flight.

## Named I/O

PeerAgent accepts two assignment styles:

- **Named assignment**: `("mr_name", target_offset, source_offset, length)`.
- **Handle assignment**: `(local_handle, remote_handle, target_offset, source_offset, length)`.

Named assignments are the ergonomic path:

```python
future = agent.read("worker-b", [("kv", 8, 0, 8)])
future.wait()
```

Handle assignments are useful for hot paths that already cached local and
remote handles:

```python
local_handle = agent.register_memory_region("recv", recv_ptr, 0, recv_nbytes)
remote_handle = agent.get_handle("data", "worker-b")

future = agent.read("worker-b", [(local_handle, remote_handle, 0, 0, 4096)], None)
future.wait()
```

Available I/O methods:

| Method                                                             | Purpose                                        |
| ------------------------------------------------------------------ | ---------------------------------------------- |
| `read(peer_alias, assignments, stream=None)`                       | RDMA read from peer into local memory.         |
| `write(peer_alias, assignments, stream=None)`                      | RDMA write from local memory into peer memory. |
| `write_with_imm(peer_alias, assignments, imm_data=0, stream=None)` | RDMA write with immediate data.                |
| `send(peer_alias, chunk, stream_handler=None)`                     | Message send through the selected endpoint.    |
| `recv(peer_alias, chunk, stream_handler=None)`                     | Message receive through the selected endpoint. |
| `imm_recv(peer_alias, stream=None)`                                | Receive immediate-data event.                  |

## Minimal Read Example

```python
import torch
from dlslime import start_peer_agent

scope = "peeragent-api-demo"

initiator = start_peer_agent(nanoctrl_url="http://127.0.0.1:3000", scope=scope)
target = start_peer_agent(nanoctrl_url="http://127.0.0.1:3000", scope=scope)

initiator_conn = initiator.connect_to(target.alias, ib_port=1, qp_num=1)
target_conn = target.connect_to(initiator.alias, ib_port=1, qp_num=1)
initiator_conn.wait()
target_conn.wait()

local = torch.zeros([16], device="cpu", dtype=torch.uint8)
remote = torch.ones([16], device="cpu", dtype=torch.uint8)

initiator.register_memory_region("kv", local.data_ptr(), 0, local.numel())
target.register_memory_region("kv", remote.data_ptr(), 0, remote.numel())

future = initiator.read(target.alias, [("kv", 8, 0, 8)])
future.wait()

initiator.shutdown()
target.shutdown()
```

## Examples

- `examples/python/p2p_rdma_rc_read_ctrl_plane.py`
- `examples/python/p2p_rdma_multi_agents_ctrl_plane.py`
- `examples/python/cache_client_example.py`
- `examples/python/rpc_example.py`
