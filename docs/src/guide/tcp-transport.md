# TCP Transport

DLSlime ships a TCP transfer engine alongside RDMA, NVLink, and Ascend Direct.
TCP is the right transport when:

- The hosts have no RDMA NICs (laptops, dev boxes, mixed clusters).
- You want a quick smoke test before a real RDMA deployment.
- A connection has to traverse a network without RDMA capability.

It exposes the same primitives as RDMA — `endpoint_info` / `connect`, two-sided
`send` / `recv`, one-sided `read` / `write`, and named memory regions — and
plugs into PeerAgent through the same control plane. Immediate-data ops
(`write_with_imm`, `imm_recv`) are RDMA-only; on TCP they raise
`NotImplementedError` (translated from a C++ `not_implemented` exception).

TCP is enabled by default at build time (`BUILD_TCP=ON`).

## Raw `TcpEndpoint`

Use `TcpEndpoint` when the application already owns connection setup, metadata
exchange, and memory lifetime — the same scope as raw `RDMAEndpoint`. No
NanoCtrl or Redis needed.

```python
import ctypes
from dlslime import TcpEndpoint

ep_a = TcpEndpoint(ip="0.0.0.0", port=0)   # 0 = OS-assigned
ep_b = TcpEndpoint(ip="0.0.0.0", port=0)
info_a = ep_a.endpoint_info()              # {"host", "port", "mr_info"}
info_b = ep_b.endpoint_info()

# Both sides bind a port and connect; the handshake is symmetric.
ep_a.connect(info_b)
ep_b.connect(info_a)

buf = ctypes.create_string_buffer(64)
fut = ep_a.send((ctypes.addressof(buf), 0, 5))
assert fut.wait() == 0
```

`send`, `recv`, `read`, and `write` return future objects with a `wait()`
method. `wait_for(seconds)` returns `None` on timeout and the status code
otherwise.

For one-sided ops, register both sides' MRs **before** calling `connect()` so
`endpoint_info()` carries them across the handshake:

```python
h_local = ep_a.register_memory_region("a", addr_a, 0, 256)
h_local_b = ep_b.register_memory_region("b", addr_b, 0, 256)
info_b = ep_b.endpoint_info()                      # carries mr_info["b"]
h_remote = ep_a.register_remote_memory_region("rb", info_b["mr_info"]["b"])
ep_a.connect(info_b)
ep_b.connect(ep_a.endpoint_info())
ep_a.write([(h_local, h_remote, 0, 0, 12)]).wait()
```

Examples:

- `dlslime/examples/python/p2p_tcp_rc_send_recv.py` — raw two-sided send/recv.
- `dlslime/tests/python/test_tcp.py` — exhaustive raw-endpoint reference.

## TCP through PeerAgent

PeerAgent supports TCP via a `transport="tcp"` kwarg on `connect_to`. RDMA
remains the default; existing RDMA callers are unchanged.

```python
from dlslime import PeerAgent

agent_a = PeerAgent(ctrl_url="http://127.0.0.1:4479", alias="tcp_a")
agent_b = PeerAgent(ctrl_url="http://127.0.0.1:4479", alias="tcp_b")

conn_a = agent_a.connect_to("tcp_b", transport="tcp")
conn_b = agent_b.connect_to("tcp_a", transport="tcp")
conn_a.wait()
conn_b.wait()

# Two-sided I/O works through the same agent facade as RDMA:
agent_a.send("tcp_b", (addr_a, 0, 5)).wait()
agent_b.recv("tcp_a", (addr_b, 0, 5)).wait()
```

`connect_to(transport="tcp")` accepts two extra kwargs:

| Kwarg        | Default   | Meaning                             |
| ------------ | --------- | ----------------------------------- |
| `local_host` | `0.0.0.0` | Local bind IP for the TCP endpoint. |
| `local_port` | `0`       | Local bind port (0 = OS-assigned).  |

`ib_port`, `qp_num`, `local_device`, and `peer_device` are accepted for
signature parity with the RDMA path but are ignored.

### One-sided read / write

Register memory regions **before** `connect_to`, so the rendezvous picks them
up when it captures `endpoint_info()`:

```python
agent_a.register_memory_region("buf_a", addr_a, 0, 64)
agent_b.register_memory_region("buf_b", addr_b, 0, 64)

conn_a = agent_a.connect_to("tcp_b", transport="tcp")
agent_b.connect_to("tcp_a", transport="tcp")
conn_a.wait()

ep_a = conn_a.endpoint                       # TcpEndpoint
peer_info = conn_a.peer_endpoint_info        # set by mailbox post-handshake
h_local = ep_a.register_memory_region("buf_a_loc", addr_a, 0, 64)
h_remote = ep_a.register_remote_memory_region(
    "buf_b_rem", peer_info["mr_info"]["buf_b"]
)
ep_a.write([(h_local, h_remote, 0, 0, 32)]).wait()
```

`PeerConnection.peer_endpoint_info` carries the peer's `endpoint_info` dict
once the handshake completes — including its `mr_info` — so callers can
resolve remote MR handles for one-sided ops without going through Redis.

### What does *not* work on TCP

| Op                                                                  | Behavior on TCP                                                        |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `agent.write_with_imm(...)`                                         | Raises `NotImplementedError`.                                          |
| `agent.imm_recv(...)`                                               | Raises `NotImplementedError`.                                          |
| Named-MR auto-resolution (`agent.read("peer", [("mr_name", ...)])`) | Use the `(local_handle, remote_handle, ...)` form via `conn.endpoint`. |

The named-MR auto-resolution path uses Redis MR records that are RDMA-specific
(`addr` / `rkey` fields). For TCP, register on `conn.endpoint` and pass
explicit local and remote handles — see the example above.

## Examples

PeerAgent + TCP:

- `dlslime/examples/python/p2p_tcp_send_recv_peer_agent.py` — combined demo.
- `dlslime/examples/python/p2p_tcp_rc_write_peer_agent.py` — one-sided write.
- `dlslime/examples/python/p2p_tcp_rc_read_peer_agent.py` — one-sided read.

Raw `TcpEndpoint`:

- `dlslime/examples/python/p2p_tcp_rc_send_recv.py`
- `dlslime/tests/python/test_tcp.py` — also exercises read/write/timeout.

## Tests

- `dlslime/tests/python/test_peer_agent_tcp.py` — PeerAgent send/recv,
  one-sided read, one-sided write, and the `NotImplementedError` contract for
  immediate-data ops. Skipped when NanoCtrl is not reachable at
  `DLSLIME_TEST_CTRL_URL` (default `http://127.0.0.1:4479`) or the binary was
  built without `BUILD_TCP`.
