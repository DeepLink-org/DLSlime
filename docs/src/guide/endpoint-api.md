# Endpoint API

The Endpoint API is DLSlime's lowest-level Python surface for direct data-plane
access. Use it when your application already owns peer placement, endpoint
metadata exchange, memory lifetime, and cleanup.

For most service deployments, prefer [PeerAgent API](peeragent-api.md). Endpoint
API is best for transport bring-up, microbenchmarks, explicit two-process tests,
and systems that already have their own control plane.

## Main Types

| Type                                                                               | Purpose                                                                             |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `RDMAEndpoint`                                                                     | Owns QPs, endpoint metadata, local/remote memory registration, and RDMA operations. |
| `RDMAMemoryPool`                                                                   | Registers and tracks local memory regions for a shared `RDMAContext`.               |
| `RDMAContext`                                                                      | Initializes RDMA device context and future processing.                              |
| `RDMAWorker`                                                                       | Optional scheduler that can process multiple endpoints.                             |
| `Assignment`                                                                       | Describes one transfer range. Python calls usually pass assignment tuples directly. |
| `SlimeReadWriteFuture`, `SlimeSendFuture`, `SlimeRecvFuture`, `SlimeImmRecvFuture` | Async operation handles. Call `wait()` before reading results or reusing buffers.   |

## Endpoint Setup

```python
from dlslime import RDMAEndpoint, available_nic

devices = available_nic()
assert devices, "No RDMA devices available"

initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type="RoCE")
target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type="RoCE")
```

Constructor forms:

```python
RDMAEndpoint(device_name="", ib_port=1, link_type="RoCE", num_qp=1, worker=None)
RDMAEndpoint(context, num_qp=1, worker=None)
RDMAEndpoint(pool, num_qp=1, worker=None)
```

Use `available_nic()` to inspect usable RDMA devices and `socket_id(device)` to
map a NIC to its NUMA socket when building worker placement.

## Memory Registration

Register local memory before issuing reads, writes, sends, or receives:

```python
handler = endpoint.register_memory_region(
    "kv",
    tensor.data_ptr(),
    int(tensor.storage_offset()),
    tensor.numel() * tensor.itemsize,
)
```

The first argument can be a string name or an integer key. String names are
easier to debug and are what higher-level PeerAgent code uses.

Expose memory metadata to the peer:

```python
endpoint_info = endpoint.endpoint_info()
mr_info = endpoint.mr_info()
```

Register remote memory after receiving the peer's metadata through your own
out-of-band channel:

```python
remote_handle = initiator.register_remote_memory_region(
    "kv",
    target.endpoint_info()["mr_info"]["kv"],
)
```

## Connecting Peers

Both sides exchange endpoint metadata and call `connect`:

```python
target.connect(initiator.endpoint_info())
initiator.connect(target.endpoint_info())
```

The Endpoint API does not prescribe how metadata is exchanged. In examples this
is done in-process; in real deployments it may be TCP, Redis, a scheduler, or an
existing service registry.

## One-Sided I/O

`read`, `write`, and `write_with_imm` accept a batch of assignments:

```python
future = initiator.read(
    [
        # local_mr, remote_mr, local_offset, remote_offset, length
        (local_handle, remote_handle, 0, 8, 8),
    ],
    None,
)
future.wait()
```

Assignment tuple shape:

| Field           | Meaning                                             |
| --------------- | --------------------------------------------------- |
| `mr_key`        | Local memory-region handle or key.                  |
| `remote_mr_key` | Remote memory-region handle or key.                 |
| `target_offset` | Local offset for `read`, remote offset for `write`. |
| `source_offset` | Remote offset for `read`, local offset for `write`. |
| `length`        | Number of bytes to transfer.                        |

The naming follows the generic `Assignment` structure, so the interpretation is
operation-dependent. When in doubt, check the examples and keep small validation
buffers around transfer code.

## Message Operations

The same endpoint also exposes message-style operations:

```python
recv_future = target.recv([(recv_handle, 0, 4096)], None)
send_future = initiator.send([(send_handle, 0, 4096)], None)

send_future.wait()
recv_future.wait()
```

For immediate data:

```python
recv_future = target.imm_recv(None)
write_future = initiator.write_with_imm(assignments, imm_data=7, stream=None)

write_future.wait()
recv_future.wait()
assert recv_future.imm_data() == 7
```

## Cleanup

```python
endpoint.shutdown()
```

Keep tensors and buffers alive until all related futures complete. Reusing or
freeing a registered buffer before `wait()` returns can corrupt transfers.

## Examples

- `examples/python/p2p_rdma_rc_read.py`
- `examples/python/p2p_rdma_rc_write.py`
- `examples/python/p2p_rdma_rc_write_with_imm_data.py`
- `examples/python/p2p_rdma_rc_send_recv_torch.py`
