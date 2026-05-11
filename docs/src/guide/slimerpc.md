# SlimeRPC

SlimeRPC is a typed RPC layer built on PeerAgent and the DLSlime RDMA data path.
Use it when application logic should look like a Python service call, while
connection setup, memory registration, and request/reply transport stay inside
DLSlime.

The public API is:

```python
from dlslime.rpc import (
    method,
    proxy,
    serve,
    serve_once,
    wait_all,
    RpcError,
    RemoteRpcError,
    RpcTimeoutError,
    SoftRnrMonitor,
)
```

## Requirements

SlimeRPC requires:

- NanoCtrl running and reachable by both peers.
- Redis reachable through NanoCtrl.
- Connected PeerAgents on both client and worker.
- A DLSlime build with the C++ RPC session enabled. If the extension was built
  without RPC support, `proxy()` raises an error asking you to rebuild with
  `BUILD_RPC=ON`.

```bash
nanoctrl start
python examples/python/rpc_example.py --ctrl http://127.0.0.1:3000
```

## Basic Service

Define a service class and mark RPC-callable methods with `@method`:

```python
from dlslime.rpc import method


class CalcService:
    @method
    def add(self, a: int, b: int) -> int:
        return a + b

    @method
    def echo(self, msg: str) -> str:
        return f"echo: {msg}"
```

By default, SlimeRPC serializes arguments and return values with `pickle`.
Both sides must import the same service class definition, because method tags
are assigned deterministically by sorted method name.

## Connect PeerAgents

SlimeRPC does not replace PeerAgent discovery. First create and connect the two
agents:

```python
from dlslime import PeerAgent

worker = PeerAgent(nanoctrl_url="http://127.0.0.1:3000", alias="worker:0")
driver = PeerAgent(nanoctrl_url="http://127.0.0.1:3000", alias="driver:0")

driver_conn = driver.connect_to("worker:0", ib_port=1, qp_num=1)
worker_conn = worker.connect_to("driver:0", ib_port=1, qp_num=1)

driver_conn.wait()
worker_conn.wait()
```

## Worker Side

`serve()` blocks and dispatches requests for one peer:

```python
from dlslime.rpc import serve

serve(worker, CalcService(), peer="driver:0")
```

For examples or tests, run it in a daemon thread:

```python
import threading

threading.Thread(
    target=serve,
    args=(worker, CalcService(), "driver:0"),
    daemon=True,
).start()
```

`serve()` can infer `peer` only when exactly one peer is connected. Pass
`peer=...` explicitly in multi-peer services.

Server dispatch settings:

| Setting                    | Purpose                                                            |
| -------------------------- | ------------------------------------------------------------------ |
| `max_workers=`             | Overrides the handler dispatch pool size.                          |
| `SLIME_RPC_SERVER_WORKERS` | Default handler pool size when `max_workers` is unset.             |
| `@method(parallel=True)`   | Allows a handler to run concurrently on the same service instance. |

Handlers are serialized by default under a per-service lock. This is safer for
stateful services such as model runners, NCCL workers, and shared PyTorch
objects.

## Client Side

Create a proxy from the client PeerAgent to the worker alias:

```python
from dlslime.rpc import proxy

calc = proxy(driver, "worker:0", CalcService)

future = calc.add(1, 2)
assert future.wait(timeout=30.0) == 3

echo_future = calc.echo("hello")
assert echo_future.wait() == "echo: hello"
```

Every proxy call returns a future. Use `wait()` to receive the result:

```python
result = calc.add(1, 2).wait()
```

Wait for several calls in order:

```python
from dlslime.rpc import wait_all

futures = [calc.add(i, i * 10) for i in range(5)]
results = wait_all(futures, timeout=30.0)
```

## Channel Buffers

SlimeRPC creates an RDMA mailbox channel per `(local_agent, peer_agent)` pair.
The channel registers receive memory under names like:

```text
rpc:mailbox:<local_alias>:<peer_alias>
```

Buffer controls:

| Setting                   |                          Default | Purpose                                     |
| ------------------------- | -------------------------------: | ------------------------------------------- |
| `agent._rpc_buffer_size`  |               `32_000_000` bytes | Per-inflight request/reply slot size.       |
| `agent._rpc_max_inflight` | `SLIME_RPC_MAX_INFLIGHT` or `16` | Number of receive slots.                    |
| `SLIME_RPC_MAX_INFLIGHT`  |                             `16` | Environment fallback for max inflight RPCs. |

Set these before creating the proxy or starting `serve()`:

```python
driver._rpc_buffer_size = 64 * 1024 * 1024
driver._rpc_max_inflight = 8
worker._rpc_buffer_size = 64 * 1024 * 1024
worker._rpc_max_inflight = 8
```

Client and worker should use compatible slot sizes for large payloads.

## Raw Mode

Use `@method(raw=True)` to bypass pickle. Raw handlers receive the channel, a
request pointer, and request byte length:

```python
import ctypes
from dlslime.rpc import method


class RawEcho:
    @method(raw=True)
    def echo(self, channel, ptr, nbytes):
        request = bytes((ctypes.c_char * nbytes).from_address(ptr))
        return request.upper()
```

The client passes one `bytes` payload and receives `bytes`:

```python
raw = proxy(driver, "worker:0", RawEcho)
response = raw.echo(b"hello").wait()
assert response == b"HELLO"
```

This is the right mode for FlatBuffers, Cap'n Proto, protobuf bytes, or custom
binary layouts. See `examples/python/rpc_flatbuf_example.py` for a complete
FlatBuffers loopback.

## In-Place Raw Replies

For very small hot-path handlers, `@method(raw=True, inplace=True)` lets the
handler write the reply directly into the registered send buffer:

```python
class InplaceService:
    @method(raw=True, inplace=True)
    def echo(self, req_ptr, req_nbytes, resp_ptr, resp_cap) -> int:
        n = min(req_nbytes, resp_cap)
        ctypes.memmove(resp_ptr, req_ptr, n)
        return n
```

The handler must return the number of bytes written. While the handler runs,
the session send lock is held, so use this only for very short handlers.

## Error Handling

Common client-visible errors:

| Error             | Meaning                                        |
| ----------------- | ---------------------------------------------- |
| `RpcTimeoutError` | `future.wait(timeout=...)` timed out.          |
| `RemoteRpcError`  | The remote handler raised an exception.        |
| `RpcError`        | Base class for SlimeRPC client-visible errors. |

```python
from dlslime.rpc import RemoteRpcError, RpcTimeoutError

try:
    result = calc.add(1, 2).wait(timeout=1.0)
except RpcTimeoutError:
    ...
except RemoteRpcError as exc:
    print(exc.type_name, exc.message)
```

## One-Shot Serving

`serve_once()` dispatches exactly one incoming call and returns. It is useful for
tests and controlled loops:

```python
from dlslime.rpc import serve_once

serve_once(worker, CalcService(), peer="driver:0")
```

## RNR Monitoring

For RDMA receiver-not-ready debugging, use `SoftRnrMonitor`:

```python
from dlslime.rpc import SoftRnrMonitor

monitor = SoftRnrMonitor()
monitor.snapshot()

# run workload

print(monitor.delta())
print(monitor.total_delta())
```

## Full Examples

- `examples/python/rpc_example.py`: typed pickle RPC loopback.
- `examples/python/rpc_flatbuf_example.py`: raw FlatBuffers RPC loopback.
- `bench/python/rpc_bench_slime_worker.py`: benchmark worker.
- `bench/python/rpc_bench_slime_driver.py`: benchmark driver.
