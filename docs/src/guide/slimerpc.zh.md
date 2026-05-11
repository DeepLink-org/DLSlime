# SlimeRPC

SlimeRPC 是构建在 PeerAgent 和 DLSlime RDMA 数据面上的 typed RPC 层。它适合把应用逻辑写成
Python 服务调用，同时把连接建立、内存注册和 request/reply 传输交给 DLSlime。

公开 API：

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

## 前置条件

- NanoCtrl 正在运行，client 和 worker 都能访问。
- Redis 可通过 NanoCtrl 发现。
- 两侧 PeerAgent 已连接。
- DLSlime 构建时启用了 C++ RPC session。若未启用，`proxy()` 会提示需要用 `BUILD_RPC=ON` 重新构建。

```bash
nanoctrl start
python examples/python/rpc_example.py --ctrl http://127.0.0.1:3000
```

## 定义服务

用 `@method` 标记可远程调用的方法：

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

默认使用 `pickle` 序列化参数和返回值。两端需要导入同一个 service class 定义，因为 method tag 会按方法名排序确定。

## 连接 PeerAgent

SlimeRPC 依赖 PeerAgent 连接，不替代 discovery 流程：

```python
from dlslime import PeerAgent

worker = PeerAgent(nanoctrl_url="http://127.0.0.1:3000", alias="worker:0")
driver = PeerAgent(nanoctrl_url="http://127.0.0.1:3000", alias="driver:0")

driver_conn = driver.connect_to("worker:0", ib_port=1, qp_num=1)
worker_conn = worker.connect_to("driver:0", ib_port=1, qp_num=1)

driver_conn.wait()
worker_conn.wait()
```

## Worker 侧

`serve()` 会阻塞并处理某个 peer 的请求：

```python
from dlslime.rpc import serve

serve(worker, CalcService(), peer="driver:0")
```

在示例或测试中可放到后台线程：

```python
import threading

threading.Thread(
    target=serve,
    args=(worker, CalcService(), "driver:0"),
    daemon=True,
).start()
```

`serve()` 只有在恰好连接一个 peer 时才能省略 `peer`。多 peer 服务应显式传 `peer=...`。

## Client 侧

```python
from dlslime.rpc import proxy, wait_all

calc = proxy(driver, "worker:0", CalcService)

future = calc.add(1, 2)
assert future.wait(timeout=30.0) == 3

futures = [calc.add(i, i * 10) for i in range(5)]
results = wait_all(futures, timeout=30.0)
```

每次 proxy 调用都会返回 future，通过 `wait()` 获取结果。

## Channel Buffer

SlimeRPC 会为每个 `(local_agent, peer_agent)` 创建一个 RDMA mailbox channel，并注册类似下面的 MR：

```text
rpc:mailbox:<local_alias>:<peer_alias>
```

常用参数：

| 设置                      |                           默认值 | 作用                                    |
| ------------------------- | -------------------------------: | --------------------------------------- |
| `agent._rpc_buffer_size`  |               `32_000_000` bytes | 每个 inflight request/reply slot 大小。 |
| `agent._rpc_max_inflight` | `SLIME_RPC_MAX_INFLIGHT` 或 `16` | receive slot 数量。                     |
| `SLIME_RPC_MAX_INFLIGHT`  |                             `16` | max inflight 的环境变量默认值。         |

创建 proxy 或启动 `serve()` 前设置：

```python
driver._rpc_buffer_size = 64 * 1024 * 1024
driver._rpc_max_inflight = 8
worker._rpc_buffer_size = 64 * 1024 * 1024
worker._rpc_max_inflight = 8
```

## Raw 模式

`@method(raw=True)` 会跳过 pickle。handler 接收 channel、请求指针和请求字节数：

```python
import ctypes
from dlslime.rpc import method


class RawEcho:
    @method(raw=True)
    def echo(self, channel, ptr, nbytes):
        request = bytes((ctypes.c_char * nbytes).from_address(ptr))
        return request.upper()
```

客户端传入一个 `bytes`，返回值也是 `bytes`：

```python
raw = proxy(driver, "worker:0", RawEcho)
response = raw.echo(b"hello").wait()
```

Raw 模式适合 FlatBuffers、protobuf bytes 或自定义二进制布局。完整示例见
`examples/python/rpc_flatbuf_example.py`。

## In-Place Raw 回复

`@method(raw=True, inplace=True)` 允许 handler 直接把回复写入已注册的 send buffer：

```python
class InplaceService:
    @method(raw=True, inplace=True)
    def echo(self, req_ptr, req_nbytes, resp_ptr, resp_cap) -> int:
        n = min(req_nbytes, resp_cap)
        ctypes.memmove(resp_ptr, req_ptr, n)
        return n
```

handler 返回写入的字节数。执行期间 session send lock 会被持有，因此只适合非常短的 hot-path handler。

## 错误处理

| 错误              | 含义                              |
| ----------------- | --------------------------------- |
| `RpcTimeoutError` | `future.wait(timeout=...)` 超时。 |
| `RemoteRpcError`  | 远端 handler 抛出异常。           |
| `RpcError`        | SlimeRPC 客户端可见错误基类。     |

```python
from dlslime.rpc import RemoteRpcError, RpcTimeoutError

try:
    result = calc.add(1, 2).wait(timeout=1.0)
except RpcTimeoutError:
    ...
except RemoteRpcError as exc:
    print(exc.type_name, exc.message)
```

## 示例

- `examples/python/rpc_example.py`：typed pickle RPC loopback。
- `examples/python/rpc_flatbuf_example.py`：raw FlatBuffers RPC loopback。
- `bench/python/rpc_bench_slime_worker.py`：benchmark worker。
- `bench/python/rpc_bench_slime_driver.py`：benchmark driver。
