# PeerAgent API

PeerAgent 是 DLSlime 面向服务场景的 endpoint facade。它会向 NanoCtrl 注册 agent 和内存元数据，
通过 Redis 交换连接信息，并提供命名 I/O helper，应用代码不需要手动交换 RDMA endpoint metadata。

适合使用 PeerAgent 的场景包括：动态 peer 发现、多租户或多任务 `scope` 隔离、命名 memory region、
以及需要自动清理过期控制面状态的服务。

## 启动 Agent

```python
from dlslime import start_peer_agent

agent = start_peer_agent(
    nanoctrl_url="http://127.0.0.1:3000",
    alias="worker-a",   # 可选；也可由 NanoCtrl 自动分配
    device="mlx5_0",    # 可选首选 NIC
    scope="job-123",    # 可选隔离前缀
)
```

也可以使用 context manager 自动清理：

```python
with start_peer_agent(scope="example") as agent:
    print(agent.alias)
```

## 发现与连接

```python
peers = agent.list_agents()
resource = agent.get_resource("worker-b")
memory_keys = agent.list_mem_keys("worker-b")

conn = agent.connect_to("worker-b", ib_port=1, qp_num=1)
conn.wait(timeout=60)
```

常用方法：

| 方法                             | 作用                                          |
| -------------------------------- | --------------------------------------------- |
| `list_agents()`                  | 返回当前 `scope` 下可见的 active aliases。    |
| `get_resource(peer_alias=None)`  | 返回本地或 peer 的 topology/resource 元数据。 |
| `list_mem_keys(peer_alias=None)` | 返回本地或 peer 已注册的 memory-region 名称。 |
| `connect_to(peer_alias, ...)`    | 建立到指定 peer 的 directed connection。      |
| `get_connections()`              | 返回按 peer alias 分组的连接句柄。            |

所有需要互相发现的 agent 必须使用同一个 `scope`。

## 内存区域

```python
handler = agent.register_memory_region(
    "kv",
    tensor.data_ptr(),
    int(tensor.storage_offset()),
    tensor.numel() * tensor.itemsize,
)

remote_handle = agent.get_handle("kv", peer_alias="worker-b")
peer_info = agent.get_mr_info("kv", peer_alias="worker-b")
```

底层 tensor 或 buffer 必须在 memory region 注册期间保持存活，并且所有相关 future 完成前不能释放或复用。

## 命名 I/O

PeerAgent 支持两种 assignment：

- 命名形式：`("mr_name", target_offset, source_offset, length)`
- handle 形式：`(local_handle, remote_handle, target_offset, source_offset, length)`

命名形式更适合普通服务代码：

```python
future = agent.read("worker-b", [("kv", 8, 0, 8)])
future.wait()
```

可用 I/O 方法：

| 方法                                                               | 作用                                |
| ------------------------------------------------------------------ | ----------------------------------- |
| `read(peer_alias, assignments, stream=None)`                       | 从 peer RDMA read 到本地内存。      |
| `write(peer_alias, assignments, stream=None)`                      | 从本地 RDMA write 到 peer 内存。    |
| `write_with_imm(peer_alias, assignments, imm_data=0, stream=None)` | 携带 immediate data 的 RDMA write。 |
| `send(peer_alias, chunk, stream_handler=None)`                     | 消息发送。                          |
| `recv(peer_alias, chunk, stream_handler=None)`                     | 消息接收。                          |
| `imm_recv(peer_alias, stream=None)`                                | 接收 immediate-data event。         |
