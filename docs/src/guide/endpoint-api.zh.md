# Endpoint API

Endpoint API 是 DLSlime 最底层的 Python 数据面接口。应用如果已经自己管理 peer 放置、
endpoint 元数据交换、内存生命周期和清理逻辑，可以直接使用这一层。

多数服务化场景建议优先使用 [PeerAgent API](peeragent-api.md)。Endpoint API 更适合传输后端
bring-up、微基准、显式双进程测试，以及已经有独立控制面的系统。

## 主要类型

| 类型                                                                               | 作用                                                       |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `RDMAEndpoint`                                                                     | 管理 QP、endpoint 元数据、本地/远端内存注册和 RDMA 操作。  |
| `RDMAMemoryPool`                                                                   | 在共享 `RDMAContext` 上注册和跟踪本地内存区域。            |
| `RDMAContext`                                                                      | 初始化 RDMA 设备上下文和 future 处理。                     |
| `RDMAWorker`                                                                       | 可选调度器，可处理多个 endpoint。                          |
| `Assignment`                                                                       | 描述一次传输范围。Python 调用通常直接传 assignment tuple。 |
| `SlimeReadWriteFuture`、`SlimeSendFuture`、`SlimeRecvFuture`、`SlimeImmRecvFuture` | 异步操作句柄，读取结果或复用 buffer 前应调用 `wait()`。    |

## 基本流程

```python
from dlslime import RDMAEndpoint, available_nic

devices = available_nic()
initiator = RDMAEndpoint(device_name=devices[0], ib_port=1, link_type="RoCE")
target = RDMAEndpoint(device_name=devices[-1], ib_port=1, link_type="RoCE")

local_handle = initiator.register_memory_region(
    "kv", local_tensor.data_ptr(), int(local_tensor.storage_offset()), local_tensor.numel()
)
target.register_memory_region(
    "kv", remote_tensor.data_ptr(), int(remote_tensor.storage_offset()), remote_tensor.numel()
)

remote_handle = initiator.register_remote_memory_region(
    "kv", target.endpoint_info()["mr_info"]["kv"]
)

target.connect(initiator.endpoint_info())
initiator.connect(target.endpoint_info())

future = initiator.read([(local_handle, remote_handle, 0, 8, 8)], None)
future.wait()
```

Endpoint API 不规定元数据交换方式。示例里通常在同一进程内直接交换；真实部署中可以通过 TCP、
Redis、调度器或已有服务注册系统完成。

## Assignment Tuple

`read`、`write` 和 `write_with_imm` 接收 assignment batch：

```python
(local_handle, remote_handle, target_offset, source_offset, length)
```

`target_offset` 和 `source_offset` 的含义随操作方向变化。建议在接入初期使用小 buffer 做校验，
并参考 `examples/python/p2p_rdma_rc_read.py` 等示例。
