# 部署

本页整理当前单机 DLSlime control-plane 的部署形态。这里刻意保持保守：
除非上层系统提供持久化或复制策略，DLSlimeCache 服务本身应视为临时内存服务。

## 组件

| 组件            | 作用                                                  |
| --------------- | ----------------------------------------------------- |
| Redis           | NanoCtrl 使用的协调后端                               |
| NanoCtrl        | 服务治理、发现和 PeerAgent 元数据                     |
| DLSlime service | 基于 PeerAgent 的服务，例如 DLSlimeCache 或 SlimeRPC  |
| Client process  | 使用 Endpoint、PeerAgent、Cache 或 RPC API 的应用进程 |

## 本地运行

```bash
nanoctrl start

dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 \
  --port 8765 \
  --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765
```
