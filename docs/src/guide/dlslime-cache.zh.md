# DLSlimeCache 服务

DLSlimeCache 是一个小型服务：它持有预分配内存区域，通过组合的 PeerAgent 暴露服务能力，
并记录 assignment manifest，让客户端通过现有 DLSlime endpoint 路径读取缓存数据。

## 生命周期

```bash
dlslime-cache start
dlslime-cache status
dlslime-cache stop
```

数据模式需要预分配内存：

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G
```
