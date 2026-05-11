# API 参考

本页是当前 Python API 的整理入口。运行时还会通过 `dlslime._slime_c` 暴露 C++/pybind 符号；
这些符号依赖本地构建选项，在生成式 C++ API 文档加入前，先通过示例和设计文档说明。

## Python 包

| 包                   | 作用                                                           |
| -------------------- | -------------------------------------------------------------- |
| `dlslime`            | 顶层包、日志辅助函数、PeerAgent 导出和 C++ bindings。          |
| `dlslime.peer_agent` | PeerAgent 运行时 facade 和拓扑发现辅助逻辑。                   |
| `dlslime.cache`      | DLSlimeCache client、service types 和 CLI 入口。               |
| `dlslime.rpc`        | SlimeRPC service、proxy、channel、registry 和 buffer helpers。 |

## CLI

| 命令            | 入口                     | 作用                                 |
| --------------- | ------------------------ | ------------------------------------ |
| `dlslime-cache` | `dlslime.cache.cli:main` | 启动、查看和停止 DLSlimeCache 服务。 |

## 常用导入

```python
from dlslime import PeerAgent, start_peer_agent
from dlslime.cache import CacheClient
from dlslime.logging import get_logger, set_log_level
```

## 已整理页面

- [Endpoint API](guide/endpoint-api.md)
- [PeerAgent API](guide/peeragent-api.md)
- [SlimeRPC](guide/slimerpc.md)
- [DLSlimeCache 服务](guide/dlslime-cache.md)
- [版本](versions.md)
