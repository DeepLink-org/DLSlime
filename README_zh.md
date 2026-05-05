<div align="center">
  <img src="docs/imgs/assets/logo.png" alt="DLSlime logo" width="44%">
</div>
<p align="center">
  <a href="docs/roadmap.md"><img src="docs/imgs/assets/roadmap.svg" width="16" height="16" style="vertical-align: middle;"> 路线图 </a> |
  <a href="https://join.slack.com/t/dlslime/shared_invite/zt-3e9zvercw-a89KI_Ig8N1UTaol_q6MXg"><img src="docs/imgs/assets/slack.svg" width="16" height="16" style="vertical-align: middle;"> Slack </a> |
  <a href="docs/imgs/assets/wechat_qrcode.jpg"><img src="docs/imgs/assets/wechat.svg" width="16" height="16" style="vertical-align: middle;"> 微信群 </a> |
  <a href="https://zhuanlan.zhihu.com/p/1950701795149067622"><img src="docs/imgs/assets/zhihu.svg" width="16" height="16" style="vertical-align: middle;"> 知乎 </a> |
  <a href="README.md">English</a> |
  <a href="README_zh.md">中文</a>
</p>
<h2 align="center"> 灵活高效的异构传输工具包 </h2>

DLSlime 是面向分布式深度学习系统的异构传输工具包。它提供 Python 和
C++ API，用于 RDMA、NVLink、Ascend Direct 以及 torch 分布式风格后端的
点对点数据传输，并在同一套数据面之上提供 PeerAgent、SlimeRPC 和缓存服务
等更高层能力。

## 能力概览

| 方向          | DLSlime 提供的能力                                                 |
| ------------- | ------------------------------------------------------------------ |
| P2P 传输      | RDMA RC read/write/send-recv、NVLink、Ascend Direct                |
| Python 控制面 | `RDMAEndpoint`、`PeerAgent`、内存区注册、异步 future               |
| RPC           | 基于 PeerAgent mailbox 传输的 SlimeRPC service/proxy               |
| 缓存服务      | `dlslime-cache`，基于 assignment directory 的 RDMA cache slab 服务 |
| Torch 集成    | 可选 torch backend 和 torchrun 示例                                |
| Benchmark     | `bench/` 下的传输、endpoint、cache、RPC 微基准                     |

## 安装

### PyPI 安装

```bash
pip install dlslime==0.0.3.rc2
```

PyPI 包使用默认 CMake flags 构建。需要可选传输后端或本地 C++ 改动时，
建议从源码构建。

### 源码构建

```bash
git clone https://github.com/deeplink-org/DLSlime.git
cd DLSlime
pip install -v --no-build-isolation -e .
```

通过环境变量传递 CMake flags：

```bash
BUILD_NVLINK=ON BUILD_TORCH_PLUGIN=ON \
  pip install -v --no-build-isolation -e .
```

仅构建 C++：

```bash
cmake -S . -B build -GNinja -DBUILD_PYTHON=OFF -DBUILD_RDMA=ON
cmake --build build
```

### 构建选项

| Flag                  |                                       默认值 | 说明                                     |
| --------------------- | -------------------------------------------: | ---------------------------------------- |
| `BUILD_RDMA`          |                                         `ON` | 构建 RDMA 传输引擎                       |
| `BUILD_PYTHON`        | CMake 中为 `OFF`，`pyproject.toml` 中为 `ON` | 构建 Python bindings                     |
| `BUILD_NVLINK`        |                                        `OFF` | 构建 NVLink 传输引擎                     |
| `BUILD_ASCEND_DIRECT` |                                        `OFF` | 构建 Ascend Direct 传输                  |
| `BUILD_TORCH_PLUGIN`  |                                        `OFF` | 构建 DLSlime torch backend               |
| `BUILD_BENCH`         |                                        `OFF` | 构建 C++ 传输引擎 benchmark              |
| `BUILD_TEST`          |                                        `OFF` | 构建 C++ 测试                            |
| `USE_MACA`            |                                        `OFF` | 为 torch backend 构建启用 Metax 平台支持 |

## 快速开始

### RDMA Endpoint

底层 endpoint API 负责注册本地内存区、交换 endpoint metadata、建立连接并
发起 RDMA 操作。

```bash
python examples/python/p2p_rdma_rc_read.py
python examples/python/p2p_rdma_rc_write.py
python examples/python/p2p_rdma_rc_write_with_imm_data.py
python examples/python/p2p_rdma_rc_send_recv_gdr.py
```

这些示例使用 `available_nic()` 查找 RDMA 设备，并使用 `RDMAEndpoint`
注册本地和远端内存区。

### PeerAgent 和 SlimeRPC

PeerAgent 在底层传输之上增加基于控制面的发现和连接管理。先启动
NanoCtrl，再运行 RPC 示例：

```bash
nanoctrl start
python examples/python/rpc_example.py --ctrl http://127.0.0.1:3000
```

该示例定义 Python service，在 worker PeerAgent 上提供服务，并通过 driver
PeerAgent 的 SlimeRPC proxy 调用。

### DLSlimeCache

DLSlimeCache 拥有一块预分配内存区，并保存 assignment manifest。客户端可
将数据写入 cache slab，再通过普通 RDMA read 读回。

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765

dlslime-cache stop
```

缓存服务设计见 [docs/design/dlslime-cache.md](docs/design/dlslime-cache.md)。

### NVLink 和 Ascend

```bash
torchrun --nproc_per_node=2 examples/python/p2p_nvlink.py
python examples/python/p2p_ascend_read.py
```

Ascend Direct 说明见
[docs/huawei_ascend/README.md](docs/huawei_ascend/README.md)。

## Benchmark

Benchmark 命令和历史性能表格已经移动到独立目录：

- [bench/README.md](bench/README.md) - 传输、endpoint、cache、RPC benchmark 入口
- [docs/benchmark-rpc.md](docs/benchmark-rpc.md) - SlimeRPC vs Ray benchmark 说明

常用入口：

```bash
# 两节点聚合 RDMA 传输 benchmark
torchrun --master-addr <addr> --master-port 6006 \
  --nnodes 2 --nproc-per-node 8 --node-rank <rank> \
  bench/python/agg_transfer_bench_spmd.py \
  --qp-num 8 --transfer-engine dlslime \
  --batch-size 64 --num-iteration 100 --num-concurrency 8

# SlimeRPC vs Ray 本地 benchmark
bash bench/python/run_rpc_bench.sh
```

## 仓库结构

```text
dlslime/          Python package 和 C++ 源码
examples/python/  可运行 Python 示例
bench/            Benchmark 脚本、结果文件和 benchmark README
docs/             设计文档、路线图、平台文档和 benchmark 说明
tests/            Python 和 C++ 测试
cmake/            CMake helper modules
scripts/          开发脚本
```

## 文档

- [文档索引](docs/README.md)
- [路线图](docs/roadmap.md)
- [DLSlimeCache 设计](docs/design/dlslime-cache.md)
- [Endpoint ownership model](docs/endpoint-ownership-model.md)
- [Endpoint DeviceSignal refactor](docs/endpoint-device-signal-refactor.md)
- [华为 Ascend 说明](docs/huawei_ascend/README.md)
- [English README](README.md)

## License

See [LICENSE](LICENSE).
