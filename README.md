<div align="center">
  <img src="docs/imgs/assets/logo.png" alt="DLSlime logo" width="44%">
</div>
<p align="center">
  <a href="docs/roadmap.md"><img src="docs/imgs/assets/roadmap.svg" width="16" height="16" style="vertical-align: middle;"> Roadmap </a> |
  <a href="https://join.slack.com/t/dlslime/shared_invite/zt-3e9zvercw-a89KI_Ig8N1UTaol_q6MXg"><img src="docs/imgs/assets/slack.svg" width="16" height="16" style="vertical-align: middle;"> Slack </a> |
  <a href="docs/imgs/assets/wechat_qrcode.jpg"><img src="docs/imgs/assets/wechat.svg" width="16" height="16" style="vertical-align: middle;"> WeChat Group </a> |
  <a href="https://zhuanlan.zhihu.com/p/1950701795149067622"><img src="docs/imgs/assets/zhihu.svg" width="16" height="16" style="vertical-align: middle;"> Zhihu </a>
</p>
<h2 align="center"> Flexible & Efficient Heterogeneous Transfer Toolkit </h2>

DLSlime is a heterogeneous transfer toolkit for distributed deep learning
systems. It provides Python and C++ APIs for peer-to-peer data movement across
RDMA, NVLink, Ascend Direct, and torch-distributed style backends, with higher
level PeerAgent, SlimeRPC, and cache-service utilities built on top of the same
data plane.

## Highlights

| Area              | What DLSlime Provides                                                    |
| ----------------- | ------------------------------------------------------------------------ |
| P2P transfer      | RDMA RC read/write/send-recv, NVLink transfer, Ascend Direct transfer    |
| Python control    | `RDMAEndpoint`, `PeerAgent`, memory-region registration, async futures   |
| RPC               | SlimeRPC service/proxy helpers over PeerAgent mailbox transport          |
| Cache service     | `dlslime-cache` service for assignment-directory backed RDMA cache slabs |
| Torch integration | Optional torch backend and torchrun examples                             |
| Benchmarks        | Transfer, endpoint, cache, and RPC microbenchmarks under `bench/`        |

## Install

### From PyPI

```bash
pip install dlslime==0.0.3.rc2
```

The PyPI package is built with the default CMake flags. Build from source when
you need optional transports or local C++ changes.

### From Source

```bash
git clone https://github.com/deeplink-org/DLSlime.git
cd DLSlime
pip install -v --no-build-isolation -e .
```

Pass CMake flags through the environment when enabling optional components:

```bash
BUILD_NVLINK=ON BUILD_TORCH_PLUGIN=ON \
  pip install -v --no-build-isolation -e .
```

For a pure C++ build:

```bash
cmake -S . -B build -GNinja -DBUILD_PYTHON=OFF -DBUILD_RDMA=ON
cmake --build build
```

### Build Flags

| Flag                  |                                  Default | Description                                            |
| --------------------- | ---------------------------------------: | ------------------------------------------------------ |
| `BUILD_RDMA`          |                                     `ON` | Build the RDMA transfer engine                         |
| `BUILD_PYTHON`        | `OFF` in CMake, `ON` in `pyproject.toml` | Build Python bindings                                  |
| `BUILD_NVLINK`        |                                    `OFF` | Build the NVLink transfer engine                       |
| `BUILD_ASCEND_DIRECT` |                                    `OFF` | Build Ascend Direct transport                          |
| `BUILD_TORCH_PLUGIN`  |                                    `OFF` | Build DLSlime as a torch backend                       |
| `BUILD_BENCH`         |                                    `OFF` | Build C++ transfer-engine benchmarks                   |
| `BUILD_TEST`          |                                    `OFF` | Build C++ tests                                        |
| `USE_MACA`            |                                    `OFF` | Enable Metax platform support for torch backend builds |

## Quick Start

### RDMA Endpoint

The low-level endpoint API registers local memory regions, exchanges endpoint
metadata out of band, connects peers, and issues RDMA operations.

```bash
python examples/python/p2p_rdma_rc_read.py
python examples/python/p2p_rdma_rc_write.py
python examples/python/p2p_rdma_rc_write_with_imm_data.py
python examples/python/p2p_rdma_rc_send_recv_gdr.py
```

The examples use `available_nic()` to find RDMA devices and `RDMAEndpoint` to
register local and remote memory regions.

### PeerAgent and SlimeRPC

PeerAgent adds control-plane based discovery and connection management. Start a
NanoCtrl instance first, then run the RPC example:

```bash
nanoctrl start
python examples/python/rpc_example.py --ctrl http://127.0.0.1:3000
```

The example defines a Python service, serves it on a worker PeerAgent, and
calls it from a driver PeerAgent through the SlimeRPC proxy API.

### DLSlimeCache

DLSlimeCache owns a preallocated memory region and stores assignment manifests
so clients can write bytes into cache slabs and read them back through normal
RDMA operations.

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765

dlslime-cache stop
```

See [docs/design/dlslime-cache.md](docs/design/dlslime-cache.md) for the cache
service design and API.

### NVLink and Ascend

```bash
torchrun --nproc_per_node=2 examples/python/p2p_nvlink.py
python examples/python/p2p_ascend_read.py
```

Ascend Direct setup details live in
[docs/huawei_ascend/README.md](docs/huawei_ascend/README.md).

## Benchmarks

Benchmark commands and historical performance tables now live under the
benchmark directory:

- [bench/README.md](bench/README.md) - transfer, endpoint, cache, and RPC benchmark entry point
- [docs/benchmark-rpc.md](docs/benchmark-rpc.md) - focused SlimeRPC vs Ray benchmark guide

Common entry points:

```bash
# Aggregated RDMA transfer benchmark, two nodes
torchrun --master-addr <addr> --master-port 6006 \
  --nnodes 2 --nproc-per-node 8 --node-rank <rank> \
  bench/python/agg_transfer_bench_spmd.py \
  --qp-num 8 --transfer-engine dlslime \
  --batch-size 64 --num-iteration 100 --num-concurrency 8

# SlimeRPC vs Ray local benchmark
bash bench/python/run_rpc_bench.sh
```

## Repository Layout

```text
dlslime/          Python package and C++ sources
examples/python/  Runnable Python examples
bench/            Benchmark scripts, result files, and benchmark README
docs/             Design notes, roadmap, platform docs, and benchmark notes
tests/            Python and C++ tests
cmake/            CMake helper modules
scripts/          Development scripts
```

## Documentation

- [Documentation index](docs/README.md)
- [Roadmap](docs/roadmap.md)
- [DLSlimeCache design](docs/design/dlslime-cache.md)
- [Endpoint ownership model](docs/endpoint-ownership-model.md)
- [Endpoint DeviceSignal refactor](docs/endpoint-device-signal-refactor.md)
- [Huawei Ascend guide](docs/huawei_ascend/README.md)
- [Chinese README](README_zh.md)

## License

See [LICENSE](LICENSE).
