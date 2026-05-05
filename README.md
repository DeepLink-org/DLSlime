<div align="center">
  <img src="docs/imgs/assets/logo.png" alt="DLSlime logo" width="44%">
</div>
<p align="center">
  <a href="docs/roadmap.md"><img src="docs/imgs/assets/roadmap.svg" width="16" height="16" style="vertical-align: middle;"> Roadmap </a> |
  <a href="https://join.slack.com/t/dlslime/shared_invite/zt-3e9zvercw-a89KI_Ig8N1UTaol_q6MXg"><img src="docs/imgs/assets/slack.svg" width="16" height="16" style="vertical-align: middle;"> Slack </a> |
  <a href="docs/imgs/assets/wechat_qrcode.jpg"><img src="docs/imgs/assets/wechat.svg" width="16" height="16" style="vertical-align: middle;"> WeChat Group </a> |
  <a href="https://zhuanlan.zhihu.com/p/1950701795149067622"><img src="docs/imgs/assets/zhihu.svg" width="16" height="16" style="vertical-align: middle;"> Zhihu </a> |
  <a href="README.md">English</a> |
  <a href="README_zh.md">中文</a>
</p>
<h2 align="center"> Flexible & Efficient Heterogeneous Transfer Toolkit </h2>

DLSlime is a PeerAgent-centered communication and microservice toolkit for
distributed AI systems. PeerAgent is the runtime hub: application services such
as SlimeRPC and DLSlimeCache build on it, NanoCtrl supplies service governance
and coordination metadata around it, and endpoint APIs below it drive
heterogeneous transports such as RDMA, NVLink, and Ascend Direct.

The goal is to let systems compose high-performance data movement without tying
application logic to one transport, one topology, or one service layout.

## PeerAgent-Centered Architecture

DLSlime is organized around PeerAgent. Lower layers move bytes and expose device
capabilities; PeerAgent turns those capabilities into peer-to-peer runtime
connections; upper layers reuse the same PeerAgent data plane as
service-shaped building blocks.

| Layer                     | Components                                             | Responsibility                                                                                           |
| ------------------------- | ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| Application services      | DLSlimeCache, SlimeRPC services, user systems          | Use DLSlime as a service substrate for cache, RPC, and custom distributed components                     |
| Service governance        | NanoCtrl                                               | Register services, discover by `kind`, maintain heartbeat TTLs, scope isolation, publish Redis addresses |
| PeerAgent runtime         | PeerAgent, coordination metadata                       | Discover peers, register memory regions, establish directed connections, clean stale state               |
| Endpoint API              | `RDMAEndpoint`, `NVLinkEndpoint`, Ascend endpoints     | Register local/remote memory and issue read/write/send-recv operations                                   |
| Transfer engines          | RDMA RC, NVLink, Ascend Direct, optional torch backend | Execute transport-specific data movement                                                                 |
| Device and topology layer | NIC/GPU/NPU discovery, resource records                | Describe available devices, ports, link types, and placement hints                                       |

<p align="center">
  <img src="docs/imgs/dlslime_arch.png" alt="DLSlime PeerAgent-centered architecture" width="92%">
</p>

## How The Layers Work Together

1. A service starts and registers itself with NanoCtrl as a generic entity, for
   example `kind=cache` or `kind=rpc-worker`.
2. Each service attaches to a PeerAgent instead of managing transport state
   directly.
3. PeerAgents register their resource records and memory regions with NanoCtrl.
4. Clients discover services by `kind` and scope, then reach the service through
   its PeerAgent.
5. PeerAgents exchange connection intent and memory-region metadata through
   NanoCtrl/Redis.
6. Endpoint objects issue the actual transfer through RDMA, NVLink, Ascend
   Direct, or the selected backend.

## Usage Scenarios

### Direct Endpoint Access

Use the Endpoint API directly when the application already controls peer
placement, metadata exchange, and memory lifetime. This is the lowest-level path
through DLSlime: it avoids NanoCtrl and PeerAgent, and maps application transfer
logic straight onto endpoint-to-endpoint data movement.

<p align="center">
  <img src="docs/imgs/endpoint2endpoint.png" alt="Direct endpoint-to-endpoint access" width="88%">
</p>

Typical examples are two-process RDMA read/write tests, NVLink transfer checks,
and backend bring-up where explicit setup is more useful than service discovery.

Example: [p2p_rdma_rc_read.py](examples/python/p2p_rdma_rc_read.py),
[p2p_rdma_rc_write.py](examples/python/p2p_rdma_rc_write.py),
[p2p_nvlink.py](examples/python/p2p_nvlink.py), and
[p2p_ascend_read.py](examples/python/p2p_ascend_read.py).

### PeerAgent-to-PeerAgent Access

Use PeerAgent when the application wants peer-to-peer data movement without
managing connection setup, memory-region discovery, and stale-state cleanup by
itself. Each process owns a PeerAgent, registers its resources through NanoCtrl,
and then uses the PeerAgent facade to read or write remote memory through the
selected endpoint.

This path keeps the same endpoint data plane as direct access, but moves
coordination into NanoCtrl and PeerAgent. It is the right starting point for
multi-process services, dynamic peer discovery, and higher-level components such
as SlimeRPC and DLSlimeCache.

<p align="center">
  <img src="docs/imgs/peer2peer.png" alt="PeerAgent-to-PeerAgent access" width="88%">
</p>

Example:
[p2p_rdma_rc_read_ctrl_plane.py](examples/python/p2p_rdma_rc_read_ctrl_plane.py)
and
[p2p_rdma_multi_agents_ctrl_plane.py](examples/python/p2p_rdma_multi_agents_ctrl_plane.py).

### DLSlimeCache Service

Use DLSlimeCache when multiple PeerAgent clients need a shared RDMA-backed cache
service. PeerAgent A and PeerAgent B discover the Cache Service through
NanoCtrl, fetch cache assignment metadata from the service, and then read or
write cache slabs through the same PeerAgent and endpoint data plane.

In this path, NanoCtrl keeps the Cache Service discoverable as a registered
service, the Cache Service owns the cache memory region and assignment
manifests, and PeerAgent clients perform the data movement without embedding
cache placement logic into each application process.

<p align="center">
  <img src="docs/imgs/cacheService.png" alt="DLSlimeCache service access" width="88%">
</p>

Example: [cache_client_example.py](examples/python/cache_client_example.py) and
[dlslime-cache design](docs/design/dlslime-cache.md).

### SlimeRPC Service

Use SlimeRPC when application logic should call a Python service while keeping
the transport and peer coordination inside DLSlime. A client process uses a
SlimeRPC proxy on top of its PeerAgent, the service process serves Python
methods through a SlimeRPC server on top of its own PeerAgent, and NanoCtrl keeps
the RPC service discoverable.

RPC request and response messages are carried by the PeerAgent transport rather
than the control plane. This keeps service invocation at the application layer
while reusing the same PeerAgent, endpoint, and mailbox data path as lower-level
peer-to-peer flows.

<p align="center">
  <img src="docs/imgs/slimeRPC.png" alt="SlimeRPC service access" width="88%">
</p>

Example: [rpc_example.py](examples/python/rpc_example.py) and
[rpc_flatbuf_example.py](examples/python/rpc_flatbuf_example.py).

### Disaggregated Inference Service

Use DLSlime for disaggregated inference when prefill and decode run as separate
serving roles. This follows the same pattern used by LMDeploy DistServe:
a proxy routes requests to dedicated Prefill and Decode workers, Prefill
computes prompt KV cache, Decode generates tokens, and a migration/data-plane
backend transfers KV cache between the two roles.

In DLSlime terms, each Prefill or Decode worker can be modeled as a service with
its own PeerAgent. NanoCtrl keeps the worker roles discoverable by `kind`, stores
resource and memory metadata for the PeerAgents, and lets the serving proxy or
workers build the required prefill-to-decode connections. The KV cache transfer
then uses the PeerAgent and endpoint data plane instead of going through the
control plane.

<p align="center">
  <img src="docs/imgs/PDDisagg.png" alt="Disaggregated inference service" width="88%">
</p>

LMDeploy reference: [DistServe HTTP endpoints](https://lmdeploy.readthedocs.io/en/v0.12.2/http-routingtable.html#distserve) and [DistServe with MooncakeTransferEngine](https://kvcache-ai.github.io/Mooncake/getting_started/examples/lmdeploy-integration-v0.9.html).

### RL Service

Coming soon.

## Component Map

| Component                           | Layer                     | What it does                                                                                                           |
| ----------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `NanoCtrl/`                         | Service governance        | Rust control-plane server and Python client for service registry, heartbeat, PeerAgent coordination, and RDMA metadata |
| `dlslime.peer_agent`                | Peer coordination         | Python PeerAgent abstraction for discovery, connection management, memory registration, and read/write facade          |
| `dlslime.rpc`                       | Application service       | Service/proxy RPC helpers built over PeerAgent mailbox transport                                                       |
| `dlslime.cache`                     | Application service       | Cache service/client wrappers around assignment manifests and RDMA cache slabs                                         |
| `dlslime._slime_c`                  | Endpoint and C++ bindings | Python bindings for endpoint, assignment, cache, and transport primitives                                              |
| `dlslime/csrc/engine`               | Transfer abstraction      | Common assignment and transfer-engine interfaces                                                                       |
| `dlslime/csrc/engine/rdma`          | Transfer engine           | RDMA RC endpoint implementation                                                                                        |
| `dlslime/csrc/engine/nvlink`        | Transfer engine           | NVLink endpoint implementation                                                                                         |
| `dlslime/csrc/engine/ascend_direct` | Transfer engine           | Ascend Direct endpoint implementation                                                                                  |
| `dlslime/csrc/device`               | Device layer              | Device API, futures, signals, and platform-specific helpers                                                            |

## Common Use Cases

| Use case                           | Start here                                                    |
| ---------------------------------- | ------------------------------------------------------------- |
| Direct P2P transfer                | `RDMAEndpoint` examples under `examples/python/p2p_rdma_*`    |
| Control-plane coordinated transfer | `PeerAgent` examples under `examples/python/*_ctrl_plane.py`  |
| Python service RPC                 | `examples/python/rpc_example.py` and `dlslime.rpc`            |
| RDMA-backed cache service          | `dlslime-cache` and `examples/python/cache_client_example.py` |
| Transport benchmarking             | `bench/README.md`                                             |
| Ascend integration                 | `docs/huawei_ascend/README.md`                                |

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
