# DLSlime Benchmarks

This directory contains DLSlime benchmark scripts and captured results. The
root README intentionally links here instead of carrying long benchmark tables,
so benchmark commands, hardware notes, and result files can evolve together.

## Directory Layout

| Path                                | Purpose                                                       |
| ----------------------------------- | ------------------------------------------------------------- |
| `python/agg_transfer_bench_spmd.py` | Multi-process transfer benchmark for aggregate RDMA bandwidth |
| `python/endpoint_io_bench.py`       | Endpoint-level I/O benchmark                                  |
| `python/endpoint_sendrecv_bench.py` | Endpoint send/recv benchmark                                  |
| `python/cache_bench.py`             | DLSlimeCache benchmark                                        |
| `python/run_rpc_bench.sh`           | SlimeRPC vs Ray benchmark wrapper                             |
| `python/rpc_bench_*.py`             | SlimeRPC and Ray benchmark implementations                    |
| `results/`                          | CSV outputs and captured worker logs                          |

## Prerequisites

- DLSlime built with the transport being measured, usually `BUILD_RDMA=ON`
- Python dependencies from `pyproject.toml`
- `torchrun` for distributed transfer benchmarks
- RDMA devices and an active RoCE/IB fabric for RDMA tests
- NanoCtrl and Redis for PeerAgent, SlimeRPC, and cache-service benchmarks

For RPC and cache benchmarks, start NanoCtrl first:

```bash
nanoctrl start
```

## Aggregated RDMA Transfer Benchmark

Run the same command on both nodes, changing only `--node-rank`.

Node 0:

```bash
torchrun --master-addr <node0-ip> --master-port 6006 \
  --nnodes 2 --nproc-per-node 8 --node-rank 0 \
  bench/python/agg_transfer_bench_spmd.py \
  --qp-num 8 \
  --transfer-engine dlslime \
  --batch-size 64 \
  --num-iteration 100 \
  --num-concurrency 8
```

Node 1:

```bash
torchrun --master-addr <node0-ip> --master-port 6006 \
  --nnodes 2 --nproc-per-node 8 --node-rank 1 \
  bench/python/agg_transfer_bench_spmd.py \
  --qp-num 8 \
  --transfer-engine dlslime \
  --batch-size 64 \
  --num-iteration 100 \
  --num-concurrency 8
```

Useful knobs:

| Option              | Meaning                                                |
| ------------------- | ------------------------------------------------------ |
| `--nproc-per-node`  | Number of local worker processes and transfer channels |
| `--qp-num`          | Queue pairs per endpoint                               |
| `--batch-size`      | Number of assignments per iteration                    |
| `--num-concurrency` | Concurrent transfer operations                         |
| `--num-iteration`   | Timed benchmark iterations                             |
| `--transfer-engine` | Transfer engine name, commonly `dlslime`               |

## Endpoint Benchmarks

Endpoint benchmarks are useful when isolating lower-level send/recv or
read/write behavior before running aggregate workloads:

```bash
python bench/python/endpoint_io_bench.py --help
python bench/python/endpoint_sendrecv_bench.py --help
```

Use the script help output for the exact transport and message-size arguments,
because these scripts are closer to the endpoint implementation surface.

## Cache Benchmark

`cache_bench.py` measures the in-process C++ cache assignment-directory path
against a Python dict baseline. It does not transfer payload bytes over RDMA.

```bash
python bench/python/cache_bench.py \
  --keys 100000 \
  --items-per-key 1 \
  --csv bench/results/cache_assignments.csv
```

For an end-to-end cache-service correctness run, start NanoCtrl and
DLSlimeCache, then run the example client:

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765

dlslime-cache stop
```

## SlimeRPC vs Ray Benchmark

The RPC benchmark compares SlimeRPC round-trip latency and bandwidth with a Ray
actor baseline.

```bash
bash bench/python/run_rpc_bench.sh
```

With explicit parameters:

```bash
bash bench/python/run_rpc_bench.sh \
  --ctrl http://127.0.0.1:3000 \
  --buf-mb 256 \
  --max-size-mb 16
```

The script writes:

```text
bench/results/slime_rpc.csv
bench/results/ray_rpc.csv
```

See [../docs/benchmark-rpc.md](../docs/benchmark-rpc.md) for the full RPC
benchmark guide and stability notes.

## Historical GDRDMA Result Snapshot

The old root README carried large tables for a ConnectX-7 environment:

- NVIDIA ConnectX-7 HHHL adapter
- 200GbE RoCE v2 / NDR200 IB
- Dual-port QSFP112
- PCIe 5.0 x16

Representative single-channel P2P read/write results:

| Batch Size | Concurrency | Message Size | Avg Latency |   Bandwidth |
| ---------: | ----------: | -----------: | ----------: | ----------: |
|          1 |           1 |        1 MiB |    0.062 ms | 17,012 MB/s |
|          1 |           1 |      128 MiB |    2.783 ms | 48,235 MB/s |
|         64 |           1 |        1 MiB |    1.443 ms | 46,510 MB/s |
|         64 |           8 |        1 MiB |    1.384 ms | 48,478 MB/s |

Representative eight-channel aggregate results:

| Batch Size | Concurrency | Message Size | Avg Latency |    Bandwidth |
| ---------: | ----------: | -----------: | ----------: | -----------: |
|          1 |           1 |        1 MiB |    0.072 ms | 127,489 MB/s |
|          1 |           1 |      128 MiB |    2.790 ms | 384,630 MB/s |

For reproducible comparisons, keep new CSVs in `bench/results/` with filenames
that include the transport, topology, batch size, concurrency, and hardware.

## Result Hygiene

- Keep generated CSVs and worker logs under `bench/results/`.
- Record the NIC, link mode, GPU/CPU topology, process count, and queue-pair
  count next to any published result.
- Use the same `--num-iteration`, batch size, and concurrency when comparing
  engine changes.
- Treat the first run after process startup as warmup unless the benchmark
  explicitly separates warmup and timed iterations.
