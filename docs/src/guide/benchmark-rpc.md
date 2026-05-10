# SlimeRPC Benchmark

This document describes the Python RPC microbenchmark added for comparing
SlimeRPC against Ray on a single machine.

## What It Measures

The benchmark measures round-trip latency and effective bandwidth for a raw
bytes echo RPC across payload sizes from `1KB` up to `16MB` by default.

It runs two implementations:

- `SlimeRPC`: RDMA-backed `PeerAgent` RPC echo between `bench-driver` and
  `bench-worker`
- `Ray`: a local `EchoActor` baseline using the same payload sizes and metrics

The comparison script prints:

- average latency
- p50 latency
- p99 latency
- effective round-trip bandwidth
- Ray/Slime speedup

## Files

- `bench/python/run_rpc_bench.sh`
- `bench/python/rpc_bench_slime_worker.py`
- `bench/python/rpc_bench_slime_driver.py`
- `bench/python/rpc_bench_ray.py`
- `bench/python/rpc_bench_compare.py`

## Prerequisites

Before running the SlimeRPC side:

1. Start NanoCtrl and make sure it is reachable.
2. Make sure Redis is reachable through NanoCtrl.
3. Build and install DLSlime with Python bindings and RDMA support.

## Run

Default run:

```bash
bash bench/python/run_rpc_bench.sh
```

Specify control-plane address or buffer size:

```bash
bash bench/python/run_rpc_bench.sh \
  --ctrl http://127.0.0.1:3000 \
  --buf-mb 256 \
  --max-size-mb 16
```

Environment-variable form:

```bash
CTRL=http://127.0.0.1:3000 BUF_MB=256 MAX_SIZE_MB=16 \
  bash bench/python/run_rpc_bench.sh
```

## Output

The script writes:

- `bench/results/slime_rpc.csv`
- `bench/results/ray_rpc.csv`

and then prints a merged comparison table.

## Stability Notes

The default `--max-size-mb` is `16`.

That limit is intentional: the current raw mailbox RPC path is validated and
stable through `16MB` in this benchmark. Larger payloads still need a dedicated
bulk-transfer path instead of the mailbox-oriented RPC data path.

## Recent Reliability Fixes

The benchmark work also exercised and hardened several runtime behaviors:

- peer rendezvous retries no longer get stuck behind stale in-flight state
- stale Redis exchange and mailbox MR keys are cleaned on startup
- cleanup events now unblock pending RDMA waits when peers exit
- `RDMAEndpoint.shutdown()` is exposed to Python for cleanup-driven teardown
- Ray benchmark setup defaults to an isolated local runtime instead of attaching
  to an ambient cluster
