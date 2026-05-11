# SlimeRPC Benchmark

This document describes the Python RPC microbenchmark added for comparing
SlimeRPC against Ray (and optionally Pulsing) on a single machine.

## What It Measures

The benchmark measures round-trip latency and effective bandwidth for a raw
bytes echo RPC across payload sizes from `1KB` up to `16MB` by default.

It runs two implementations by default, with a third opt-in baseline:

- `SlimeRPC`: RDMA-backed `PeerAgent` RPC echo between `bench-driver` and
  `bench-worker`
- `Ray`: a local `EchoActor` baseline using the same payload sizes and metrics
- `Pulsing` (optional, off by default): a `@pul.remote` actor echo using the
  same payload sizes and metrics. Enable with `--with-pulsing`.

The comparison script prints:

- average latency
- p50 latency
- p99 latency
- effective round-trip bandwidth
- `S/Ray` = Ray avg latency / SlimeRPC avg latency (> 1 means SlimeRPC wins)
- `S/Pul` = Pulsing avg latency / SlimeRPC avg latency (only shown when
  Pulsing was enabled)

## Files

- `bench/python/run_rpc_bench.sh`
- `bench/python/rpc_bench_slime_worker.py`
- `bench/python/rpc_bench_slime_driver.py`
- `bench/python/rpc_bench_ray.py`
- `bench/python/rpc_bench_pulsing.py`
- `bench/python/rpc_bench_compare.py`

## Prerequisites

Before running the SlimeRPC side:

1. Start NanoCtrl and make sure it is reachable.
2. Make sure Redis is reachable through NanoCtrl.
3. Build and install DLSlime with Python bindings and RDMA support.

For the optional Pulsing baseline, also install `pulsing`
(`pip install pulsing`) in the same environment.

## Run

Default run (SlimeRPC + Ray, Pulsing disabled):

```bash
bash bench/python/run_rpc_bench.sh
```

Include the Pulsing baseline:

```bash
bash bench/python/run_rpc_bench.sh --with-pulsing
# or
WITH_PULSING=1 bash bench/python/run_rpc_bench.sh
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

The script always writes:

- `bench/results/slime_rpc.csv`
- `bench/results/ray_rpc.csv`

and, when `--with-pulsing` is passed, additionally writes:

- `bench/results/pulsing_rpc.csv`

It then prints a merged comparison table. The `S/Pul` column only appears in
the table when the Pulsing CSV is present.

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
