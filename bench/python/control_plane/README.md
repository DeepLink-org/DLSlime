# Control-plane experiments

Benchmarks comparing how two control-plane styles behave under transient
conditions (bootstrap races, scale-out). Steady-state latency is *not* the
goal here — that's what `bench/python/agg_transfer_bench_spmd.py` measures.
These experiments target MTTR and bootstrap tail latency, which is where
the Mooncake-vs-NanoCtrl design difference actually shows up.

Philosophy behind each experiment is in `docs/design/control-plane-vs-mooncake.md`.

## Backends

| flag                           | what it drives                                                                              |
| ------------------------------ | ------------------------------------------------------------------------------------------- |
| `--backend dlslime-peer-agent` | `start_peer_agent` + `set_desired_topology` (NanoCtrl + Redis Streams reconciler).          |
| `--backend mooncake`           | `mooncake.engine.TransferEngine` against a Redis / HTTP metadata store (or `P2PHANDSHAKE`). |

## Prerequisites

- Ray (`pip install ray`) — already a dep for `rpc_bench_ray.py`.
- For `dlslime-peer-agent`: a running NanoCtrl (`http://127.0.0.1:3000`) and Redis (`127.0.0.1:6379`).
- For `mooncake`: the `mooncake` Python package importable. The default
  metadata mode is `P2PHANDSHAKE` (Mooncake's built-in zero-external-store
  handshake) which needs no extra service. If you want to use
  `redis://...` / `http://...` / `etcd://...` the installed Mooncake build
  must have that plugin compiled in — missing plugins surface as
  `transfer_metadata_plugin.cpp:590 Unable to find metadata storage plugin ...`
  followed by SIGABRT.
- At least one RDMA-capable NIC — same requirement as the rest of `bench/python`.

## Running

### Exp 2 — bootstrap race

N pairs, each with `rand(0..5s)` startup stagger. Measures time from A
process-up to first successful A→B transfer.

```bash
# NanoCtrl reconciler path
python -m bench.python.control_plane.exp2_bootstrap_race \
    --backend dlslime-peer-agent --num-trials 100 \
    --out bench/results/exp2_dlslime.csv

# Mooncake (default: P2PHANDSHAKE — no external metadata store)
python -m bench.python.control_plane.exp2_bootstrap_race \
    --backend mooncake --num-trials 100 \
    --out bench/results/exp2_mooncake.csv

# Mooncake against an external metadata store (only if the build has the
# matching plugin compiled in — fails fast with SIGABRT otherwise):
python -m bench.python.control_plane.exp2_bootstrap_race \
    --backend mooncake --num-trials 100 \
    --mooncake-metadata-conn redis://127.0.0.1:6379 \
    --out bench/results/exp2_mooncake_redis.csv
```

Each run prints a summary (`mean / P50 / P99 / max`) and writes per-trial
rows to the CSV for post-processing.

### Exp 4 — elastic scale-out

Start M peers fully connected, then add peer M+1. Measures max per-edge
latency from new-peer process-up to full bidirectional connectivity.

```bash
python -m bench.python.control_plane.exp4_scaleout \
    --backend dlslime-peer-agent --mesh-size 8 --num-trials 10 \
    --out bench/results/exp4_dlslime.csv

# Show the pure declarative win: existing peers run zero application code.
python -m bench.python.control_plane.exp4_scaleout \
    --backend dlslime-peer-agent --mesh-size 8 --num-trials 10 \
    --no-touch-existing \
    --out bench/results/exp4_dlslime_symmetric.csv

python -m bench.python.control_plane.exp4_scaleout \
    --backend mooncake --mesh-size 8 --num-trials 10 \
    --out bench/results/exp4_mooncake.csv
```

## What to look at in the output

- **P99 and max** matter more than mean. The reconciler advantage shows up
  in tails — steady-state the two styles are comparable.
- For `exp4 mooncake`, `max_edge_sec` per trial is the O(N) caller cost
  the hypothesis talks about.
- For `exp4 dlslime-peer-agent --no-touch-existing`, `t_wallclock_sec`
  approximates the reconciler's end-to-end latency for one topology edit.

## Not yet implemented

- Exp 1 (NIC blip) and Exp 3 (churn) require privileged operations
  (`ip link set`, SIGKILL across host boundaries) and per-peer stale-QP
  monitoring via `rdma statistic`. Add alongside when you have a host
  where blasting the NIC is OK.
