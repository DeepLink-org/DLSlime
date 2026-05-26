#!/usr/bin/env python3
"""Ray RPC benchmark — same-machine CPU bytes echo.

Measures round-trip latency for bytes echo via a Ray actor across a range of
payload sizes, using identical metrics as rpc_bench_slime_driver.py so the
two CSVs can be compared directly.

Note: Ray uses shared memory (plasma store) for large objects on the same
machine, which bypasses network entirely.  This is intentionally included as a
baseline for what the best achievable latency looks like on the same host.

Usage:
    python rpc_bench_ray.py [--out results/ray_rpc.csv] [--address local]
"""

import argparse
import csv
import os
import time

import ray

# ── Ray actor ────────────────────────────────────────────────────────────────


@ray.remote
class EchoActor:
    def echo(self, data: bytes) -> bytes:
        return data


# ── Sizes must match SlimeRPC benchmark ──────────────────────────────────────

SIZES = [
    1 * 1024,
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1 * 1024 * 1024,
    4 * 1024 * 1024,
    16 * 1024 * 1024,
    64 * 1024 * 1024,
]


# ── Benchmark logic ──────────────────────────────────────────────────────────


def _benchmark(actor, data: bytes, warmup: int, iterations: int) -> dict:
    for _ in range(warmup):
        ray.get(actor.echo.remote(data))

    latencies_us = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        ray.get(actor.echo.remote(data))
        latencies_us.append((time.perf_counter() - t0) * 1e6)

    latencies_us.sort()
    n = len(latencies_us)
    avg = sum(latencies_us) / n
    return {
        "avg_us": avg,
        "p50_us": latencies_us[n // 2],
        "p99_us": latencies_us[int(n * 0.99)],
        "bw_gbps": (2 * len(data)) / 1e9 / (avg / 1e6),
    }


def _label(size: int) -> str:
    return f"{size // 1024}KB" if size < 1024 * 1024 else f"{size // (1024 * 1024)}MB"


def main():
    parser = argparse.ArgumentParser(description="Ray RPC benchmark")
    parser.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "..", "results", "ray_rpc.csv"),
    )
    parser.add_argument(
        "--address",
        default="local",
        help=(
            "Ray address to use. Defaults to 'local' so the benchmark always "
            "starts an isolated same-machine Ray runtime instead of attaching "
            "to an existing cluster from the environment."
        ),
    )
    args = parser.parse_args()

    init_kwargs = {
        "address": args.address,
        "include_dashboard": False,
        "ignore_reinit_error": True,
    }
    if args.address in ("", "local", None):
        init_kwargs["num_cpus"] = 2

    ray.init(**init_kwargs)
    actor = EchoActor.remote()

    header = f"{'Size':<10} | {'Avg (µs)':<12} | {'P50 (µs)':<12} | {'P99 (µs)':<12} | {'BW (GB/s)':<10}"
    print(header)
    print("-" * len(header))

    records = []
    for size in SIZES:
        data = bytes(size)
        iters = max(50, min(500, 50_000_000 // size))
        r = _benchmark(actor, data, warmup=20, iterations=iters)
        records.append({"size": size, **r})
        print(
            f"{_label(size):<10} | {r['avg_us']:<12.1f} | {r['p50_us']:<12.1f} "
            f"| {r['p99_us']:<12.1f} | {r['bw_gbps']:<10.3f}"
        )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["size", "avg_us", "p50_us", "p99_us", "bw_gbps"]
        )
        writer.writeheader()
        writer.writerows(records)
    print(f"\nResults saved → {args.out}")

    ray.shutdown()


if __name__ == "__main__":
    main()
