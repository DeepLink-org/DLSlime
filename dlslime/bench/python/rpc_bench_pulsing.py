#!/usr/bin/env python3
"""Pulsing RPC benchmark — same-machine CPU bytes echo.

Measures round-trip latency for bytes echo via a Pulsing actor across a range
of payload sizes, using identical metrics as rpc_bench_slime_driver.py and
rpc_bench_ray.py so the three CSVs can be compared directly.

Usage:
    python rpc_bench_pulsing.py [--out results/pulsing_rpc.csv]
"""

import argparse
import asyncio
import csv
import os
import time

import pulsing as pul


@pul.remote
class EchoActor:
    def echo(self, data: bytes) -> bytes:
        return data


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


async def _benchmark(actor, data: bytes, warmup: int, iterations: int) -> dict:
    for _ in range(warmup):
        await actor.echo(data)

    latencies_us = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        await actor.echo(data)
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


async def _run(out_path: str):
    await pul.init()
    try:
        actor = await EchoActor.spawn()

        header = (
            f"{'Size':<10} | {'Avg (µs)':<12} | {'P50 (µs)':<12} "
            f"| {'P99 (µs)':<12} | {'BW (GB/s)':<10}"
        )
        print(header)
        print("-" * len(header))

        records = []
        for size in SIZES:
            data = bytes(size)
            iters = max(50, min(500, 50_000_000 // size))
            r = await _benchmark(actor, data, warmup=20, iterations=iters)
            records.append({"size": size, **r})
            print(
                f"{_label(size):<10} | {r['avg_us']:<12.1f} | {r['p50_us']:<12.1f} "
                f"| {r['p99_us']:<12.1f} | {r['bw_gbps']:<10.3f}"
            )

        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["size", "avg_us", "p50_us", "p99_us", "bw_gbps"]
            )
            writer.writeheader()
            writer.writerows(records)
        print(f"\nResults saved → {out_path}")
    finally:
        await pul.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Pulsing RPC benchmark")
    parser.add_argument(
        "--out",
        default=os.path.join(
            os.path.dirname(__file__), "..", "results", "pulsing_rpc.csv"
        ),
    )
    args = parser.parse_args()
    asyncio.run(_run(args.out))


if __name__ == "__main__":
    main()
