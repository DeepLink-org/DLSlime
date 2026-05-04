#!/usr/bin/env python3
"""SlimeRPC benchmark — driver (client) side.

Measures round-trip latency and bandwidth for raw-bytes echo across a range
of payload sizes.  Results are saved to a CSV for comparison with Ray.

Usage:
    python rpc_bench_slime_driver.py [--ctrl http://127.0.0.1:3000] [--out results/slime.csv]
"""

import argparse
import csv
import os
import time

from dlslime import PeerAgent
from dlslime.rpc import method, proxy as make_proxy

# ── Payload sizes to test ────────────────────────────────────────────────────

SIZES = [
    1 * 1024,  # 1 KB
    4 * 1024,  # 4 KB
    16 * 1024,  # 16 KB
    64 * 1024,  # 64 KB
    256 * 1024,  # 256 KB
    1 * 1024 * 1024,  # 1 MB
    4 * 1024 * 1024,  # 4 MB
    16 * 1024 * 1024,  # 16 MB
    64 * 1024 * 1024,  # 64 MB
]

DEFAULT_MAX_SIZE = 16 * 1024 * 1024


# ── Service stub (driver side only needs the class for tag alignment) ────────


class EchoService:
    @method(raw=True)
    def echo(self, channel, ptr, nbytes):
        pass  # never called on the driver side


# ── Benchmark logic ──────────────────────────────────────────────────────────


def _benchmark(proxy, data: bytes, warmup: int, iterations: int) -> dict:
    for _ in range(warmup):
        proxy.echo(data).wait()

    latencies_us = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        proxy.echo(data).wait()
        latencies_us.append((time.perf_counter() - t0) * 1e6)

    latencies_us.sort()
    n = len(latencies_us)
    avg = sum(latencies_us) / n
    return {
        "avg_us": avg,
        "p50_us": latencies_us[n // 2],
        "p99_us": latencies_us[int(n * 0.99)],
        # RTT carries the payload twice (send + echo reply)
        "bw_gbps": (2 * len(data)) / 1e9 / (avg / 1e6),
    }


def _label(size: int) -> str:
    return f"{size // 1024}KB" if size < 1024 * 1024 else f"{size // (1024 * 1024)}MB"


def main():
    parser = argparse.ArgumentParser(description="SlimeRPC benchmark driver")
    parser.add_argument("--ctrl", default="http://127.0.0.1:3000")
    parser.add_argument("--scope", default="rpc-bench")
    parser.add_argument(
        "--buf-mb",
        type=int,
        default=256,
        help="RPC channel buffer size in MB (must match worker)",
    )
    parser.add_argument(
        "--out",
        default=os.path.join(
            os.path.dirname(__file__), "..", "results", "slime_rpc.csv"
        ),
    )
    parser.add_argument(
        "--max-size-mb",
        type=int,
        default=DEFAULT_MAX_SIZE // (1024 * 1024),
        help=(
            "Largest payload size to benchmark in MB. Defaults to 16MB because "
            "the current raw-mailbox RPC path is stable through 16MB, while "
            "larger multi-chunk replies are still under investigation."
        ),
    )
    parser.add_argument(
        "--max-inflight",
        type=int,
        default=1,
        help=(
            "Number of mailbox slots. Default 1 picks the no-pump "
            "single-flight path (lowest latency). Set >1 to enable "
            "the pump-backed runtime and pipeline inflight calls."
        ),
    )
    args = parser.parse_args()

    buf_bytes = args.buf_mb * 1024 * 1024
    max_size_bytes = args.max_size_mb * 1024 * 1024
    # Only test sizes that fit in the buffer (payload must be < buffer size)
    sizes = [s for s in SIZES if s < buf_bytes and s <= max_size_bytes]

    driver = PeerAgent(alias="bench-driver", server_url=args.ctrl, scope=args.scope)
    driver._rpc_buffer_size = buf_bytes
    driver._rpc_max_inflight = max(1, int(getattr(args, "max_inflight", 4)))

    driver.set_desired_topology(["bench-worker"])
    print("Waiting for worker…")
    driver.wait_for_peers(["bench-worker"], timeout_sec=120)
    print("Connected. Starting benchmark.\n")

    w = make_proxy(driver, "bench-worker", EchoService)

    header = f"{'Size':<10} | {'Avg (µs)':<12} | {'P50 (µs)':<12} | {'P99 (µs)':<12} | {'BW (GB/s)':<10}"
    print(header)
    print("-" * len(header))

    records = []
    for size in sizes:
        data = bytes(size)
        iters = max(50, min(500, 50_000_000 // size))
        r = _benchmark(w, data, warmup=20, iterations=iters)
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

    driver.shutdown()


if __name__ == "__main__":
    main()
