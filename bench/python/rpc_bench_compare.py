#!/usr/bin/env python3
"""Print a side-by-side comparison of SlimeRPC, Pulsing, and Ray benchmark CSVs.

Usage:
    python rpc_bench_compare.py \\
        [--slime results/slime_rpc.csv] \\
        [--pulsing results/pulsing_rpc.csv] \\
        [--ray results/ray_rpc.csv]
"""

import argparse
import csv
import os


def load(path: str) -> dict[int, dict]:
    with open(path) as f:
        return {int(r["size"]): r for r in csv.DictReader(f)}


def label(size: int) -> str:
    return f"{size // 1024}KB" if size < 1024 * 1024 else f"{size // (1024 * 1024)}MB"


def main():
    default_dir = os.path.join(os.path.dirname(__file__), "..", "results")
    parser = argparse.ArgumentParser(
        description="Compare SlimeRPC vs Pulsing vs Ray benchmark results"
    )
    parser.add_argument("--slime", default=os.path.join(default_dir, "slime_rpc.csv"))
    parser.add_argument(
        "--pulsing", default=os.path.join(default_dir, "pulsing_rpc.csv")
    )
    parser.add_argument("--ray", default=os.path.join(default_dir, "ray_rpc.csv"))
    args = parser.parse_args()

    for path in (args.slime, args.pulsing, args.ray):
        if not os.path.exists(path):
            print(f"Missing results file: {path}")
            print(
                "Run rpc_bench_slime_driver.py, rpc_bench_pulsing.py, and "
                "rpc_bench_ray.py first."
            )
            return

    slime = load(args.slime)
    pulsing = load(args.pulsing)
    ray_r = load(args.ray)

    common = sorted(set(slime) & set(pulsing) & set(ray_r))
    if not common:
        print("No overlapping sizes between the three result files.")
        return

    col = 12
    sep_width = 10 + (col + 3) * 9 + (10 + 3) + 10
    sep = "-" * sep_width

    print(
        "\n┌─ Avg Latency (µs) ─────────────────────────────────────────────────────────┐"
    )
    header = (
        f"{'Size':<10} | "
        f"{'Slime avg':>{col}} | {'Slime p99':>{col}} | {'Slime BW':>{col}} | "
        f"{'Puls avg':>{col}} | {'Puls p99':>{col}} | {'Puls BW':>{col}} | "
        f"{'Ray avg':>{col}} | {'Ray p99':>{col}} | {'Ray BW':>{col}} | "
        f"{'S/Pul':>10} | {'S/Ray':>10}"
    )
    print(header)
    print(sep)

    for size in common:
        sl_avg = float(slime[size]["avg_us"])
        sl_p99 = float(slime[size]["p99_us"])
        sl_bw = float(slime[size]["bw_gbps"])
        pu_avg = float(pulsing[size]["avg_us"])
        pu_p99 = float(pulsing[size]["p99_us"])
        pu_bw = float(pulsing[size]["bw_gbps"])
        ray_avg = float(ray_r[size]["avg_us"])
        ray_p99 = float(ray_r[size]["p99_us"])
        ray_bw = float(ray_r[size]["bw_gbps"])
        sp_pul = pu_avg / sl_avg  # >1 means SlimeRPC is faster than Pulsing
        sp_ray = ray_avg / sl_avg  # >1 means SlimeRPC is faster than Ray

        print(
            f"{label(size):<10} | "
            f"{sl_avg:>{col}.1f} | {sl_p99:>{col}.1f} | {sl_bw:>{col}.3f} | "
            f"{pu_avg:>{col}.1f} | {pu_p99:>{col}.1f} | {pu_bw:>{col}.3f} | "
            f"{ray_avg:>{col}.1f} | {ray_p99:>{col}.1f} | {ray_bw:>{col}.3f} | "
            f"{'%.2fx' % sp_pul:>10} | {'%.2fx' % sp_ray:>10}"
        )

    print(sep)
    print(
        "S/Pul = Pulsing avg latency / SlimeRPC avg latency  (>1 means SlimeRPC wins)"
    )
    print(
        "S/Ray = Ray avg latency / SlimeRPC avg latency      (>1 means SlimeRPC wins)"
    )
    print()


if __name__ == "__main__":
    main()
