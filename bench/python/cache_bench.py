#!/usr/bin/env python3
"""Microbenchmark for DLSlimeCache metadata and assignment-directory paths.

This does not benchmark RDMA payload transfer. It measures the in-process
Python binding cost for:
  * C++ CacheServer shallow store/load/delete
  * C++ CacheServer assignment store/query/delete with slab splitting
  * a pure Python dict baseline for perspective
"""

from __future__ import annotations

import argparse
import csv
import gc
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from dlslime._slime_c import Assignment, cache


@dataclass
class Result:
    backend: str
    operation: str
    keys: int
    items_per_key: int
    seconds: float

    @property
    def ns_per_op(self) -> float:
        return self.seconds * 1e9 / self.keys

    @property
    def ops_per_sec(self) -> float:
        return self.keys / self.seconds


def make_extents(items_per_key: int, item_size: int) -> list[cache.Extent]:
    return [
        cache.Extent(
            peer="worker-a", mr_handle=7, offset=i * item_size, length=item_size
        )
        for i in range(items_per_key)
    ]


def make_assignments(items_per_key: int, item_size: int) -> list[Assignment]:
    return [
        Assignment(11, 22, i * item_size, i * item_size, item_size)
        for i in range(items_per_key)
    ]


def time_once(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def median_result(results: list[Result]) -> Result:
    best = min(
        results,
        key=lambda r: abs(r.seconds - statistics.median(x.seconds for x in results)),
    )
    return best


def bench_cpp_shallow(
    keys: int, items_per_key: int, item_size: int, repeats: int
) -> list[Result]:
    extents = make_extents(items_per_key, item_size)
    results: list[Result] = []

    for _ in range(repeats):
        srv = cache.CacheServer()
        store_s = time_once(
            lambda: [srv.store(f"k{i}", extents, mode="shallow") for i in range(keys)]
        )
        load_s = time_once(lambda: [srv.load(f"k{i}") for i in range(keys)])
        delete_s = time_once(lambda: [srv.delete(f"k{i}") for i in range(keys)])

        results.extend(
            [
                Result("cpp-shallow", "store", keys, items_per_key, store_s),
                Result("cpp-shallow", "load", keys, items_per_key, load_s),
                Result("cpp-shallow", "delete", keys, items_per_key, delete_s),
            ]
        )

    return results


def bench_cpp_assignments(
    keys: int, items_per_key: int, item_size: int, slab_size: int, repeats: int
) -> list[Result]:
    batch = make_assignments(items_per_key, item_size)
    results: list[Result] = []

    for _ in range(repeats):
        srv = cache.CacheServer(slab_size=slab_size)
        versions: list[int] = []
        store_s = time_once(
            lambda: [
                versions.append(srv.store_assignments("engine-a", batch).version)
                for _ in range(keys)
            ]
        )
        query_s = time_once(
            lambda: [srv.query_assignments("engine-a", v) for v in versions]
        )
        delete_s = time_once(
            lambda: [srv.delete_assignments("engine-a", v) for v in versions]
        )
        results.extend(
            [
                Result("cpp-assign", "store", keys, items_per_key, store_s),
                Result("cpp-assign", "query", keys, items_per_key, query_s),
                Result("cpp-assign", "delete", keys, items_per_key, delete_s),
            ]
        )

    return results


def bench_python_dict(
    keys: int, items_per_key: int, item_size: int, repeats: int
) -> list[Result]:
    extents = [(i * item_size, item_size) for i in range(items_per_key)]
    results: list[Result] = []

    for _ in range(repeats):
        d: dict[str, list[tuple[int, int]]] = {}
        store_s = time_once(
            lambda: [d.__setitem__(f"k{i}", extents) for i in range(keys)]
        )
        load_s = time_once(lambda: [d.get(f"k{i}") for i in range(keys)])
        delete_s = time_once(lambda: [d.pop(f"k{i}", None) for i in range(keys)])
        results.extend(
            [
                Result("python-dict", "store", keys, items_per_key, store_s),
                Result("python-dict", "load", keys, items_per_key, load_s),
                Result("python-dict", "delete", keys, items_per_key, delete_s),
            ]
        )

    return results


def collapse(results: list[Result]) -> list[Result]:
    collapsed: list[Result] = []
    keys = sorted({(r.backend, r.operation) for r in results})
    for backend, operation in keys:
        group = [
            r for r in results if r.backend == backend and r.operation == operation
        ]
        collapsed.append(median_result(group))
    return collapsed


def print_table(results: list[Result]) -> None:
    print(
        f"{'backend':<14} {'op':<8} {'keys':>8} {'items/key':>9} {'ns/op':>12} {'ops/s':>14}"
    )
    print("-" * 72)
    for r in results:
        print(
            f"{r.backend:<14} {r.operation:<8} {r.keys:>8} {r.items_per_key:>9} "
            f"{r.ns_per_op:>12.1f} {r.ops_per_sec:>14.0f}"
        )


def write_csv(path: Path, results: list[Result]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "backend",
                "operation",
                "keys",
                "items_per_key",
                "seconds",
                "ns_per_op",
                "ops_per_sec",
            ],
        )
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "backend": r.backend,
                    "operation": r.operation,
                    "keys": r.keys,
                    "items_per_key": r.items_per_key,
                    "seconds": f"{r.seconds:.9f}",
                    "ns_per_op": f"{r.ns_per_op:.1f}",
                    "ops_per_sec": f"{r.ops_per_sec:.0f}",
                }
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--keys", type=int, default=100_000)
    p.add_argument("--items-per-key", type=int, default=1)
    p.add_argument("--item-size", type=int, default=16 * 1024)
    p.add_argument("--slab-size", type=int, default=256 * 1024)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--csv", type=Path, default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        all_results = []
        all_results.extend(
            bench_python_dict(
                args.keys, args.items_per_key, args.item_size, args.repeats
            )
        )
        all_results.extend(
            bench_cpp_shallow(
                args.keys, args.items_per_key, args.item_size, args.repeats
            )
        )
        all_results.extend(
            bench_cpp_assignments(
                args.keys,
                args.items_per_key,
                args.item_size,
                args.slab_size,
                args.repeats,
            )
        )
        results = collapse(all_results)
    finally:
        if gc_was_enabled:
            gc.enable()

    print_table(results)
    if args.csv is not None:
        write_csv(args.csv, results)


if __name__ == "__main__":
    main()
