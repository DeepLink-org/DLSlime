"""Exp 4 — Elastic scale-out.

Hypothesis
----------
Adding an (N+1)-th peer to a mesh forces O(N) caller-side action in
Mooncake but zero in NanoCtrl's declarative model. The time-to-full-
connectivity should be bounded by the reconciler tick for NanoCtrl and
by the existing peers' probe interval for Mooncake.

Setup
-----
1. Start M peers, fully connected (mesh).
2. Verify the mesh: every peer can transfer to every other peer.
3. Start peer (M+1).
4. Measure time from peer (M+1) process-up to:
   - every existing peer has successfully transferred to peer (M+1), and
   - peer (M+1) has successfully transferred to every existing peer.

What the driver DOES differ by backend
--------------------------------------
- dlslime-peer-agent:
    Existing peers' desired_topology is extended to include the new peer
    (one `set_desired_topology` call per peer). The reconciler converges
    without any application-layer probe loop. Measurement is reconciler
    latency end-to-end.
    To also demonstrate the symmetric-rendezvous win: run with
    --no-touch-existing; only the *new* peer declares intent and relies
    on the always-symmetric merge to populate existing peers' specs
    server-side.
- mooncake:
    Every existing peer must poll the new peer until a transfer succeeds.
    That polling loop IS the "O(N) caller-side action" the hypothesis
    talks about; this script writes it, times it, and reports the tail.

Usage
-----
  python -m bench.python.control_plane.exp4_scaleout \
      --backend dlslime-peer-agent --mesh-size 8 --num-trials 10 \
      --out bench/results/exp4_dlslime.csv

  python -m bench.python.control_plane.exp4_scaleout \
      --backend mooncake --mesh-size 8 --num-trials 10 \
      --out bench/results/exp4_mooncake.csv
"""

from __future__ import annotations

import argparse
import time
import traceback
from typing import List, Optional

import ray

from .common import (
    BackendConfig,
    make_actor,
    print_summary,
    TrialResult,
    unique_scope,
    write_csv,
)


def parse_args() -> argparse.Namespace:
    """Return parsed CLI arguments."""
    p = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    p.add_argument(
        "--backend",
        required=True,
        choices=["dlslime-peer-agent", "mooncake"],
    )
    p.add_argument(
        "--mesh-size", type=int, default=8, help="M existing peers before scale-out"
    )
    p.add_argument("--num-trials", type=int, default=10)
    p.add_argument("--poll-interval-sec", type=float, default=0.1)
    p.add_argument("--connect-timeout-sec", type=float, default=30.0)
    p.add_argument(
        "--no-touch-existing",
        action="store_true",
        help=(
            "PeerAgent only: rely on NanoCtrl's symmetric mode — only the new "
            "peer declares intent; existing peers' specs are merged server-side. "
            "Shows the 'zero application-code action' scenario."
        ),
    )
    p.add_argument("--ib-port", type=int, default=1)
    p.add_argument("--link-type", default="RoCE")
    p.add_argument("--qp-num", type=int, default=1)
    p.add_argument("--nanoctrl-url", default="http://127.0.0.1:3000")
    p.add_argument(
        "--mooncake-metadata-conn",
        default="P2PHANDSHAKE",
        help=(
            "Metadata store for Mooncake. P2PHANDSHAKE (default) uses the "
            "built-in zero-metadata-store handshake. Other options "
            "(redis://..., http://..., etcd://...) only work if the installed "
            "Mooncake build has the matching plugin compiled in."
        ),
    )
    p.add_argument("--ray-address", default="local")
    p.add_argument("--out", default="bench/results/exp4_scaleout.csv")
    return p.parse_args()


def _start_mesh(cfg: BackendConfig, aliases: List[str]) -> dict:
    """Start M actors and fully connect them. Returns {alias: (actor, info)}."""
    actors = {a: make_actor(cfg, a) for a in aliases}
    infos = ray.get([actors[a].start.remote() for a in aliases])
    peers = dict(zip(aliases, infos))

    if cfg.backend == "dlslime-peer-agent":
        # Full mesh via declarative topology.
        futs = []
        for a in aliases:
            targets = [b for b in aliases if b != a]
            futs.append(actors[a].set_desired_topology.remote(targets))
        ray.get(futs)

    # Force a successful initial transfer on every edge so we know the mesh
    # is actually up before we add peer (M+1).
    verify_futs = []
    for src in aliases:
        for dst in aliases:
            if src == dst:
                continue
            verify_futs.append(actors[src].connect_to.remote(dst, peers[dst]))
    ray.get(verify_futs)
    return {"actors": actors, "peers": peers}


def _connect_all_to_new(
    mesh: dict,
    new_alias: str,
    new_info: dict,
) -> List[float]:
    """Each existing peer tries to transfer to new peer. Returns per-peer
    elapsed seconds from `connect_to` invocation to first success."""
    existing = [a for a in mesh["actors"] if a != new_alias]
    futs = {
        a: mesh["actors"][a].connect_to.remote(new_alias, new_info) for a in existing
    }
    return ray.get([futs[a] for a in existing])


def run_one_trial(trial_id: int, args: argparse.Namespace) -> TrialResult:
    """One scale-out trial: M peers + one new peer."""
    cfg = BackendConfig(
        backend=args.backend,
        ib_port=args.ib_port,
        link_type=args.link_type,
        qp_num=args.qp_num,
        poll_interval_sec=args.poll_interval_sec,
        connect_timeout_sec=args.connect_timeout_sec,
        nanoctrl_url=args.nanoctrl_url,
        scope=unique_scope("exp4"),
        mooncake_metadata_conn=args.mooncake_metadata_conn,
    )

    aliases = [f"peer-{trial_id:03d}-{i:02d}" for i in range(args.mesh_size)]
    new_alias = f"peer-{trial_id:03d}-new"

    mesh = None
    new_actor = None
    elapsed: Optional[float] = None
    extras = {"mesh_size": args.mesh_size}
    try:
        mesh = _start_mesh(cfg, aliases)

        # --- T0: peer (M+1) comes up ---
        t0 = time.perf_counter()
        new_actor = make_actor(cfg, new_alias)
        new_info = ray.get(new_actor.start.remote())
        mesh["actors"][new_alias] = new_actor
        mesh["peers"][new_alias] = new_info

        if cfg.backend == "dlslime-peer-agent":
            # New peer declares full intent. The control plane is always
            # symmetric, so NanoCtrl also merges the new peer into every
            # existing peer's spec — no application code on existing peers
            # runs when --no-touch-existing is set.
            existing = [a for a in aliases]
            ray.get(new_actor.set_desired_topology.remote(existing))
            if not args.no_touch_existing:
                # Also have existing peers declare the new peer. This
                # measures how long reconciliation takes when both sides
                # push the spec simultaneously (the common real-world case).
                ray.get(
                    [
                        mesh["actors"][a].set_desired_topology.remote(
                            [b for b in aliases + [new_alias] if b != a]
                        )
                        for a in aliases
                    ]
                )

        # Existing → new and new → existing. The existing-side probe loop
        # is what this experiment is fundamentally measuring.
        existing_to_new = _connect_all_to_new(mesh, new_alias, new_info)

        # New peer's view: probe every existing peer.
        new_to_existing_futs = [
            new_actor.connect_to.remote(a, mesh["peers"][a]) for a in aliases
        ]
        new_to_existing = ray.get(new_to_existing_futs)

        per_edge_latencies = list(existing_to_new) + list(new_to_existing)
        elapsed = max(per_edge_latencies)

        extras.update(
            {
                "p50_edge_sec": f"{sorted(per_edge_latencies)[len(per_edge_latencies)//2]:.6f}",
                "max_edge_sec": f"{max(per_edge_latencies):.6f}",
                "t_wallclock_sec": f"{time.perf_counter() - t0:.6f}",
            }
        )

    except Exception as e:  # noqa: BLE001 — benchmark harness
        print(f"[trial {trial_id}] FAILED: {e!r}")
        traceback.print_exc()
        elapsed = None
    finally:
        if mesh is not None:
            for a, actor in mesh["actors"].items():
                try:
                    ray.get(actor.shutdown.remote(), timeout=10)
                except Exception:
                    pass
                try:
                    ray.kill(actor)
                except Exception:
                    pass

    return TrialResult(
        trial_id=trial_id,
        backend=cfg.backend,
        time_to_first_xfer_sec=elapsed,
        extras=extras,
    )


def main() -> None:
    """Entry point."""
    args = parse_args()

    ray.init(
        address=args.ray_address,
        include_dashboard=False,
        ignore_reinit_error=True,
    )

    results = []
    try:
        for trial_id in range(args.num_trials):
            r = run_one_trial(trial_id, args)
            results.append(r)
            status = (
                f"max_edge={r.time_to_first_xfer_sec*1e3:.1f}ms"
                if r.time_to_first_xfer_sec is not None
                else "FAILED"
            )
            print(f"[trial {trial_id:3d}] mesh={args.mesh_size} -> {status}")
    finally:
        write_csv(args.out, results)
        print_summary(f"Exp 4 / {args.backend} (mesh={args.mesh_size})", results)
        print(f"CSV → {args.out}")
        ray.shutdown()


if __name__ == "__main__":
    main()
