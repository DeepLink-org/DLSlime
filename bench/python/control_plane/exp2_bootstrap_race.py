"""Exp 2 — Bootstrap race.

Hypothesis
----------
Mooncake's asymmetric openSegment leaves races when both sides come up at
random times; NanoCtrl's symmetric rendezvous doesn't.

Setup
-----
For N trials:
  - start side A at t=0
  - sleep rand(0, --max-start-jitter-sec)
  - start side B
  - A tries to transfer to B every --poll-interval-sec with a
    --connect-timeout-sec budget.
Measure time-from-A-start to first-successful-transfer.

A per-trial scope (redis_key_prefix for PeerAgent, unique alias for all
backends) isolates trials from each other so we can run them sequentially
against a shared NanoCtrl / Redis / metadata server without collision.

Usage
-----
  # DLSlime PeerAgent against NanoCtrl at 127.0.0.1:3000 + Redis 6379:
  python -m bench.python.control_plane.exp2_bootstrap_race \
      --backend dlslime-peer-agent --num-trials 100 \
      --out bench/results/exp2_dlslime.csv

  # Mooncake TransferEngine (P2PHANDSHAKE — no external metadata store):
  python -m bench.python.control_plane.exp2_bootstrap_race \
      --backend mooncake --num-trials 100 \
      --out bench/results/exp2_mooncake.csv
"""

from __future__ import annotations

import argparse
import random
import time
import traceback

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
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument(
        "--backend",
        required=True,
        choices=["dlslime-peer-agent", "mooncake"],
    )
    p.add_argument("--num-trials", type=int, default=100)
    p.add_argument("--max-start-jitter-sec", type=float, default=5.0)
    p.add_argument("--poll-interval-sec", type=float, default=0.1)
    p.add_argument("--connect-timeout-sec", type=float, default=30.0)
    p.add_argument("--ib-port", type=int, default=1)
    p.add_argument("--link-type", default="RoCE")
    p.add_argument("--qp-num", type=int, default=1)
    p.add_argument("--nanoctrl-url", default="http://127.0.0.1:3000")
    p.add_argument(
        "--mooncake-metadata-conn",
        default="P2PHANDSHAKE",
        help=(
            "Metadata store for Mooncake. P2PHANDSHAKE (default) uses the "
            "built-in zero-metadata-store handshake — matches the default in "
            "bench/python/agg_transfer_bench_spmd.py. Other options "
            "(redis://..., http://..., etcd://...) only work if the installed "
            "Mooncake build has the matching plugin compiled in."
        ),
    )
    p.add_argument("--ray-address", default="local")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--out", default="bench/results/exp2_bootstrap_race.csv")
    return p.parse_args()


def run_one_trial(
    trial_id: int,
    cfg: BackendConfig,
    delay_sec: float,
) -> TrialResult:
    """One A/B pair with staggered start. Returns elapsed time from A start
    to first successful A→B transfer, or None on timeout/failure."""
    alias_a = f"probe-a-{trial_id:04d}"
    alias_b = f"probe-b-{trial_id:04d}"

    actor_a = None
    actor_b = None
    elapsed_sec = None
    try:
        # --- T0: A comes up ---
        t0 = time.perf_counter()
        actor_a = make_actor(cfg, alias_a)
        info_a = ray.get(actor_a.start.remote(), timeout=60)

        # For the declarative backend, A's desired spec includes B. The
        # control plane is always symmetric: NanoCtrl will also add A to
        # B's spec the moment B registers, so the rendezvous completes
        # regardless of startup order. This is the *fair* comparison to
        # Mooncake: A declares intent before B exists.
        if cfg.backend == "dlslime-peer-agent":
            ray.get(actor_a.set_desired_topology.remote(target_peers=[alias_b]))

        # Kick off A's probe loop in parallel with B's startup.
        connect_future = actor_a.connect_to.remote(alias_b, info_a)

        # --- T0 + delay: B comes up ---
        time.sleep(delay_sec)
        actor_b = make_actor(cfg, alias_b)
        info_b = ray.get(actor_b.start.remote(), timeout=60)

        if cfg.backend == "dlslime-peer-agent":
            # Complete the rendezvous — B also declares intent.
            ray.get(actor_b.set_desired_topology.remote(target_peers=[alias_a]))
        else:
            # For Mooncake: A's probe loop needs B's endpoint info. Pass it
            # in by restarting connect_to with the full info.
            # Cancel the initial (info-less) attempt and relaunch.
            ray.cancel(connect_future, force=False)
            try:
                ray.get(connect_future)
            except Exception:
                pass
            connect_future = actor_a.connect_to.remote(alias_b, info_b)

        elapsed_sec = ray.get(connect_future, timeout=cfg.connect_timeout_sec + 10)
        # `elapsed_sec` is measured from the moment connect_to was invoked
        # on A. For Mooncake we relaunched after B started, so we add the
        # stagger back to recover "time from A process-up to first success".
        # For PeerAgent the original call is still running so no adjustment
        # is needed.
        if cfg.backend != "dlslime-peer-agent":
            elapsed_sec += delay_sec
        else:
            # Reference t0 for PeerAgent too.
            elapsed_sec = time.perf_counter() - t0

    except Exception as e:
        print(f"[trial {trial_id}] FAILED: {e!r}")
        traceback.print_exc()
        elapsed_sec = None
    finally:
        for a in (actor_a, actor_b):
            if a is not None:
                try:
                    ray.get(a.shutdown.remote(), timeout=10)
                except Exception:
                    pass
                try:
                    ray.kill(a)
                except Exception:
                    pass

    return TrialResult(
        trial_id=trial_id,
        backend=cfg.backend,
        time_to_first_xfer_sec=elapsed_sec,
        extras={"delay_sec": f"{delay_sec:.3f}"},
    )


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    init_kwargs = {
        "address": args.ray_address,
        "include_dashboard": False,
        "ignore_reinit_error": True,
    }
    ray.init(**init_kwargs)

    results = []
    try:
        for trial_id in range(args.num_trials):
            cfg = BackendConfig(
                backend=args.backend,
                ib_port=args.ib_port,
                link_type=args.link_type,
                qp_num=args.qp_num,
                poll_interval_sec=args.poll_interval_sec,
                connect_timeout_sec=args.connect_timeout_sec,
                nanoctrl_url=args.nanoctrl_url,
                scope=unique_scope("exp2"),
                mooncake_metadata_conn=args.mooncake_metadata_conn,
            )
            delay = rng.uniform(0.0, args.max_start_jitter_sec)
            r = run_one_trial(trial_id, cfg, delay)
            results.append(r)
            status = (
                f"{r.time_to_first_xfer_sec*1e3:.1f}ms"
                if r.time_to_first_xfer_sec is not None
                else "TIMEOUT"
            )
            print(f"[trial {trial_id:3d}] delay={delay:.2f}s -> {status}")
    finally:
        write_csv(args.out, results)
        print_summary(f"Exp 2 / {args.backend}", results)
        print(f"CSV → {args.out}")
        ray.shutdown()


if __name__ == "__main__":
    main()
