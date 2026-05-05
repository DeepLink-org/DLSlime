"""Shared harness for control-plane experiments (bootstrap race, scale-out, ...).

Design
------
Each experiment spawns one Ray actor per peer. Actors implement a tiny
uniform interface (`PeerActor` protocol below) that hides backend-specific
bootstrap / connect / transfer logic behind three methods:

    start(peer_id: str) -> dict       # returns opaque "info" the driver
                                      # may redistribute to other peers
    connect_to(peer_id, info) -> float # returns wall-clock time-to-first-
                                      # successful-transfer, or raises
    shutdown() -> None

The driver decides *when* actors come up and hands them each other's info.

Backends
--------
- "dlslime-peer-agent":  start_peer_agent + set_desired_topology (NanoCtrl).
- "mooncake":            mooncake.engine.TransferEngine against a metadata
                         store (Redis / HTTP / etcd), or P2PHANDSHAKE for
                         the built-in zero-store handshake.

For every backend, `connect_to` polls until a small zero-length-ish test
transfer succeeds, and returns elapsed seconds since it was first invoked.
"""

from __future__ import annotations

import csv
import os
import random
import socket
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import ray

# ──────────────────────────────────────────────────────────────────────────
# Config & stats
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class BackendConfig:
    """Backend-agnostic configuration passed into each peer actor."""

    backend: str  # "dlslime-peer-agent" | "mooncake"
    # Shared
    ib_port: int = 1
    link_type: str = "RoCE"
    qp_num: int = 1
    poll_interval_sec: float = 0.1  # 100ms, matches the design doc
    connect_timeout_sec: float = 30.0
    # dlslime-peer-agent
    nanoctrl_url: str = "http://127.0.0.1:3000"
    scope: Optional[str] = None  # redis key prefix for isolation between runs
    # mooncake
    mooncake_metadata_conn: str = "P2PHANDSHAKE"


@dataclass
class TrialResult:
    trial_id: int
    backend: str
    # Time from the first peer's process-up moment to the first successful
    # transfer, in seconds. None ⇒ timed out.
    time_to_first_xfer_sec: Optional[float]
    # Backend-specific extras (e.g. delay between A and B starting, mesh size).
    extras: Dict[str, Any] = field(default_factory=dict)


def summarize(results: List[TrialResult]) -> Dict[str, float]:
    """Return mean/P50/P99/max over successful trials, and the failure rate."""
    ok = [
        r.time_to_first_xfer_sec
        for r in results
        if r.time_to_first_xfer_sec is not None
    ]
    n_total = len(results)
    n_ok = len(ok)
    if n_ok == 0:
        return {"n": n_total, "ok": 0, "failure_rate": 1.0}
    ok.sort()
    return {
        "n": n_total,
        "ok": n_ok,
        "failure_rate": (n_total - n_ok) / n_total,
        "mean_sec": statistics.mean(ok),
        "p50_sec": ok[int(n_ok * 0.50)],
        "p99_sec": ok[min(n_ok - 1, int(n_ok * 0.99))],
        "max_sec": ok[-1],
    }


def write_csv(path: str, results: List[TrialResult]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    extras_keys: List[str] = []
    for r in results:
        for k in r.extras.keys():
            if k not in extras_keys:
                extras_keys.append(k)
    fields = ["trial_id", "backend", "time_to_first_xfer_sec", *extras_keys]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            row = {
                "trial_id": r.trial_id,
                "backend": r.backend,
                "time_to_first_xfer_sec": (
                    f"{r.time_to_first_xfer_sec:.6f}"
                    if r.time_to_first_xfer_sec is not None
                    else ""
                ),
            }
            for k in extras_keys:
                row[k] = r.extras.get(k, "")
            w.writerow(row)


# ──────────────────────────────────────────────────────────────────────────
# Peer actor base class — backends plug in via subclass
# ──────────────────────────────────────────────────────────────────────────


class _PeerActorBase:
    """Shared init; subclasses implement _start / _connect_to / _shutdown."""

    def __init__(self, cfg: BackendConfig, alias: str):
        self.cfg = cfg
        self.alias = alias
        self._started = False
        self._t_started: Optional[float] = None

    # ------------------------------------------------------------------
    # Public surface exposed to the driver
    # ------------------------------------------------------------------
    def start(self) -> dict:
        """Bring the peer fully online; return whatever info other peers
        need to talk to us (may be empty if discovery is via a shared
        registry like NanoCtrl)."""
        self._t_started = time.perf_counter()
        info = self._start()
        self._started = True
        return info or {}

    def connect_to(self, peer_alias: str, peer_info: dict) -> float:
        """Block until a test transfer to `peer_alias` succeeds. Returns
        elapsed seconds from the moment this method was called."""
        if not self._started:
            raise RuntimeError(f"{self.alias}: start() not called")
        t0 = time.perf_counter()
        deadline = t0 + self.cfg.connect_timeout_sec
        last_err: Optional[Exception] = None
        while time.perf_counter() < deadline:
            try:
                self._connect_and_transfer_once(peer_alias, peer_info)
                return time.perf_counter() - t0
            except Exception as e:  # noqa: BLE001 — benchmark harness
                last_err = e
                time.sleep(self.cfg.poll_interval_sec)
        raise TimeoutError(
            f"{self.alias}: connect_to({peer_alias}) timed out after "
            f"{self.cfg.connect_timeout_sec:.1f}s; last_err={last_err!r}"
        )

    def ping(self) -> str:
        """Liveness check for the driver."""
        return self.alias

    def shutdown(self) -> None:
        try:
            self._shutdown()
        except Exception as e:  # noqa: BLE001
            print(f"{self.alias}: shutdown warning: {e!r}")

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------
    def _start(self) -> dict:
        raise NotImplementedError

    def _connect_and_transfer_once(self, peer_alias: str, peer_info: dict) -> None:
        """Attempt one full connect + test-transfer. Raise on failure."""
        raise NotImplementedError

    def _shutdown(self) -> None:
        raise NotImplementedError


# ──────────────────────────────────────────────────────────────────────────
# Backend #1 — DLSlime PeerAgent (NanoCtrl + reconciler)
# ──────────────────────────────────────────────────────────────────────────


@ray.remote(num_cpus=1)
class DlslimePeerAgentActor(_PeerActorBase):
    def __init__(self, cfg: BackendConfig, alias: str):
        super().__init__(cfg, alias)
        self._agent = None
        self._mr_tensor = None  # keep the buffer alive
        self._mr_handle: Optional[int] = None
        self._mr_name = "probe"

    def _start(self) -> dict:
        from dlslime import start_peer_agent

        self._agent = start_peer_agent(
            alias=self.alias,  # pinned so the driver can reference us by name
            server_url=self.cfg.nanoctrl_url,
            scope=self.cfg.scope,
        )
        # 4KB staging buffer — we just want a register + rdma read to succeed.
        import torch  # heavy import; defer until start()

        self._mr_tensor = torch.zeros([4096], dtype=torch.uint8, device="cpu")
        self._mr_handle = self._agent.register_memory_region(
            self._mr_name,
            self._mr_tensor.data_ptr(),
            int(self._mr_tensor.storage_offset()),
            self._mr_tensor.numel() * self._mr_tensor.itemsize,
        )
        return {"alias": self._agent.alias}

    def set_desired_topology(self, target_peers: List[str]) -> None:
        for peer in target_peers:
            self._agent.set_desired_topology(
                peer,
                ib_port=self.cfg.ib_port,
                qp_num=self.cfg.qp_num,
            )

    def connect_to(self, peer_alias: str, peer_info: dict) -> float:
        """Block on PeerAgent.wait_for_peers (event-driven since the
        condition-variable rewrite), then issue one probe transfer. No
        polling loop on the benchmark side — the elapsed time is the
        reconciler's latency from invocation + one RDMA RTT."""
        if not self._started:
            raise RuntimeError(f"{self.alias}: start() not called")
        t0 = time.perf_counter()
        self._agent.wait_for_peers(
            [peer_alias], timeout_sec=self.cfg.connect_timeout_sec
        )
        self._connect_and_transfer_once(peer_alias, peer_info)
        return time.perf_counter() - t0

    def _connect_and_transfer_once(self, peer_alias: str, peer_info: dict) -> None:
        # Resolve the peer's published MR and issue a small read. By the time
        # we get here, wait_for_peers has confirmed the reconciler finished
        # its handshake, so the lookup should not fail.
        remote_handle = self._agent.get_remote_handle(peer_alias, self._mr_name)
        slot = self._agent.read(
            peer_alias,
            [(self._mr_handle, remote_handle, 0, 0, 8)],
            None,
        )
        slot.wait()

    def _shutdown(self) -> None:
        if self._agent is not None:
            self._agent.shutdown()
            self._agent = None


# ──────────────────────────────────────────────────────────────────────────
# Backend #2 — Mooncake TransferEngine
# ──────────────────────────────────────────────────────────────────────────


@ray.remote(num_cpus=1)
class MooncakeActor(_PeerActorBase):
    """Mooncake TransferEngine wrapper.

    The Python binding does not expose `openSegment` directly — discovery is
    implicit: `transfer_sync_write(target_hostname, ...)` resolves the
    hostname through the metadata store on first use. We exploit that: a
    "connect attempt" is a tiny probe transfer; failures surface as non-zero
    status (no connection yet, peer not registered, ...) and the driver's
    poll loop retries.
    """

    def __init__(self, cfg: BackendConfig, alias: str):
        super().__init__(cfg, alias)
        self._engine = None
        self._buffer_addr: int = 0
        self._buffer_len: int = 4096
        self._hostname: str = ""
        self._rpc_port: int = 0

    def _start(self) -> dict:
        from dlslime import available_nic
        from mooncake.engine import TransferEngine

        devs = available_nic()
        if not devs:
            raise RuntimeError("no RDMA devices")
        device = devs[0]

        # Use the host IP so Mooncake's hostname-based addressing works.
        self._hostname = _local_ip()
        self._engine = TransferEngine()

        # Allocate a real free port *before* init — Mooncake binds to the
        # literal port we pass, it does not interpret :0 as "any free port".
        # There's a small TOCTOU window; benchmarks accept it.
        self._rpc_port = _pick_free_port()
        local_server_name = f"{self._hostname}:{self._rpc_port}"

        # Python-binding signature is
        #   initialize(local_server_name, metadata_conn, transport, device)
        # — see bench/python/agg_transfer_bench_spmd.py:170 for the
        # established usage. `P2PHANDSHAKE` is a sentinel for Mooncake's
        # built-in handshake without an external metadata store.
        rc = self._engine.initialize(
            local_server_name,
            self.cfg.mooncake_metadata_conn,
            "rdma",
            device,
        )
        if rc != 0:
            raise RuntimeError(
                f"Mooncake initialize failed: rc={rc} "
                f"(server={local_server_name}, metadata={self.cfg.mooncake_metadata_conn})"
            )
        # If Mooncake reallocated the port (it shouldn't, but the API
        # exposes this), re-read it.
        try:
            self._rpc_port = self._engine.get_rpc_port() or self._rpc_port
        except Exception:  # noqa: BLE001
            pass

        # Register a 4KB buffer using allocate_managed_buffer so the address
        # is one Mooncake gave us and therefore discoverable.
        self._buffer_addr = self._engine.allocate_managed_buffer(self._buffer_len)
        if self._buffer_addr == 0:
            raise RuntimeError("Mooncake allocate_managed_buffer failed")

        return {
            "alias": self.alias,
            "hostname": self._hostname,
            "rpc_port": self._rpc_port,
            "buffer_addr": self._buffer_addr,
            "buffer_len": self._buffer_len,
        }

    def _connect_and_transfer_once(self, peer_alias: str, peer_info: dict) -> None:
        if not peer_info:
            raise RuntimeError(f"no peer_info for {peer_alias}")
        target_hostname = f"{peer_info['hostname']}:{peer_info['rpc_port']}"
        peer_addr = int(peer_info["buffer_addr"])
        rc = self._engine.transfer_sync_write(
            target_hostname,
            self._buffer_addr,
            peer_addr,
            8,  # 8 bytes, smallest meaningful probe
        )
        if rc != 0:
            raise RuntimeError(f"Mooncake transfer_sync_write rc={rc}")

    def _shutdown(self) -> None:
        if self._engine is not None and self._buffer_addr:
            try:
                self._engine.free_managed_buffer(self._buffer_addr, self._buffer_len)
            except Exception:
                pass
        self._engine = None


# ──────────────────────────────────────────────────────────────────────────
# Actor factory
# ──────────────────────────────────────────────────────────────────────────


def make_actor(cfg: BackendConfig, alias: str):
    """Spawn a Ray actor for the configured backend."""
    if cfg.backend == "dlslime-peer-agent":
        return DlslimePeerAgentActor.remote(cfg, alias)
    if cfg.backend == "mooncake":
        return MooncakeActor.remote(cfg, alias)
    raise ValueError(f"unknown backend: {cfg.backend}")


# ──────────────────────────────────────────────────────────────────────────
# Misc helpers
# ──────────────────────────────────────────────────────────────────────────


def _local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def _pick_free_port() -> int:
    """Ask the kernel for an unused TCP port. Small TOCTOU window before the
    caller binds it; acceptable for benchmarks that spawn a few hundred
    actors."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def unique_scope(prefix: str) -> str:
    """Per-run scope so repeated runs don't step on each other's Redis keys."""
    return f"{prefix}-{int(time.time())}-{random.randint(0, 1 << 16):04x}"


def print_summary(title: str, results: List[TrialResult]) -> None:
    s = summarize(results)
    print(f"\n=== {title} ===")
    if s.get("ok", 0) == 0:
        print(f"  n={s['n']}  ok=0  failure_rate={s.get('failure_rate', 1.0):.2%}")
        return
    print(
        f"  n={s['n']}  ok={s['ok']}  failure_rate={s['failure_rate']:.2%}  "
        f"mean={s['mean_sec']*1e3:.1f}ms  P50={s['p50_sec']*1e3:.1f}ms  "
        f"P99={s['p99_sec']*1e3:.1f}ms  max={s['max_sec']*1e3:.1f}ms"
    )
