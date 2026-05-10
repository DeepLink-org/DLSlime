"""
ObsReporter: Daemon thread that periodically snapshots C++ obs counters
and publishes them to Redis for cluster-wide visibility.

Architecture:
    C++ atomic counters  →  obs_snapshot() (pybind)  →  enrich with PeerAgent state
    →  SET {scope}:obs:peer:{peer_id}  +  PEXPIRE

Enable: DLSLIME_OBS=1
Config:
    DLSLIME_OBS_TIME_STEP_MS  (default 1000)
    DLSLIME_OBS_REDIS         (default 1, set 0 to disable Redis writes)

The snapshot JSON schema (version 1):
    {
        "schema_version": 1,
        "session_id": "{alias}:{pid}:{start_ms}",
        "peer_id": "agent-1",
        "host": "10.0.0.1",
        "pid": 1234,
        "reported_at_ms": 1715000000000,
        "summary": { ... },       // from C++ obs_snapshot()
        "nics": [ ... ],           // from C++ obs_snapshot()
        "ewma_bandwidth_bps": 0,   // computed here
        "connections": [ ... ],    // from PeerAgent state
    }
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ._agent import PeerAgent

logger = logging.getLogger("dlslime.obs")

# EWMA smoothing factor
_EWMA_ALPHA = 0.3


class ObsReporter:
    """Background thread that publishes obs snapshots to Redis."""

    def __init__(self, agent: "PeerAgent") -> None:
        self._agent = agent
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._time_step_ms = int(os.environ.get("DLSLIME_OBS_TIME_STEP_MS", "1000"))
        self._redis_enabled = os.environ.get("DLSLIME_OBS_REDIS", "1") != "0"
        self._host = socket.gethostname()
        self._pid = os.getpid()
        self._start_ms = int(time.time() * 1000)
        self._session_id = f"{agent.alias}:{self._pid}:{self._start_ms}"

        # Peer-level EWMA state
        self._prev_completed_bytes: int = 0
        self._prev_time_ms: int = self._start_ms
        self._ewma_bw_bps: float = 0.0

        # Per-NIC EWMA state. Keys are NIC device names (e.g. "mlx5_0").
        self._prev_nic_completed_bytes: Dict[str, int] = {}
        self._prev_nic_time_ms: Dict[str, int] = {}
        self._nic_ewma_bw_bps: Dict[str, float] = {}

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"obs-reporter-{self._agent.alias}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "ObsReporter started for %s (step=%dms, redis=%s)",
            self._agent.alias,
            self._time_step_ms,
            self._redis_enabled,
        )

    def stop(self) -> None:
        """Stop the reporter thread and publish one final "stopped" snapshot
        so operators see the transition immediately in `nanoctrl obs peers`
        instead of waiting for the TTL to evict the last `alive` snapshot.
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        try:
            self._emit_final_snapshot()
        except Exception:
            logger.debug("ObsReporter final snapshot failed", exc_info=True)
        logger.info("ObsReporter stopped for %s", self._agent.alias)

    def _emit_final_snapshot(self) -> None:
        """Write one last snapshot with status="stopped" so the CLI can
        distinguish clean shutdown from a crashed/stale agent.

        Uses a short TTL (max(3*step, 10 s)) — the key should disappear
        quickly after graceful shutdown, unlike alive snapshots which
        linger for the full _ttl_ms() window.
        """
        if not self._redis_enabled:
            return
        redis_client = self._get_redis_client()
        if redis_client is None:
            return

        try:
            import dlslime._slime_c as _c  # type: ignore
        except ImportError:
            return

        now_ms = int(time.time() * 1000)
        snap = _c.obs_snapshot() if _c.obs_enabled() else {}
        summary = snap.get("summary", {}) if isinstance(snap, dict) else {}
        nics = snap.get("nics", []) if isinstance(snap, dict) else []

        connections = self._gather_connections()

        full_snap: Dict[str, Any] = {
            "schema_version": 1,
            "session_id": self._session_id,
            "peer_id": self._agent.alias,
            "host": self._host,
            "pid": self._pid,
            "reported_at_ms": now_ms,
            # Explicit lifecycle marker. CLI renders this as the STATE
            # column when present, overriding the alive/stale derivation
            # that depends on reported_at_ms age.
            "status": "stopped",
            "summary": summary,
            "nics": nics,
            "ewma_bandwidth_bps": 0.0,
            "connections": connections,
        }

        scope = self._agent._scope or ""
        prefix = f"{scope}:" if scope else ""
        key = f"{prefix}obs:peer:{self._agent.alias}"
        # Short TTL — graceful shutdown should clear quickly, not linger
        # for the full alive-snapshot window.
        ttl_ms = max(3 * self._time_step_ms, 10_000)

        try:
            pipe = redis_client.pipeline(transaction=False)
            pipe.set(key, json.dumps(full_snap))
            pipe.pexpire(key, ttl_ms)
            pipe.execute()
        except Exception:
            logger.debug("ObsReporter Redis final-write failed", exc_info=True)

    def _run(self) -> None:
        try:
            import dlslime._slime_c as _c  # type: ignore
        except ImportError:
            logger.warning("Cannot import _slime_c, obs reporter disabled")
            return

        redis_client = self._get_redis_client()

        while not self._stop_event.is_set():
            try:
                self._tick(_c, redis_client)
            except Exception:
                logger.debug("ObsReporter tick error", exc_info=True)

            self._stop_event.wait(timeout=self._time_step_ms / 1000.0)

    def _tick(self, _c: Any, redis_client: Any) -> None:
        # 1. Get C++ snapshot
        snap = _c.obs_snapshot()
        if not snap.get("enabled", False):
            return

        now_ms = int(time.time() * 1000)

        # 2. Compute EWMA bandwidth at the peer level
        summary = snap.get("summary", {})
        completed_bytes = summary.get("completed_bytes_total", 0)
        delta_bytes = completed_bytes - self._prev_completed_bytes
        delta_time_s = max((now_ms - self._prev_time_ms) / 1000.0, 0.001)

        instant_bps = 8.0 * delta_bytes / delta_time_s
        self._ewma_bw_bps = (
            _EWMA_ALPHA * instant_bps + (1 - _EWMA_ALPHA) * self._ewma_bw_bps
        )

        self._prev_completed_bytes = completed_bytes
        self._prev_time_ms = now_ms

        # 2b. Compute per-NIC EWMA bandwidth. `completed_bytes_total` is
        # monotonic, so delta is always >= 0. Mutates each NIC dict in
        # place so the stamped field ends up in the Redis snapshot.
        nics_out = snap.get("nics", []) or []
        for nic in nics_out:
            nic_name = nic.get("nic") or ""
            if not nic_name:
                continue
            nic_completed = int(nic.get("completed_bytes_total", 0))
            prev_bytes = self._prev_nic_completed_bytes.get(nic_name, nic_completed)
            prev_ms = self._prev_nic_time_ms.get(nic_name, now_ms)
            prev_ewma = self._nic_ewma_bw_bps.get(nic_name, 0.0)

            nic_delta_bytes = nic_completed - prev_bytes
            nic_delta_s = max((now_ms - prev_ms) / 1000.0, 0.001)
            nic_instant_bps = 8.0 * nic_delta_bytes / nic_delta_s
            nic_ewma = _EWMA_ALPHA * nic_instant_bps + (1 - _EWMA_ALPHA) * prev_ewma

            self._prev_nic_completed_bytes[nic_name] = nic_completed
            self._prev_nic_time_ms[nic_name] = now_ms
            self._nic_ewma_bw_bps[nic_name] = nic_ewma
            nic["ewma_bandwidth_bps"] = nic_ewma

        # 3. Gather connection info
        connections = self._gather_connections()

        # 4. Build full snapshot
        full_snap: Dict[str, Any] = {
            "schema_version": 1,
            "session_id": self._session_id,
            "peer_id": self._agent.alias,
            "host": self._host,
            "pid": self._pid,
            "reported_at_ms": now_ms,
            "summary": summary,
            "nics": nics_out,
            "ewma_bandwidth_bps": self._ewma_bw_bps,
            "connections": connections,
        }

        # 5. Write to Redis
        if self._redis_enabled and redis_client is not None:
            scope = self._agent._scope or ""
            prefix = f"{scope}:" if scope else ""
            key = f"{prefix}obs:peer:{self._agent.alias}"
            # TTL is decoupled from the CLI's --stale-ms (default 45s).
            # The key must outlive the stale threshold so that after a
            # crash/hard-kill the CLI has a window to show `stale` before
            # Redis evicts the key; otherwise users only ever see
            # `alive` -> gone.
            ttl_ms = max(3 * self._time_step_ms, 180_000)

            try:
                pipe = redis_client.pipeline(transaction=False)
                pipe.set(key, json.dumps(full_snap))
                pipe.pexpire(key, ttl_ms)
                pipe.execute()
            except Exception:
                logger.debug("ObsReporter Redis write failed", exc_info=True)

    def _gather_connections(self) -> list:
        """Extract lightweight connection info from PeerAgent.

        ``conn`` is a DirectedConnection (not a PeerConnection), so it
        exposes ``state`` but no ``is_connected()`` method. Connection
        readiness is determined via ``agent._is_connection_connected``,
        which consults the separate ``_connected_peers`` set.
        """
        result = []
        try:
            agent = self._agent
            with agent._connections_lock:
                items = list(agent._connections.items())
            for conn_id, conn in items:
                try:
                    result.append(
                        {
                            "conn_id": conn_id,
                            "peer": conn.peer_alias,
                            "local_nic": conn.local_key.device,
                            "remote_nic": conn.peer_key.device,
                            "state": conn.state,
                            "connected": agent._is_connection_connected(conn_id),
                        }
                    )
                except Exception:
                    logger.debug(
                        "obs _gather_connections: failed to snapshot conn_id=%s",
                        conn_id,
                        exc_info=True,
                    )
        except Exception:
            logger.debug("obs _gather_connections failed", exc_info=True)
        return result

    def _get_redis_client(self) -> Any:
        """Get a Redis client from the PeerAgent if available."""
        if not self._redis_enabled:
            return None
        try:
            return self._agent._redis_client
        except AttributeError:
            return None
