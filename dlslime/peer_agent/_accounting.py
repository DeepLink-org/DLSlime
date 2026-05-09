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
from typing import TYPE_CHECKING, Any, Dict, Optional

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

        self._time_step_ms = int(
            os.environ.get("DLSLIME_OBS_TIME_STEP_MS", "1000")
        )
        self._redis_enabled = os.environ.get("DLSLIME_OBS_REDIS", "1") != "0"
        self._host = socket.gethostname()
        self._pid = os.getpid()
        self._start_ms = int(time.time() * 1000)
        self._session_id = f"{agent.alias}:{self._pid}:{self._start_ms}"

        # EWMA state
        self._prev_completed_bytes: int = 0
        self._prev_time_ms: int = self._start_ms
        self._ewma_bw_bps: float = 0.0

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
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("ObsReporter stopped for %s", self._agent.alias)

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

        # 2. Compute EWMA bandwidth
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
            "nics": snap.get("nics", []),
            "ewma_bandwidth_bps": self._ewma_bw_bps,
            "connections": connections,
        }

        # 5. Write to Redis
        if self._redis_enabled and redis_client is not None:
            scope = self._agent._scope or ""
            prefix = f"{scope}:" if scope else ""
            key = f"{prefix}obs:peer:{self._agent.alias}"
            ttl_ms = max(3 * self._time_step_ms, 45000)

            try:
                pipe = redis_client.pipeline(transaction=False)
                pipe.set(key, json.dumps(full_snap))
                pipe.pexpire(key, ttl_ms)
                pipe.execute()
            except Exception:
                logger.debug("ObsReporter Redis write failed", exc_info=True)

    def _gather_connections(self) -> list:
        """Extract lightweight connection info from PeerAgent."""
        result = []
        try:
            agent = self._agent
            with agent._connections_lock:
                items = list(agent._connections.items())
            for conn_id, conn in items:
                result.append(
                    {
                        "conn_id": conn_id,
                        "peer": conn.peer_alias,
                        "local_nic": conn.local_key.device,
                        "remote_nic": conn.peer_key.device,
                        "connected": conn.is_connected(),
                    }
                )
        except Exception:
            pass
        return result

    def _get_redis_client(self) -> Any:
        """Get a Redis client from the PeerAgent if available."""
        if not self._redis_enabled:
            return None
        try:
            return self._agent._redis_client
        except AttributeError:
            return None
