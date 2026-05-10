"""Tests for the ObsReporter daemon thread."""

import json
import os
import threading
import time
import types

import pytest


def _try_import_accounting():
    try:
        from dlslime.peer_agent._accounting import ObsReporter

        return ObsReporter
    except ImportError:
        pytest.skip("dlslime.peer_agent._accounting not available")


class FakeAgent:
    """Minimal mock of PeerAgent for ObsReporter tests."""

    def __init__(self, alias="test-agent", scope="test-scope"):
        self.alias = alias
        self._scope = scope
        self._connections_lock = threading.Lock()
        self._connections = {}
        self._connected_set: set = set()
        self._redis_client = None

    def _is_connection_connected(self, conn_id: str) -> bool:
        return conn_id in self._connected_set


def _make_fake_directed_conn(
    peer_alias: str = "peer-B",
    local_device: str = "mlx5_0",
    remote_device: str = "mlx5_1",
    state: str = "connected",
) -> types.SimpleNamespace:
    """Build a duck-typed DirectedConnection fit for _gather_connections()."""
    return types.SimpleNamespace(
        peer_alias=peer_alias,
        local_key=types.SimpleNamespace(device=local_device),
        peer_key=types.SimpleNamespace(device=remote_device),
        state=state,
    )


class TestObsReporterUnit:
    """Unit tests for ObsReporter (no Redis needed)."""

    def test_reporter_starts_and_stops(self, monkeypatch):
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_TIME_STEP_MS", "100")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "0")  # disable Redis

        ObsReporter = _try_import_accounting()
        agent = FakeAgent()
        reporter = ObsReporter(agent)

        reporter.start()
        assert reporter._thread is not None
        assert reporter._thread.is_alive()

        time.sleep(0.3)  # let a few ticks happen
        reporter.stop()
        assert reporter._thread is None

    def test_reporter_session_id(self, monkeypatch):
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "0")

        ObsReporter = _try_import_accounting()
        agent = FakeAgent(alias="my-agent")
        reporter = ObsReporter(agent)

        assert reporter._session_id.startswith("my-agent:")
        parts = reporter._session_id.split(":")
        assert len(parts) == 3
        assert int(parts[1]) == os.getpid()

    def test_gather_connections_empty(self, monkeypatch):
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "0")

        ObsReporter = _try_import_accounting()
        agent = FakeAgent()
        reporter = ObsReporter(agent)

        conns = reporter._gather_connections()
        assert conns == []

    def test_gather_connections_with_directed_connection(self, monkeypatch):
        """_gather_connections must use conn.state + agent._is_connection_connected,
        not a nonexistent DirectedConnection.is_connected() method."""
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "0")

        ObsReporter = _try_import_accounting()
        agent = FakeAgent()
        reporter = ObsReporter(agent)

        conn_id = "peer-B:mlx5_0->peer-B:mlx5_1"
        agent._connections[conn_id] = _make_fake_directed_conn()
        agent._connected_set.add(conn_id)

        conns = reporter._gather_connections()
        assert len(conns) == 1
        c = conns[0]
        assert c["conn_id"] == conn_id
        assert c["peer"] == "peer-B"
        assert c["local_nic"] == "mlx5_0"
        assert c["remote_nic"] == "mlx5_1"
        assert c["state"] == "connected"
        assert c["connected"] is True

    def test_gather_connections_disconnected(self, monkeypatch):
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "0")

        ObsReporter = _try_import_accounting()
        agent = FakeAgent()
        reporter = ObsReporter(agent)

        conn_id = "not-yet-ready"
        agent._connections[conn_id] = _make_fake_directed_conn(state="connecting")
        # _connected_set intentionally empty

        conns = reporter._gather_connections()
        assert len(conns) == 1
        assert conns[0]["state"] == "connecting"
        assert conns[0]["connected"] is False


class TestObsReporterRedis:
    """Integration tests requiring a local Redis instance."""

    @pytest.fixture
    def redis_client(self):
        try:
            import redis as redis_mod

            client = redis_mod.Redis(host="127.0.0.1", port=6379, decode_responses=True)
            client.ping()
            yield client
            # Cleanup
            for key in client.scan_iter("test-obs:obs:peer:*"):
                client.delete(key)
        except Exception:
            pytest.skip("Redis not available at localhost:6379")

    def test_reporter_writes_to_redis(self, monkeypatch, redis_client):
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_TIME_STEP_MS", "200")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "1")

        ObsReporter = _try_import_accounting()
        agent = FakeAgent(alias="redis-test-agent", scope="test-obs")
        agent._redis_client = redis_client

        reporter = ObsReporter(agent)
        reporter.start()

        # Wait for at least one alive tick to write — then read before
        # stop() emits the final stopped snapshot (which overwrites the
        # same key with a shorter TTL and status="stopped").
        time.sleep(1.0)

        key = "test-obs:obs:peer:redis-test-agent"
        val = redis_client.get(key)
        reporter.stop()

        if val is None:
            pytest.skip(
                "Reporter did not write (obs might not be enabled at C++ level)"
            )

        snap = json.loads(val)

        # Full schema v0: every documented field must be present.
        for k in (
            "schema_version",
            "session_id",
            "peer_id",
            "host",
            "pid",
            "reported_at_ms",
            "summary",
            "nics",
            "connections",
            "ewma_bandwidth_bps",
        ):
            assert k in snap, f"missing schema field: {k}"

        assert snap["schema_version"] == 1
        assert snap["peer_id"] == "redis-test-agent"
        assert snap["pid"] == os.getpid()
        assert isinstance(snap["summary"], dict)
        assert isinstance(snap["nics"], list)
        assert isinstance(snap["connections"], list)
        # Alive snapshot must NOT carry the stopped marker.
        assert snap.get("status") != "stopped"

    def test_reporter_writes_stopped_snapshot_on_stop(self, monkeypatch, redis_client):
        """On graceful stop() the reporter must publish a final snapshot with
        status="stopped" so operators see the transition immediately instead
        of waiting for Redis TTL to evict the last alive snapshot."""
        monkeypatch.setenv("DLSLIME_OBS", "1")
        monkeypatch.setenv("DLSLIME_OBS_TIME_STEP_MS", "200")
        monkeypatch.setenv("DLSLIME_OBS_REDIS", "1")

        ObsReporter = _try_import_accounting()
        agent = FakeAgent(alias="stopped-test-agent", scope="test-obs")
        agent._redis_client = redis_client

        reporter = ObsReporter(agent)
        reporter.start()
        time.sleep(0.6)  # at least one alive tick
        reporter.stop()  # must emit final stopped snapshot synchronously

        key = "test-obs:obs:peer:stopped-test-agent"
        val = redis_client.get(key)
        if val is None:
            pytest.skip(
                "Reporter did not write (obs might not be enabled at C++ level)"
            )

        snap = json.loads(val)
        assert snap.get("status") == "stopped"
        assert snap["peer_id"] == "stopped-test-agent"

        # Stopped snapshots use a short TTL so they clear quickly.
        ttl = redis_client.pttl(key)
        assert 0 < ttl <= 15_000, f"stopped snapshot TTL should be short; got {ttl}"
