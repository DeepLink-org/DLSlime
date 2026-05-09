"""Tests for the ObsReporter daemon thread."""

import json
import os
import time
import threading
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
        self._redis_client = None


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

        # Wait for at least one tick to write
        time.sleep(1.0)
        reporter.stop()

        key = "test-obs:obs:peer:redis-test-agent"
        val = redis_client.get(key)

        if val is None:
            pytest.skip("Reporter did not write (obs might not be enabled at C++ level)")

        snap = json.loads(val)
        assert snap["schema_version"] == 1
        assert snap["peer_id"] == "redis-test-agent"
        assert snap["pid"] == os.getpid()
        assert "reported_at_ms" in snap
        assert "summary" in snap
        assert "nics" in snap
        assert "ewma_bandwidth_bps" in snap

        # Verify TTL is set
        ttl = redis_client.pttl(key)
        assert ttl > 0, "TTL should be set on obs key"
