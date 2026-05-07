"""Python wrapper tests for dlslime.cache."""

import pytest
from dlslime._slime_c import Assignment
from dlslime.cache import (
    assignment_to_tuple,
    CacheClient,
    CacheService,
    manifest_to_json,
)


class DummyPeerAgent:
    alias = "engine:0"
    nanoctrl_url = "http://127.0.0.1:3000"
    _redis_key_prefix = "s0"

    def __init__(self):
        self.connections = []

    class Connection:
        def __init__(self, owner):
            self.owner = owner

        def wait(self, timeout=60.0):
            self.owner.waited = timeout
            return self

    def register_memory_region(self, name, ptr, offset, length):
        self.registered = (name, ptr, offset, length)
        return 123

    def get_resource(self):
        return {
            "nics": [{"name": "mlx5_0", "ports": [{"port": 1, "link_type": "RoCE"}]}]
        }

    def connect_to(self, peer_alias, **kwargs):
        self.connections.append((peer_alias, kwargs))
        return self.Connection(self)


def test_cache_service_store_query_splits_slabs():
    service = CacheService(
        DummyPeerAgent(), slab_size=128 * 1024, memory_size=512 * 1024
    )
    assert service.peer_agent.alias == "engine:0"
    assert service.memory_size == 512 * 1024

    stored = service.store_assignments(
        "engine:0",
        [Assignment(1, 2, 0, 4096, 300 * 1024)],
    )
    queried = service.query_assignments("engine:0", stored.version)

    assert queried is not None
    assert queried.peer_agent_id == "engine:0"
    assert queried.slab_ids == [0, 1, 2]
    assert [a.length for a in queried.assignments] == [
        128 * 1024,
        128 * 1024,
        44 * 1024,
    ]
    assert [a.source_offset for a in queried.assignments] == [
        0,
        128 * 1024,
        256 * 1024,
    ]
    assert manifest_to_json(queried)["slab_ids"] == [0, 1, 2]


def test_cache_service_deletes_assignment_manifest():
    service = CacheService(
        DummyPeerAgent(), slab_size=128 * 1024, memory_size=512 * 1024
    )
    stored = service.store_assignments(
        "engine:0",
        [Assignment(1, 2, 0, 0, 64)],
    )

    assert service.delete_assignments("engine:0", stored.version) is True
    assert service.delete_assignments("engine:0", stored.version) is False
    assert service.query_assignments("engine:0", stored.version) is None


def test_assignment_to_tuple_matches_rdma_endpoint_order():
    assert assignment_to_tuple(Assignment(1, 2, 3, 4, 5)) == (1, 2, 3, 4, 5)


def test_cache_service_exposes_peer_agent_info():
    service = CacheService(
        DummyPeerAgent(), slab_size=128 * 1024, memory_size=512 * 1024
    )

    info = service.peer_agent_info()

    assert info["peer_agent_id"] == "engine:0"
    assert info["nanoctrl_url"] == "http://127.0.0.1:3000"
    assert info["scope"] == "s0"
    assert info["cache_mr_name"] == "cache"
    assert info["cache_mr_handle"] == 123
    assert info["memory_size"] == 512 * 1024
    assert info["resource"]["nics"][0]["name"] == "mlx5_0"


def test_cache_service_peer_agent_info_requires_cache_mr():
    service = CacheService(DummyPeerAgent(), slab_size=128 * 1024, memory_size=0)

    with pytest.raises(RuntimeError, match="preallocated cache memory"):
        service.peer_agent_info()


def test_cache_client_composes_peer_agent_for_default_owner_id():
    client = CacheClient(url="http://127.0.0.1:1", peer_agent=DummyPeerAgent())
    assert client.default_peer_agent_id() == "engine:0"


def test_cache_client_store_accepts_peer_agent_argument(monkeypatch):
    seen = {}

    def fake_post_json(url, path, payload, *, timeout=5.0):
        seen.update(url=url, path=path, payload=payload, timeout=timeout)
        return {"version": 1}

    monkeypatch.setattr("dlslime.cache.client.post_json", fake_post_json)

    client = CacheClient(url="http://127.0.0.1:1")
    result = client.store(
        [Assignment(1, 2, 0, 0, 64)],
        peer_agent=DummyPeerAgent(),
    )

    assert result == {"version": 1}
    assert seen["payload"]["peer_agent_id"] == "engine:0"


def test_cache_client_delete(monkeypatch):
    seen = {}

    def fake_post_json(url, path, payload, *, timeout=5.0):
        seen.update(url=url, path=path, payload=payload, timeout=timeout)
        return {"deleted": True}

    monkeypatch.setattr("dlslime.cache.client.post_json", fake_post_json)

    client = CacheClient(url="http://127.0.0.1:1")

    assert client.delete("engine:0", 7) is True
    assert seen["path"] == "/delete"
    assert seen["payload"] == {"peer_agent_id": "engine:0", "version": 7}


def test_cache_client_requires_owner_without_peer_agent():
    client = CacheClient(url="http://127.0.0.1:1")
    with pytest.raises(ValueError, match="peer_agent_id"):
        client.default_peer_agent_id()


def test_cache_client_connects_to_server_peer_agent(monkeypatch):
    def fake_get_json(url, path, *, timeout=5.0):
        assert path == "/peer-agent"
        return {
            "peer_agent_id": "cache-agent:0",
            "nanoctrl_url": "http://127.0.0.1:3000",
            "scope": "s0",
            "cache_mr_name": "cache",
        }

    monkeypatch.setattr("dlslime.cache.client.get_json", fake_get_json)
    peer_agent = DummyPeerAgent()
    client = CacheClient(url="http://127.0.0.1:1", peer_agent=peer_agent)

    info = client.connect_to_server(timeout_sec=3.0)

    assert info["peer_agent_id"] == "cache-agent:0"
    assert peer_agent.connections == [
        (
            "cache-agent:0",
            {
                "local_device": None,
                "peer_device": None,
                "ib_port": 1,
                "qp_num": 1,
            },
        )
    ]
    assert peer_agent.waited == 3.0


def test_cache_client_reports_peer_agent_restart_hint(monkeypatch):
    def fake_get_json(url, path, *, timeout=5.0):
        raise RuntimeError(
            '/peer-agent failed with HTTP 503: {"error":"cache service was started without a PeerAgent"}'
        )

    monkeypatch.setattr("dlslime.cache.client.get_json", fake_get_json)
    client = CacheClient(url="http://127.0.0.1:1")

    with pytest.raises(RuntimeError, match="dlslime-cache start --ctrl"):
        client.server_peer_agent()
