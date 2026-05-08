import threading

import pytest
from dlslime.peer_agent._agent import (
    DirectedConnection,
    LogicalMemoryRegion,
    MaterializedMemoryRegion,
    PeerAgent,
    RdmaResourceKey,
)


class FakeEndpoint:
    def __init__(self):
        self.read_calls = []
        self.write_calls = []
        self.write_with_imm_calls = []
        self.send_calls = []

    def read(self, assign, stream=None):
        self.read_calls.append((assign, stream))
        return "read-future"

    def write(self, assign, stream=None):
        self.write_calls.append((assign, stream))
        return "write-future"

    def write_with_imm(self, assign, imm_data=0, stream=None):
        self.write_with_imm_calls.append((assign, imm_data, stream))
        return "write-imm-future"

    def send(self, chunk, stream=None):
        self.send_calls.append((chunk, stream))
        return "send-future"


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.deleted = []

    def set(self, key, value):
        self.values[key] = value

    def delete(self, *keys):
        self.deleted.extend(keys)
        for key in keys:
            self.values.pop(key, None)
            self.hashes.pop(key, None)

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)


class FakePool:
    def __init__(self):
        self.unregistered = []

    def unregister_memory_region(self, handle):
        self.unregistered.append(handle)
        return 0


def _agent(endpoint):
    agent = PeerAgent.__new__(PeerAgent)
    agent.alias = "local"
    agent._shutdown_called = True
    agent._connections_lock = threading.Lock()
    conn = DirectedConnection(
        agent=agent,
        peer_alias="peer",
        local_key=RdmaResourceKey("mlx5_0", 1, "RoCE"),
        peer_key=RdmaResourceKey("mlx5_1", 1, "RoCE"),
        qp_num=1,
    )
    conn.attach_endpoint(endpoint, memory_pool=None)
    conn.mark_connected()
    agent._connections = {conn.conn_id: conn}
    agent._connected_peers = {conn.conn_id}
    agent._connected_peers_lock = threading.Lock()
    agent._endpoints = {conn.conn_id: endpoint}
    agent._endpoints_lock = threading.Lock()

    def get_handle(region, peer_alias=None, resource_key=None, endpoint=None):
        if peer_alias is None:
            assert resource_key == RdmaResourceKey("mlx5_0", 1, "RoCE")
            return {"kv": 11, "scratch": 12}[region]
        assert peer_alias == "peer"
        assert resource_key == RdmaResourceKey("mlx5_1", 1, "RoCE")
        assert isinstance(endpoint, FakeEndpoint)
        return {"kv": 21, "kv_remote": 22, "remote_scratch": 23}[region]

    agent.get_handle = get_handle
    return agent


def test_peer_agent_unregister_memory_region_removes_local_and_published_state():
    agent = PeerAgent.__new__(PeerAgent)
    agent.alias = "local"
    agent._shutdown_called = True
    agent._redis_key_prefix = ""
    agent._redis_client = FakeRedis()
    agent._local_resource = {"memory_keys": ["kv"]}
    agent._logical_regions = {"kv": LogicalMemoryRegion("kv", 1000, 0, 64)}
    agent._regions_lock = threading.Lock()
    agent._mr_info_cache = {
        ("local", "kv", ""): {"rkey": 1},
        ("peer", "kv", ""): {"rkey": 2},
    }
    agent._mr_info_cache_lock = threading.Lock()
    key = RdmaResourceKey("mlx5_0", 1, "RoCE")
    agent._materialized_regions = {
        ("kv", key): MaterializedMemoryRegion("kv", key, 7, {"rkey": 1})
    }
    pool = FakePool()
    agent._get_context_and_pool = lambda resource_key: (object(), pool)

    base_key = "mr:local:kv"
    specific_key = "mr:local:kv:mlx5_0:1:RoCE"
    agent._redis_client.set(base_key, "{}")
    agent._redis_client.set(specific_key, "{}")

    assert agent.unregister_memory_region("kv") is True

    assert agent._logical_regions == {}
    assert agent._materialized_regions == {}
    assert pool.unregistered == [7]
    assert base_key in agent._redis_client.deleted
    assert specific_key in agent._redis_client.deleted
    assert agent._redis_client.values == {}
    assert agent._mr_info_cache == {("peer", "kv", ""): {"rkey": 2}}
    assert agent._local_resource["memory_keys"] == []
    assert agent._redis_client.hashes["agent:local"]["memory_keys"] == "[]"


def test_peer_agent_unregister_memory_region_reports_missing_region():
    agent = PeerAgent.__new__(PeerAgent)
    agent.alias = "local"
    agent._shutdown_called = True
    agent._redis_key_prefix = ""
    agent._redis_client = None
    agent._logical_regions = {}
    agent._materialized_regions = {}
    agent._regions_lock = threading.Lock()
    agent._mr_info_cache = {}
    agent._mr_info_cache_lock = threading.Lock()

    assert agent.unregister_memory_region("missing") is False


def test_get_connections_returns_selected_connection():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    connections = agent.get_connections()

    assert list(connections) == ["peer"]
    connection = next(iter(connections["peer"].values()))
    assert connection.endpoint is endpoint
    assert connection.state == "connected"
    assert connection.local_nic == "mlx5_0"
    assert connection.remote_nic == "mlx5_1"


def test_query_connection_filters_by_peer_and_nics():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    connection = agent.query_connection("peer", local_nic="mlx5_0", remote_nic="mlx5_1")
    assert connection is not None
    assert connection.endpoint is endpoint
    assert agent.query_connection("peer", local_nic="mlx5_2") is None


def test_one_connection_per_nic_pair_allows_multiple_peer_connections():
    agent = PeerAgent.__new__(PeerAgent)
    agent.alias = "local"
    agent._shutdown_called = True
    agent._connections = {}
    agent._connections_lock = threading.Lock()
    local_0 = RdmaResourceKey("mlx5_0", 1, "RoCE")
    remote_0 = RdmaResourceKey("mlx5_1", 1, "RoCE")
    local_1 = RdmaResourceKey("mlx5_2", 1, "RoCE")
    remote_1 = RdmaResourceKey("mlx5_3", 1, "RoCE")

    conn0 = agent._get_or_create_connection(
        "peer", local_key=local_0, peer_key=remote_0, qp_num=1
    )
    conn1 = agent._get_or_create_connection(
        "peer", local_key=local_1, peer_key=remote_1, qp_num=1
    )

    assert conn0.conn_id != conn1.conn_id
    assert set(agent.get_connections()["peer"]) == {conn0.conn_id, conn1.conn_id}
    assert (
        agent.query_connection("peer", local_nic="mlx5_0", remote_nic="mlx5_1").conn_id
        == conn0.conn_id
    )
    assert (
        agent.query_connection("peer", local_nic="mlx5_2", remote_nic="mlx5_3").conn_id
        == conn1.conn_id
    )
    assert (
        agent.query_connection("peer").conn_id
        == sorted([conn0.conn_id, conn1.conn_id])[0]
    )


def test_data_plane_selectors_choose_matching_connection():
    endpoint0 = FakeEndpoint()
    endpoint1 = FakeEndpoint()
    agent = _agent(endpoint0)
    conn1 = DirectedConnection(
        agent=agent,
        peer_alias="peer",
        local_key=RdmaResourceKey("mlx5_2", 1, "RoCE"),
        peer_key=RdmaResourceKey("mlx5_3", 1, "RoCE"),
        qp_num=1,
    )
    conn1.attach_endpoint(endpoint1, memory_pool=None)
    conn1.mark_connected()
    agent._connections[conn1.conn_id] = conn1
    agent._connected_peers.add(conn1.conn_id)
    agent._endpoints[conn1.conn_id] = endpoint1

    assert agent.send("peer", "default") == "send-future"
    assert (
        agent.send("peer", "selected", local_nic="mlx5_2", remote_nic="mlx5_3")
        == "send-future"
    )
    assert endpoint0.send_calls == [("default", None)]
    assert endpoint1.send_calls == [("selected", None)]


def test_get_endpoint_rejects_mismatched_selectors():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    with pytest.raises(RuntimeError, match="not found"):
        agent._get_endpoint("peer", "mlx5_2", "mlx5_1")

    with pytest.raises(RuntimeError, match="not found"):
        agent._get_endpoint("peer", "mlx5_0", "mlx5_2")


def test_peer_agent_read_accepts_named_batches():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    result = agent.read(
        "peer",
        [
            ("kv", 8, 16, 32),
            ("scratch", "remote_scratch", 40, 48, 64),
        ],
        stream="stream",
    )

    assert result == "read-future"
    assert endpoint.read_calls == [
        (
            [
                (11, 21, 16, 8, 32),
                (12, 23, 48, 40, 64),
            ],
            "stream",
        )
    ]


def test_peer_agent_write_accepts_named_batches():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    result = agent.write(
        "peer",
        [
            ("kv", "kv_remote", 8, 16, 32),
        ],
        stream="stream",
    )

    assert result == "write-future"
    assert endpoint.write_calls == [([(11, 22, 16, 8, 32)], "stream")]


def test_peer_agent_write_with_imm_accepts_named_batches():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    result = agent.write_with_imm(
        "peer",
        [("kv", "kv_remote", 8, 16, 32)],
        imm_data=7,
        stream="stream",
    )

    assert result == "write-imm-future"
    assert endpoint.write_with_imm_calls == [([(11, 22, 16, 8, 32)], 7, "stream")]


def test_peer_agent_raw_assignment_passthrough():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    raw = [(1, 2, 3, 4, 5)]

    assert agent.read("peer", raw, "stream") == "read-future"
    assert agent.write("peer", raw, "stream") == "write-future"
    assert endpoint.read_calls == [(raw, "stream")]
    assert endpoint.write_calls == [(raw, "stream")]
