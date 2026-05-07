import threading

import pytest
from dlslime.peer_agent._agent import DirectedConnection, PeerAgent, RdmaResourceKey


class FakeEndpoint:
    def __init__(self):
        self.read_calls = []
        self.write_calls = []
        self.write_with_imm_calls = []

    def read(self, assign, stream=None):
        self.read_calls.append((assign, stream))
        return "read-future"

    def write(self, assign, stream=None):
        self.write_calls.append((assign, stream))
        return "write-future"

    def write_with_imm(self, assign, imm_data=0, stream=None):
        self.write_with_imm_calls.append((assign, imm_data, stream))
        return "write-imm-future"


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
    agent._connections = {"peer": conn}
    agent._connected_peers = {"peer"}
    agent._connected_peers_lock = threading.Lock()
    agent._endpoints = {"peer": endpoint}
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


def test_get_endpoint_rejects_mismatched_selectors():
    endpoint = FakeEndpoint()
    agent = _agent(endpoint)

    with pytest.raises(RuntimeError, match="local device"):
        agent._get_endpoint("peer", "mlx5_2", "mlx5_1")

    with pytest.raises(RuntimeError, match="peer device"):
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
