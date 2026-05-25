import threading

import pytest
from dlslime import discover_topology
from dlslime.peer_agent import _agent as peer_agent_mod
from dlslime.peer_agent._agent import DirectedConnection, PeerAgent, RdmaResourceKey


def _bare_agent(address: str = "10.1.2.3") -> PeerAgent:
    agent = PeerAgent.__new__(PeerAgent)
    agent.alias = "agent_0"
    agent._redis_key_prefix = ""
    agent._redis_client = None
    agent._logical_regions = {}
    agent._regions_lock = threading.Lock()
    agent._resource_cache = {}
    agent._resource_cache_lock = threading.Lock()
    agent._local_resource = {
        "schema_version": 1,
        "host": {"hostname": address, "address": address},
        "nics": [],
        "accelerators": [],
        "memory_keys": [],
    }
    agent._shutdown_called = True
    return agent


def _bare_agent_with_connections(*, connected=()):
    agent = _bare_agent()
    conn = DirectedConnection(
        agent=agent,
        peer_alias="peer",
        local_key=RdmaResourceKey("mlx5_0", 1, "RoCE"),
        peer_key=RdmaResourceKey("mlx5_1", 1, "RoCE"),
        qp_num=1,
    )
    agent._connections = {conn.conn_id: conn}
    agent._connections_lock = threading.Lock()
    agent._connected_peers = {conn.conn_id for peer in connected if peer == "peer"}
    agent._connected_peers_lock = threading.Lock()
    agent._connected_peers_cond = threading.Condition(agent._connected_peers_lock)
    return agent, conn.conn_id


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def scan_iter(self, match, count):
        prefix = match.removesuffix("*")
        for key in sorted(self.hashes):
            if key.startswith(prefix):
                yield key

    def ttl(self, key):
        return 30 if key in self.hashes else -2


def test_peer_connection_wait_accepts_single_peer():
    agent, conn_id = _bare_agent_with_connections(connected={"peer"})
    conn = peer_agent_mod.PeerConnection(agent, conn_id)

    assert conn.wait() is conn


def test_peer_connection_wait_rejects_names_without_connection():
    agent, _ = _bare_agent_with_connections()
    conn = peer_agent_mod.PeerConnection(agent, "mlx5_0")

    with pytest.raises(ValueError, match="device names belong"):
        conn.wait(timeout=0.01)


def test_connect_to_rejects_self_alias():
    agent = _bare_agent()

    with pytest.raises(ValueError, match="cannot target this agent's own alias"):
        agent.connect_to(agent.alias)


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _add_fake_nic(root, name, pci_bus_id, *, state, link_layer, active_mtu, numa_node):
    nic_root = root / "class" / "infiniband" / name
    port_root = nic_root / "ports" / "1"
    pci_root = root / "devices" / "pci0000:00" / pci_bus_id

    _write(port_root / "state", state)
    _write(port_root / "link_layer", link_layer)
    _write(port_root / "active_mtu", str(active_mtu))
    _write(pci_root / "numa_node", str(numa_node))
    (nic_root / "device").symlink_to(pci_root, target_is_directory=True)


def test_discover_topology_prefers_requested_device_and_normalizes_sysfs(tmp_path):
    _add_fake_nic(
        tmp_path,
        "mlx5_0",
        "0000:18:00.0",
        state="4: ACTIVE\n",
        link_layer="Ethernet\n",
        active_mtu=4096,
        numa_node=0,
    )
    _add_fake_nic(
        tmp_path,
        "mlx5_1",
        "0000:19:00.0",
        state="1: DOWN\n",
        link_layer="InfiniBand\n",
        active_mtu=2048,
        numa_node=1,
    )

    resource = discover_topology(
        preferred_device="mlx5_0",
        ib_port=1,
        preferred_link_type=None,
        sysfs_root=str(tmp_path),
        devices=["mlx5_1", "mlx5_0"],
    )

    assert resource["schema_version"] == 1
    assert resource["host"]["hostname"]
    assert resource["host"]["address"]
    assert resource["memory_keys"] == []
    assert [nic["name"] for nic in resource["nics"]] == ["mlx5_0", "mlx5_1"]

    mlx5_0 = resource["nics"][0]
    assert mlx5_0["health"] == "AVAILABLE"
    assert mlx5_0["numa_node"] == 0
    assert mlx5_0["pci_bus_id"] == "0000:18:00.0"
    assert mlx5_0["ports"] == [
        {
            "port": 1,
            "state": "ACTIVE",
            "link_type": "RoCE",
            "active_mtu": 4096,
        }
    ]

    mlx5_1 = resource["nics"][1]
    assert mlx5_1["health"] == "DEGRADED"
    assert mlx5_1["ports"][0]["link_type"] == "IB"
    assert mlx5_1["ports"][0]["state"] == "DOWN"


def test_discover_topology_uses_preferred_protocol_when_sysfs_is_missing(tmp_path):
    resource = discover_topology(
        preferred_device=None,
        ib_port=1,
        preferred_link_type="RoCE",
        sysfs_root=str(tmp_path),
        devices=["mlx5_0"],
    )

    nic = resource["nics"][0]
    assert nic["health"] == "AVAILABLE"
    assert nic["ports"] == [
        {
            "port": 1,
            "state": "ACTIVE",
            "link_type": "RoCE",
        }
    ]


def test_discover_topology_does_not_readd_preferred_device_outside_device_list(
    tmp_path,
):
    resource = discover_topology(
        preferred_device="mlx5_hidden",
        ib_port=1,
        preferred_link_type="RoCE",
        sysfs_root=str(tmp_path),
        devices=["mlx5_0"],
    )

    assert [nic["name"] for nic in resource["nics"]] == ["mlx5_0"]


def test_peer_agent_discovery_delegates_local_query_to_cpp_topology(monkeypatch):
    seen = {}

    def fake_discover_topology(
        preferred_device=None,
        ib_port=1,
        preferred_link_type=None,
        sysfs_root="/sys",
        devices=None,
    ):
        seen.update(
            {
                "preferred_device": preferred_device,
                "ib_port": ib_port,
                "preferred_link_type": preferred_link_type,
                "sysfs_root": sysfs_root,
                "devices": devices,
            }
        )
        return {
            "schema_version": 1,
            "host": {"hostname": "local-host", "address": "10.0.0.1"},
            "nics": [
                {
                    "name": "mlx5_0",
                    "health": "AVAILABLE",
                    "ports": [
                        {
                            "port": ib_port,
                            "state": "ACTIVE",
                            "link_type": preferred_link_type or "RoCE",
                        }
                    ],
                }
            ],
            "accelerators": [],
            "memory_keys": [],
            "topology_epoch": 1,
        }

    monkeypatch.setattr(peer_agent_mod, "discover_topology", fake_discover_topology)

    resource = _bare_agent()._discover_local_resource(
        preferred_device="mlx5_hidden",
        preferred_ib_port=1,
        preferred_link_type="RoCE",
    )

    assert seen == {
        "preferred_device": "mlx5_hidden",
        "ib_port": 1,
        "preferred_link_type": "RoCE",
        "sysfs_root": "/sys",
        "devices": None,
    }
    assert [nic["name"] for nic in resource["nics"]] == ["mlx5_0"]


def test_first_usable_resource_key_filters_device_port_and_protocol():
    resource = {
        "nics": [
            {
                "name": "mlx5_0",
                "health": "AVAILABLE",
                "ports": [{"port": 1, "state": "ACTIVE", "link_type": "RoCE"}],
            },
            {
                "name": "mlx5_1",
                "health": "AVAILABLE",
                "ports": [{"port": 1, "state": "ACTIVE", "link_type": "IB"}],
            },
            {
                "name": "mlx5_2",
                "health": "UNAVAILABLE",
                "ports": [{"port": 1, "state": "ACTIVE", "link_type": "RoCE"}],
            },
        ]
    }
    agent = _bare_agent()

    assert agent._first_usable_resource_key(resource, ib_port=1) == RdmaResourceKey(
        device="mlx5_0",
        ib_port=1,
        link_type="RoCE",
    )
    assert agent._first_usable_resource_key(
        resource,
        device="mlx5_1",
        ib_port=1,
        link_type="IB",
    ) == RdmaResourceKey(device="mlx5_1", ib_port=1, link_type="IB")

    with pytest.raises(RuntimeError, match="No usable RDMA resource"):
        agent._first_usable_resource_key(
            resource,
            device="mlx5_1",
            ib_port=1,
            link_type="RoCE",
        )


def test_publish_resource_record_and_get_resource_round_trip():
    redis_client = FakeRedis()
    writer = _bare_agent(address="10.0.0.1")
    writer.alias = "agent_0"
    writer._redis_client = redis_client
    writer._logical_regions = {"attn": object(), "kv": object()}
    writer._local_resource.update(
        {
            "nics": [
                {
                    "name": "mlx5_0",
                    "health": "AVAILABLE",
                    "ports": [{"port": 1, "state": "ACTIVE", "link_type": "RoCE"}],
                }
            ],
            "topology_epoch": 123,
        }
    )

    writer._publish_resource_record()

    reader = _bare_agent(address="10.0.0.2")
    reader.alias = "agent_1"
    reader._redis_client = redis_client

    assert reader.list_agents() == ["agent_0"]
    assert reader.list_mem_keys("agent_0") == ["attn", "kv"]

    resource = reader.get_resource("agent_0")
    assert resource is not None
    assert resource["host"]["address"] == "10.0.0.1"
    assert resource["memory_keys"] == ["attn", "kv"]
    assert resource["nics"][0]["name"] == "mlx5_0"

    own_resource = writer.get_resource()
    assert own_resource is writer._local_resource
    assert resource["nics"][0]["ports"][0]["link_type"] == "RoCE"


def test_peer_agent_scope_isolates_discovery_namespace():
    redis_client = FakeRedis()

    initiator = _bare_agent(address="10.0.0.1")
    initiator.alias = "dlslime1"
    initiator._redis_key_prefix = "example_scope"
    initiator._redis_client = redis_client
    initiator._publish_resource_record()

    same_scope_target = _bare_agent(address="10.0.0.2")
    same_scope_target.alias = "dlslime0"
    same_scope_target._redis_key_prefix = "example_scope"
    same_scope_target._redis_client = redis_client
    same_scope_target._publish_resource_record()

    default_scope_target_with_same_alias = _bare_agent(address="10.0.0.3")
    default_scope_target_with_same_alias.alias = "dlslime0"
    default_scope_target_with_same_alias._redis_key_prefix = ""
    default_scope_target_with_same_alias._redis_client = redis_client
    default_scope_target_with_same_alias._publish_resource_record()

    assert initiator.list_agents() == ["dlslime0", "dlslime1"]
    assert initiator.get_resource("dlslime0") is not None
    assert initiator.get_resource("dlslime0")["host"]["address"] == "10.0.0.2"

    assert default_scope_target_with_same_alias.list_agents() == ["dlslime0"]
    assert (
        default_scope_target_with_same_alias.get_resource("dlslime0")["host"]["address"]
        == "10.0.0.3"
    )
    assert default_scope_target_with_same_alias.get_resource("dlslime1") is None


def test_discover_topology_requires_at_least_one_nic(tmp_path):
    with pytest.raises(RuntimeError, match="No RDMA devices available"):
        discover_topology(
            preferred_device=None,
            ib_port=1,
            preferred_link_type=None,
            sysfs_root=str(tmp_path),
            devices=[],
        )
