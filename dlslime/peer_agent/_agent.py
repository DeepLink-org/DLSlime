"""PeerAgent: declarative control-plane client + I/O facade.

Owns lifecycle (register, heartbeat, shutdown), per-peer RDMA endpoint
state, MR registration, and the convenience facade over the per-endpoint
I/O primitives. Handshake protocol lives in ``_mailbox.py``.
"""

from __future__ import annotations

import inspect
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import httpx
    import redis
except ImportError as e:
    raise ImportError(
        "PeerAgent requires 'httpx' and 'redis' packages. "
        "Install them with: pip install httpx redis"
    ) from e

from nanoctrl import NanoCtrlClient

from dlslime import discover_topology, RDMAContext, RDMAEndpoint, RDMAMemoryPool
from dlslime.logging import get_logger
from ._mailbox import StreamMailbox
from ._obs import _tlog

logger = get_logger("peer_agent")


def _lifecycle_notice(message: str, *args: Any) -> None:
    rendered = message % args if args else message
    logger.info(rendered)
    print(rendered, flush=True)


@dataclass(frozen=True)
class RdmaResourceKey:
    device: str
    ib_port: int
    link_type: str

    def redis_suffix(self) -> str:
        return f"{self.device}:{self.ib_port}:{self.link_type}"


@dataclass
class LogicalMemoryRegion:
    name: str
    ptr: int
    offset: int
    length: int


@dataclass
class MaterializedMemoryRegion:
    name: str
    resource_key: RdmaResourceKey
    handler: int
    info: Dict[str, Any]


class DirectedConnection:
    """Internal connection metadata for one directed PeerAgent endpoint."""

    def __init__(
        self,
        agent: "PeerAgent",
        peer_alias: str,
        local_key: RdmaResourceKey,
        peer_key: RdmaResourceKey,
        qp_num: int,
        profile: str = "default",
    ) -> None:
        self._agent = agent
        self.peer_alias = peer_alias
        self.local_key = local_key
        self.peer_key = peer_key
        self.qp_num = qp_num
        self.profile = profile
        self.endpoint: Optional[RDMAEndpoint] = None
        self.memory_pool: Optional[RDMAMemoryPool] = None
        self.state = "connecting"

    @property
    def conn_id(self) -> str:
        return (
            f"{self._agent.alias}:{self.local_key.device}:port{self.local_key.ib_port}"
            f"->{self.peer_alias}:{self.peer_key.device}:port{self.peer_key.ib_port}"
            f"#qp{self.qp_num}"
        )

    def attach_endpoint(
        self, endpoint: RDMAEndpoint, memory_pool: RDMAMemoryPool
    ) -> None:
        self.endpoint = endpoint
        self.memory_pool = memory_pool

    def mark_connected(self) -> None:
        self.state = "connected"

    def mark_failed(self) -> None:
        self.state = "failed"


class PeerConnection:
    """Public handle for one peer connection."""

    def __init__(self, agent: "PeerAgent", conn_id: str) -> None:
        self._agent = agent
        self._conn_id = conn_id

    def _connection(self) -> DirectedConnection:
        with self._agent._connections_lock:
            conn = self._agent._connections.get(self._conn_id)
        if conn is None:
            raise RuntimeError(
                f"Connection {self._conn_id!r} not found. "
                "Call connect_to(peer, ...) first."
            )
        return conn

    @property
    def peer_alias(self) -> str:
        """Return the peer alias for this connection."""
        return self._connection().peer_alias

    def wait(self, timeout: float = 60.0) -> "PeerConnection":
        """Block until this peer connection is ready."""
        self._agent._wait_connected(self._conn_id, timeout_sec=timeout)
        return self

    def is_connected(self) -> bool:
        """Return whether this peer is connected locally."""
        return self._agent._is_connection_connected(self._conn_id)

    @property
    def conn_id(self) -> str:
        """Return the stable connection id."""
        return self._conn_id

    @property
    def local_nic(self) -> str:
        """Return the local RDMA NIC selected for this connection."""
        return self._connection().local_key.device

    @property
    def remote_nic(self) -> str:
        """Return the peer RDMA NIC selected for this connection."""
        return self._connection().peer_key.device

    @property
    def state(self) -> str:
        """Return the current connection state."""
        return self._connection().state

    @property
    def endpoint(self) -> Optional[RDMAEndpoint]:
        """Return the selected endpoint once created, otherwise None."""
        conn = self._connection()
        if conn.endpoint is not None:
            return conn.endpoint
        with self._agent._endpoints_lock:
            endpoint = self._agent._endpoints.get(self._conn_id)
        if endpoint is not None:
            _, pool = self._agent._get_context_and_pool(conn.local_key)
            conn.attach_endpoint(endpoint, pool)
        return endpoint


class PeerAgent:
    """PeerAgent manages RDMA connections via declarative topology reconciliation."""

    def __init__(
        self,
        nanoctrl_url: str = "http://127.0.0.1:3000",
        alias: Optional[str] = None,
        device: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        """
        Initialize a PeerAgent.

        Args:
            nanoctrl_url: URL of the control plane server (NanoCtrl)
            alias: (Optional) Agent name. If None, requests unique name from NanoCtrl.
            device: RDMA device name (e.g., "mlx5_0"), if None, auto-select
            scope: Scope string for multi-tenant isolation (used as Redis key prefix).
        """
        self.nanoctrl_url = nanoctrl_url
        self._redis_address: Optional[str] = None
        self.alias: str = alias or ""
        self._preferred_device = device

        # Build Redis key prefix from scope parameter
        self._redis_key_prefix = scope or ""

        # NanoCtrl HTTP client
        self._client = NanoCtrlClient(
            nanoctrl_url, scope=self._redis_key_prefix or None
        )

        # Local topology is discovered before registration and published through
        # NanoCtrl/Redis. RDMA resources are created lazily per selected
        # (device, port, link_type) instead of being bound to the whole agent.
        self._local_resource = self._discover_local_resource(
            preferred_device=device,
            preferred_ib_port=1,
            preferred_link_type=None,
        )
        self._resource_cache: Dict[str, Dict[str, Any]] = {}
        self._resource_cache_lock = threading.Lock()

        self._endpoints: Dict[str, RDMAEndpoint] = {}
        self._endpoints_lock = (
            threading.Lock()
        )  # Protects _endpoints for concurrent reconcile
        self._connected_peers: Set[str] = set()
        self._connected_peers_lock = threading.Lock()
        # Notified whenever _connected_peers changes, so PeerConnection.wait()
        # can block on a condition instead of polling. Shares the existing
        # lock so is_peer_connected / mark_peer_connected call sites are
        # unchanged.
        self._connected_peers_cond = threading.Condition(self._connected_peers_lock)

        # Prevents duplicate in-flight connect attempts for the same connection.
        self._connecting_peers: Set[str] = set()
        self._connecting_peers_lock = threading.Lock()

        # Connections we've already XADDed our qp_ready for. Used to suppress
        # ping-pong: without this, an inbound qp_ready triggers another
        # outbound qp_ready, which triggers another inbound one, etc. —
        # each round walking the mailbox listener's single-threaded queue.
        self._notified_peers: Set[str] = set()
        self._notified_peers_lock = threading.Lock()

        self._connections: Dict[str, DirectedConnection] = {}
        self._connections_lock = threading.Lock()

        self._contexts: Dict[RdmaResourceKey, RDMAContext] = {}
        self._pools: Dict[RdmaResourceKey, RDMAMemoryPool] = {}
        self._resource_lock = threading.Lock()

        self._logical_regions: Dict[str, LogicalMemoryRegion] = {}
        self._materialized_regions: Dict[
            Tuple[str, RdmaResourceKey], MaterializedMemoryRegion
        ] = {}
        self._regions_lock = threading.Lock()

        # Worker pool for eager RDMAEndpoint construction in
        # connect_to. RDMAEndpoint() allocates QPs and takes
        # ~10-15 ms each; doing them serially on the mailbox listener
        # makes max_edge scale as O(num_peers) — moving the allocation
        # here instead, in parallel, keeps the listener's per-message
        # cost to a dict lookup.
        self._endpoint_creation_pool = ThreadPoolExecutor(
            max_workers=16,
            thread_name_prefix="peer-agent-ep-create",
        )

        # MR info cache (persistent, no TTL - MR info is immutable after registration)
        # Architecture: Redis (source of truth) → PeerAgent (cache) → endpoint
        # Zero overhead in hot path after warm-up
        self._mr_info_cache: Dict[tuple, dict] = {}
        self._mr_info_cache_lock = threading.Lock()

        # Redis is discovered from NanoCtrl during registration.
        self._redis_client: Optional[redis.Redis] = None

        self._stop_event = threading.Event()
        self._shutdown_called = False

        # Event listener for cleanup only (legacy inbox)
        self._event_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._mailbox: Optional[StreamMailbox] = None

        # Register with control plane
        self._register()

        # Purge stale QP exchange keys from previous crashed runs of the same alias.
        # Otherwise we may connect to dead QP metadata and fail on first WR.
        self._cleanup_stale_exchange_keys()
        self._cleanup_stale_mr_keys()

        # Start StreamMailbox (event-driven connection management)
        self._mailbox = StreamMailbox(self, stream_block_ms=5)
        self._mailbox.start()

        # Start cleanup event listener
        self._start_cleanup_listener()

        # Start heartbeat thread (keeps agent alive in NanoCtrl)
        self._start_heartbeat()

    # ------------------------------------------------------------------
    # Registration / lifecycle
    # ------------------------------------------------------------------
    def _redis_prefix(self) -> str:
        return f"{self._redis_key_prefix}:" if self._redis_key_prefix else ""

    def _agent_key(self, alias: str) -> str:
        return f"{self._redis_prefix()}agent:{alias}"

    def _local_address(self) -> str:
        host = self._local_resource.get("host")
        if isinstance(host, dict):
            address = host.get("address")
            if address:
                return str(address)
        return ""

    def _normalize_link_type(self, link_type: Optional[str]) -> str:
        if not link_type:
            return "UNKNOWN"
        value = str(link_type).strip()
        lower = value.lower()
        if lower in {"ethernet", "roce", "rocev1", "rocev2", "roce v2"}:
            return "RoCE"
        if lower in {"infiniband", "ib"}:
            return "IB"
        return value

    def _discover_local_resource(
        self,
        *,
        preferred_device: Optional[str],
        preferred_ib_port: int,
        preferred_link_type: Optional[str],
    ) -> Dict[str, Any]:
        return discover_topology(
            preferred_device,
            preferred_ib_port,
            preferred_link_type,
        )

    def _first_usable_resource_key(
        self,
        resource: Dict[str, Any],
        *,
        device: Optional[str] = None,
        ib_port: Optional[int] = 1,
        link_type: Optional[str] = None,
    ) -> RdmaResourceKey:
        wanted_link = self._normalize_link_type(link_type) if link_type else None
        nics = resource.get("nics") or []
        for nic in nics:
            if device is not None and nic.get("name") != device:
                continue
            if nic.get("health", "AVAILABLE") == "UNAVAILABLE":
                continue
            for port in nic.get("ports") or []:
                port_num = int(port.get("port", 1))
                if ib_port is not None and port_num != int(ib_port):
                    continue
                state = str(port.get("state", "ACTIVE")).upper()
                if state not in {"ACTIVE", "UNKNOWN"}:
                    continue
                port_link = self._normalize_link_type(port.get("link_type"))
                if wanted_link and port_link != wanted_link:
                    continue
                if port_link == "UNKNOWN":
                    raise RuntimeError(
                        f"Cannot select RDMA port for {nic.get('name')}: unknown link_type"
                    )
                return RdmaResourceKey(str(nic["name"]), port_num, port_link)

        detail = f"device={device!r}, ib_port={ib_port!r}, link_type={link_type!r}"
        raise RuntimeError(f"No usable RDMA resource found ({detail})")

    def _get_context_and_pool(
        self, key: RdmaResourceKey
    ) -> Tuple[RDMAContext, RDMAMemoryPool]:
        with self._resource_lock:
            if key not in self._contexts:
                ctx = RDMAContext()
                rc = ctx.init(key.device, key.ib_port, key.link_type)
                if rc != 0:
                    raise RuntimeError(f"RDMAContext.init failed for {key}")
                self._contexts[key] = ctx
                self._pools[key] = RDMAMemoryPool(ctx)
            pool = self._pools[key]
            ctx = self._contexts[key]

        return ctx, pool

    def _register(self) -> None:
        """Register this agent with the control plane (also allocates name if not provided)."""
        max_retries = 5
        retry_delay = 1.0

        _lifecycle_notice(
            "PeerAgent: Registering with control plane at %s", self.nanoctrl_url
        )

        for attempt in range(max_retries):
            try:
                result = self._register_peer_with_nanoctrl()

                # Extract allocated name from response
                if "name" in result:
                    self.alias = result["name"]
                    _lifecycle_notice("PeerAgent: Registered with name: %s", self.alias)
                else:
                    raise RuntimeError("NanoCtrl did not return agent name")

                if "redis_address" not in result:
                    raise RuntimeError("NanoCtrl did not return redis_address")

                self._redis_address = result["redis_address"]
                redis_host, redis_port = self._redis_address.split(":")
                self._redis_client = redis.Redis(
                    host=redis_host, port=int(redis_port), decode_responses=True
                )
                self._publish_resource_record()
                return
            except (
                httpx.ConnectError,
                httpx.TimeoutException,
                httpx.HTTPStatusError,
            ) as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)
                    logger.warning(
                        "PeerAgent %s registration failed (attempt %s/%s): %s. "
                        "Retrying in %.1fs...",
                        self.alias,
                        attempt + 1,
                        max_retries,
                        e,
                        wait_time,
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        "PeerAgent %s registration failed after %s attempts",
                        self.alias,
                        max_retries,
                    )
                    raise

    def _register_peer_with_nanoctrl(self) -> Dict[str, Any]:
        """Call NanoCtrlClient.register_peer across installed client versions."""
        params = inspect.signature(self._client.register_peer).parameters
        kwargs: Dict[str, Any] = {
            "alias": self.alias or None,
            "address": self._local_address(),
        }

        if "resource" in params:
            kwargs["resource"] = self._local_resource

        if {"device", "ib_port", "link_type", "name_prefix"} & set(params):
            key = self._first_usable_resource_key(
                self._local_resource,
                device=self._preferred_device,
                ib_port=1,
                link_type=None,
            )
            if "device" in params:
                kwargs["device"] = key.device
            if "ib_port" in params:
                kwargs["ib_port"] = key.ib_port
            if "link_type" in params:
                kwargs["link_type"] = key.link_type
            if "name_prefix" in params:
                kwargs["name_prefix"] = "agent"

        return self._client.register_peer(**kwargs)

    def _publish_resource_record(self) -> None:
        if self._redis_client is None or not self.alias:
            return
        memory_keys = sorted(self._logical_regions.keys())
        self._local_resource["memory_keys"] = memory_keys
        key = self._agent_key(self.alias)
        try:
            self._redis_client.hset(
                key,
                mapping={
                    "addr": self._local_address(),
                    "resource": json.dumps(self._local_resource),
                    "topology": json.dumps(self._local_resource),
                    "memory_keys": json.dumps(memory_keys),
                    "updated_at": str(int(time.time())),
                },
            )
        except Exception as e:
            logger.warning("PeerAgent %s: Resource publish warning: %s", self.alias, e)

    def list_agents(self) -> List[str]:
        """Return active peer aliases in the same scope from Redis."""
        if self._redis_client is None:
            return []
        prefix = self._redis_prefix()
        pattern = f"{prefix}agent:*"
        key_prefix = f"{prefix}agent:"
        active: List[str] = []
        for key in self._redis_client.scan_iter(match=pattern, count=200):
            ttl = self._redis_client.ttl(key)
            if ttl == 0 or ttl == -2:
                continue
            active.append(str(key).removeprefix(key_prefix))
        return sorted(active)

    def get_resource(
        self, peer_alias: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Return local resource or read a peer's published resource from Redis."""
        if peer_alias is None or peer_alias == self.alias:
            return self._local_resource
        with self._resource_cache_lock:
            cached = self._resource_cache.get(peer_alias)
            if cached is not None:
                return cached
        if self._redis_client is None:
            return None
        record = self._redis_client.hgetall(self._agent_key(peer_alias))
        if not record:
            return None
        raw = record.get("resource") or record.get("topology")
        if raw:
            try:
                resource = json.loads(raw)
            except json.JSONDecodeError:
                resource = None
        else:
            resource = {
                "schema_version": 1,
                "nics": [
                    {
                        "name": record.get("device", ""),
                        "health": "AVAILABLE",
                        "ports": [
                            {
                                "port": int(record.get("ib_port", 1)),
                                "state": "ACTIVE",
                                "link_type": self._normalize_link_type(
                                    record.get("link_type", "RoCE")
                                ),
                            }
                        ],
                    }
                ],
                "accelerators": [],
            }
        if resource is not None:
            with self._resource_cache_lock:
                self._resource_cache[peer_alias] = resource
        return resource

    def list_mem_keys(self, peer_alias: Optional[str] = None) -> List[str]:
        """Return local or peer logical memory region names."""
        if peer_alias is None or peer_alias == self.alias:
            with self._regions_lock:
                return sorted(self._logical_regions.keys())
        if self._redis_client is None:
            return []
        record = self._redis_client.hgetall(self._agent_key(peer_alias))
        raw = record.get("memory_keys") if record else None
        if raw:
            try:
                keys = json.loads(raw)
                if isinstance(keys, list):
                    return sorted(str(k) for k in keys)
            except json.JSONDecodeError:
                pass

        prefix = self._redis_prefix()
        pattern = f"{prefix}mr:{peer_alias}:*"
        keys = []
        mr_prefix = f"{prefix}mr:{peer_alias}:"
        for key in self._redis_client.scan_iter(match=pattern, count=200):
            suffix = str(key).removeprefix(mr_prefix)
            keys.append(suffix.split(":", 1)[0])
        return sorted(set(keys))

    def _start_cleanup_listener(self) -> None:
        """Listen for cleanup events from peers (NanoCtrl pushes to inbox)."""
        # Flush any stale events left by a previous run so we don't act on them.
        inbox_key = f"{self._redis_key_prefix}:inbox:{self.alias}"
        self._redis_client.delete(inbox_key)

        def event_loop():
            while not self._stop_event.is_set():
                try:
                    result = self._redis_client.blpop(inbox_key, timeout=1)
                    if result:
                        _, event_str = result
                        event = json.loads(event_str)
                        if event.get("type") == "cleanup":
                            peer = event.get("peer")
                            logger.info(
                                "PeerAgent %s: Cleanup from peer %s", self.alias, peer
                            )
                            removed = []
                            with self._endpoints_lock:
                                with self._connections_lock:
                                    conn_ids = [
                                        conn_id
                                        for conn_id, conn in self._connections.items()
                                        if conn.peer_alias == peer
                                    ]
                                for conn_id in conn_ids:
                                    if conn_id not in self._endpoints:
                                        continue
                                    # Force-complete any blocked RDMA waits before
                                    # dropping the endpoint reference. Otherwise a
                                    # thread blocked in imm_recv()/wait() can hang
                                    # forever after the remote peer exits.
                                    endpoint = self._endpoints[conn_id]
                                    if hasattr(endpoint, "shutdown"):
                                        endpoint.shutdown()
                                    with self._connected_peers_lock:
                                        self._connected_peers.discard(conn_id)
                                        self._connected_peers_cond.notify_all()
                                    self._clear_peer_notified(conn_id)
                                    del self._endpoints[conn_id]
                                    removed.append(conn_id)
                            for conn_id in removed:
                                logger.info(
                                    "PeerAgent %s: Removed endpoint %s for %s",
                                    self.alias,
                                    conn_id,
                                    peer,
                                )
                except redis.exceptions.ConnectionError:
                    time.sleep(0.1)
                except Exception as e:
                    logger.warning(
                        "PeerAgent %s: Cleanup listener error: %s", self.alias, e
                    )
                    time.sleep(0.1)

        self._event_thread = threading.Thread(target=event_loop, daemon=True)
        self._event_thread.start()

    def _cleanup_stale_exchange_keys(self) -> None:
        """Delete stale Redis exchange keys involving this alias.

        These keys carry QP metadata. If a previous run crashed before shutdown,
        stale exchange data may point to dead QPs, leading to retry exceeded errors
        on the first RDMA WR after a seemingly successful logical handshake.
        """
        if not self.alias:
            return

        prefix = f"{self._redis_key_prefix}:" if self._redis_key_prefix else ""
        patterns = [
            f"{prefix}exchange:{self.alias}:*",
            f"{prefix}exchange:*:{self.alias}",
        ]
        try:
            keys_to_delete: list[str] = []
            for pattern in patterns:
                keys_to_delete.extend(
                    list(self._redis_client.scan_iter(match=pattern, count=200))
                )
            if keys_to_delete:
                self._redis_client.delete(*keys_to_delete)
        except Exception as e:
            logger.warning("PeerAgent %s: Exchange cleanup warning: %s", self.alias, e)

    def _cleanup_stale_mr_keys(self) -> None:
        """Delete stale Redis MR keys registered by a previous run of this alias.

        RPC mailboxes are process-local buffers. If a previous process crashed
        before shutdown, peers may discover the old addr/rkey from Redis and
        attempt RDMA writes into dead memory, which surfaces as local/remote
        access errors on the first RPC.
        """
        if not self.alias:
            return

        prefix = f"{self._redis_key_prefix}:" if self._redis_key_prefix else ""
        pattern = f"{prefix}mr:{self.alias}:*"
        try:
            keys_to_delete = list(
                self._redis_client.scan_iter(match=pattern, count=200)
            )
            if keys_to_delete:
                self._redis_client.delete(*keys_to_delete)
        except Exception as e:
            logger.warning("PeerAgent %s: MR cleanup warning: %s", self.alias, e)

    def _start_heartbeat(self, interval: float = 15.0) -> None:
        """Periodically POST /heartbeat to refresh agent TTL in NanoCtrl."""

        def heartbeat_loop():
            # Registration already sets a fresh TTL, so the first heartbeat does
            # not need to race the startup path. Waiting one interval avoids a
            # spurious immediate "not_found" response right after register.
            while not self._stop_event.wait(interval):
                try:
                    resp = self._client.heartbeat_peer(self.alias)
                    if resp.get("status") == "not_found":
                        logger.warning(
                            "PeerAgent %s: Heartbeat returned not_found, "
                            "re-registering...",
                            self.alias,
                        )
                        self._register()
                except Exception as e:
                    logger.warning("PeerAgent %s: Heartbeat failed: %s", self.alias, e)

        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    # ------------------------------------------------------------------
    # Peer / endpoint state
    # ------------------------------------------------------------------
    def _resolve_peer_resource_key(
        self,
        peer_alias: str,
        *,
        peer_device: Optional[str],
        ib_port: Optional[int],
        link_type: Optional[str],
        fallback_device: Optional[str],
    ) -> RdmaResourceKey:
        peer_resource = self.get_resource(peer_alias)
        if peer_resource is not None:
            return self._first_usable_resource_key(
                peer_resource,
                device=peer_device,
                ib_port=ib_port,
                link_type=link_type,
            )
        fallback_local = self._first_usable_resource_key(
            self._local_resource,
            device=fallback_device,
            ib_port=ib_port,
            link_type=link_type,
        )
        fallback_link = link_type or fallback_local.link_type
        return RdmaResourceKey(
            peer_device or fallback_device or fallback_local.device,
            int(ib_port or 1),
            self._normalize_link_type(fallback_link),
        )

    def _get_or_create_connection(
        self,
        peer_alias: str,
        *,
        local_key: Optional[RdmaResourceKey] = None,
        peer_key: Optional[RdmaResourceKey] = None,
        qp_num: Optional[int] = None,
        profile: str = "default",
    ) -> DirectedConnection:
        with self._connections_lock:
            peer_connections = [
                conn
                for conn in self._connections.values()
                if conn.peer_alias == peer_alias
            ]
            if local_key is None:
                if len(peer_connections) == 1:
                    return peer_connections[0]
                if len(peer_connections) > 1:
                    raise RuntimeError(
                        f"Multiple connections to {peer_alias} exist. "
                        "Use query_connection(peer, local_nic=..., remote_nic=...) "
                        "to select one."
                    )

            if local_key is None:
                local_key = self._first_usable_resource_key(
                    self._local_resource,
                    ib_port=1,
                    link_type=None,
                )
            if peer_key is None:
                peer_key = RdmaResourceKey(
                    local_key.device,
                    local_key.ib_port,
                    local_key.link_type,
                )
            for conn in peer_connections:
                if conn.local_key == local_key and conn.peer_key == peer_key:
                    if qp_num is not None and conn.qp_num != qp_num:
                        raise RuntimeError(
                            f"Connection {conn.conn_id} already exists with "
                            f"qp_num={conn.qp_num}, not {qp_num}."
                        )
                    return conn
            conn = DirectedConnection(
                self,
                peer_alias,
                local_key,
                peer_key,
                qp_num or 1,
                profile=profile,
            )
            self._connections[conn.conn_id] = conn
            return conn

    def _connection_meta(self, conn_id: str) -> Dict[str, Any]:
        with self._connections_lock:
            conn = self._connections[conn_id]
        return {
            "conn_id": conn.conn_id,
            "src": self.alias,
            "dst": conn.peer_alias,
            "src_device": conn.local_key.device,
            "dst_device": conn.peer_key.device,
            "ib_port": conn.local_key.ib_port,
            "qp_num": conn.qp_num,
            "link_type": conn.local_key.link_type,
            "profile": conn.profile,
        }

    def _ensure_connection_from_meta(
        self, peer_alias: str, meta: Dict[str, Any]
    ) -> DirectedConnection:
        """Create local connection state from a peer's directed request."""
        local_key = self._first_usable_resource_key(
            self._local_resource,
            device=meta.get("dst_device"),
            ib_port=int(meta.get("ib_port") or 1),
            link_type=meta.get("link_type"),
        )
        peer_key = RdmaResourceKey(
            str(meta.get("src_device") or local_key.device),
            int(meta.get("ib_port") or local_key.ib_port),
            self._normalize_link_type(meta.get("link_type") or local_key.link_type),
        )
        if local_key.link_type != peer_key.link_type:
            raise RuntimeError(
                f"Cannot connect {self.alias}:{local_key.device} to "
                f"{peer_alias}:{peer_key.device}: link_type mismatch "
                f"{local_key.link_type} != {peer_key.link_type}"
            )
        return self._get_or_create_connection(
            peer_alias,
            local_key=local_key,
            peer_key=peer_key,
            qp_num=int(meta.get("qp_num") or 1),
            profile=str(meta.get("profile") or "default"),
        )

    def _ensure_local_endpoint_created(self, conn_id: str) -> RDMAEndpoint:
        """
        Idempotent: create endpoint for peer if not exists.
        Returns the endpoint (existing or newly created). Thread-safe.

        The RDMAEndpoint constructor allocates QPs, which takes ~10-15 ms
        per call. We release `_endpoints_lock` during construction so that
        concurrent pre-creation of many endpoints (see connect_to)
        actually runs in parallel — otherwise holding the lock would
        serialize the QP allocations and defeat the purpose.
        """
        # Fast path: lookup under lock.
        with self._endpoints_lock:
            ep = self._endpoints.get(conn_id)
            if ep is not None:
                with self._connections_lock:
                    conn = self._connections[conn_id]
                if conn.endpoint is None:
                    _, pool = self._get_context_and_pool(conn.local_key)
                    conn.attach_endpoint(ep, pool)
                return ep

        with self._connections_lock:
            conn = self._connections[conn_id]
        _, pool = self._get_context_and_pool(conn.local_key)
        self._materialize_all_regions_for_key(conn.local_key)

        # Slow path: construct outside the lock. Another thread may also be
        # constructing for the same peer; we handle that with a double-check.
        new_ep = RDMAEndpoint(
            pool=pool,
            num_qp=conn.qp_num,
        )

        with self._endpoints_lock:
            existing = self._endpoints.get(conn_id)
            if existing is not None:
                # Lost the race. `new_ep` goes out of scope; its destructor
                # releases the QPs we allocated. Cheap in absolute terms
                # (~14 ms once) and rare in practice.
                return existing
            self._endpoints[conn_id] = new_ep
            conn.attach_endpoint(new_ep, pool)
            return new_ep

    def get_connections(self) -> Dict[str, Dict[str, PeerConnection]]:
        """Return local connections grouped by peer and connection id."""
        result: Dict[str, Dict[str, PeerConnection]] = {}
        with self._connections_lock:
            items = list(self._connections.items())
        for conn_id, conn in items:
            result.setdefault(conn.peer_alias, {})[conn_id] = PeerConnection(
                self, conn_id
            )
        return result

    def query_connection(
        self,
        peer_alias: str,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ) -> Optional[PeerConnection]:
        """Return a connection matching peer and optional NIC filters."""
        matches = []
        with self._connections_lock:
            for conn_id, conn in sorted(self._connections.items()):
                if conn.peer_alias != peer_alias:
                    continue
                if local_nic is not None and conn.local_key.device != local_nic:
                    continue
                if remote_nic is not None and conn.peer_key.device != remote_nic:
                    continue
                matches.append(conn_id)
        if not matches:
            return None
        return PeerConnection(self, matches[0])

    def _is_connection_connected(self, conn_id: str) -> bool:
        with self._connected_peers_lock:
            return conn_id in self._connected_peers

    def _mark_connection_connected(self, conn_id: str) -> None:
        with self._connections_lock:
            conn = self._connections.get(conn_id)
            if conn is not None:
                conn.mark_connected()
        with self._connected_peers_lock:
            self._connected_peers.add(conn_id)
            self._connected_peers_cond.notify_all()

    def _has_notified_peer(self, conn_id: str) -> bool:
        """True iff we've already sent qp_ready for this connection."""
        with self._notified_peers_lock:
            return conn_id in self._notified_peers

    def _mark_peer_notified(self, conn_id: str) -> None:
        with self._notified_peers_lock:
            self._notified_peers.add(conn_id)

    def _clear_peer_notified(self, conn_id: str) -> None:
        """Allow the next handshake cycle to re-notify — called when a
        peer disconnects / is cleaned up so a reconnect works."""
        with self._notified_peers_lock:
            self._notified_peers.discard(conn_id)

    # ------------------------------------------------------------------
    # Topology
    # ------------------------------------------------------------------
    def connect_to(
        self,
        peer_alias: str,
        local_device: Optional[str] = None,
        peer_device: Optional[str] = None,
        ib_port: Optional[int] = 1,
        qp_num: Optional[int] = 1,
        min_bw: Optional[str] = None,
    ) -> PeerConnection:
        """Start connecting to a peer and return a connection handle."""
        if not isinstance(peer_alias, str) or not peer_alias:
            raise TypeError("connect_to() requires a non-empty peer alias string")

        _tlog(f"{self.alias}: connect_to({peer_alias}) ENTER")
        t0 = time.perf_counter()

        local_key = self._first_usable_resource_key(
            self._local_resource,
            device=local_device,
            ib_port=ib_port,
            link_type=None,
        )
        peer_key = self._resolve_peer_resource_key(
            peer_alias,
            peer_device=peer_device,
            ib_port=ib_port if ib_port is not None else local_key.ib_port,
            link_type=local_key.link_type,
            fallback_device=local_key.device,
        )
        if local_key.link_type != peer_key.link_type:
            raise RuntimeError(
                f"Cannot connect {self.alias}:{local_key.device} to "
                f"{peer_alias}:{peer_key.device}: link_type mismatch "
                f"{local_key.link_type} != {peer_key.link_type}"
            )

        conn = self._get_or_create_connection(
            peer_alias,
            local_key=local_key,
            peer_key=peer_key,
            qp_num=qp_num or 1,
        )

        # Pre-create the local endpoint asynchronously. DirectedConnection is
        # only internal state; callers use the returned PeerConnection handle.
        future = self._endpoint_creation_pool.submit(
            self._ensure_local_endpoint_created, conn.conn_id
        )

        def _mark_failed_on_error(f):
            try:
                f.result()
            except Exception as e:
                conn.mark_failed()
                logger.warning(
                    "PeerAgent %s: endpoint pre-create failed: %s", self.alias, e
                )

        future.add_done_callback(_mark_failed_on_error)

        result = self._client.set_desired_topology(
            self.alias,
            target_peers=[peer_alias],
            min_bw=min_bw,
        )
        _tlog(
            f"{self.alias}: connect_to HTTP RTT "
            f"+{(time.perf_counter() - t0) * 1000:.3f}ms"
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"connect_to failed: {result}")

        return PeerConnection(self, conn.conn_id)

    def _wait_connected(self, conn_id: str, timeout_sec: float = 60.0) -> None:
        """Block until one peer connection is ready.

        Event-driven: mark_peer_connected notifies the condition variable,
        so the wakeup latency is whatever threading.Condition adds on top
        of the reconciler's mark — no polling interval floor.
        """
        with self._connections_lock:
            unknown = conn_id not in self._connections
            known = sorted(self._connections)
        if unknown:
            raise ValueError(
                f"wait() got unknown connection: {conn_id!r}. "
                f"Known connections: {known}. Call connect_to(peer, ...) first; "
                "device names belong in connect_to(), not wait()."
            )

        _tlog(f"{self.alias}: wait({conn_id}) ENTER")
        t0 = time.perf_counter()
        deadline = time.monotonic() + timeout_sec
        with self._connected_peers_cond:
            while True:
                if conn_id in self._connected_peers:
                    _tlog(
                        f"{self.alias}: wait({conn_id}) DONE "
                        f"+{(time.perf_counter() - t0) * 1000:.3f}ms"
                    )
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timeout waiting for connection {conn_id!r}. "
                        f"Connected connections: {sorted(self._connected_peers)}"
                    )
                self._connected_peers_cond.wait(timeout=remaining)

    # ------------------------------------------------------------------
    # Memory region registration
    # ------------------------------------------------------------------
    def register_memory_region(
        self,
        mr_name: str,
        ptr: int,
        offset: int,
        length: int,
    ) -> int:
        """Register a logical memory region and publish materialized keys."""
        region = LogicalMemoryRegion(mr_name, ptr, offset, length)
        with self._regions_lock:
            self._logical_regions[mr_name] = region

        # Preserve the old return contract by materializing immediately on a
        # deterministic default resource. Connection-level read/write will
        # materialize again for a different resource key if needed.
        key = self._default_local_resource_key()
        materialized = self._materialize_region(mr_name, key)
        self._publish_memory_keys()
        return materialized.handler

    def _default_local_resource_key(self) -> RdmaResourceKey:
        with self._connections_lock:
            if self._connections:
                return next(iter(self._connections.values())).local_key
        return self._first_usable_resource_key(
            self._local_resource,
            ib_port=1,
            link_type=None,
        )

    def _publish_memory_keys(self) -> None:
        if self._redis_client is None or not self.alias:
            return
        with self._regions_lock:
            memory_keys = sorted(self._logical_regions.keys())
        self._local_resource["memory_keys"] = memory_keys
        try:
            self._redis_client.hset(
                self._agent_key(self.alias),
                mapping={
                    "memory_keys": json.dumps(memory_keys),
                    "resource": json.dumps(
                        {**self._local_resource, "memory_keys": memory_keys}
                    ),
                    "topology": json.dumps(
                        {**self._local_resource, "memory_keys": memory_keys}
                    ),
                    "updated_at": str(int(time.time())),
                },
            )
        except Exception as e:
            logger.warning(
                "PeerAgent %s: Memory key publish warning: %s", self.alias, e
            )

    def unregister_memory_region(self, mr_name: str) -> bool:
        """Unregister a local logical memory region.

        This is a local MR lifecycle primitive. The caller must ensure there are
        no in-flight RDMA operations and no remote peer will continue using a
        previously published addr/rkey. It does not broadcast invalidation to
        peers that may have cached old MR metadata.
        """
        with self._regions_lock:
            had_logical = mr_name in self._logical_regions
            materialized = [
                region
                for key, region in self._materialized_regions.items()
                if key[0] == mr_name
            ]

        if not had_logical and not materialized:
            return False

        for region in materialized:
            try:
                _, pool = self._get_context_and_pool(region.resource_key)
                if not hasattr(pool, "unregister_memory_region"):
                    raise RuntimeError(
                        "underlying memory pool does not support unregister"
                    )
                rc = pool.unregister_memory_region(region.handler)
                if rc != 0:
                    raise RuntimeError(
                        "underlying memory pool returned "
                        f"{rc} for handle {region.handler}"
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to unregister memory region {mr_name!r} on "
                    f"{region.resource_key}: {e}"
                ) from e

        with self._regions_lock:
            self._logical_regions.pop(mr_name, None)
            for region in materialized:
                self._materialized_regions.pop((mr_name, region.resource_key), None)

        if self._redis_client is not None and self.alias:
            prefix = self._redis_prefix()
            mr_key = f"{prefix}mr:{self.alias}:{mr_name}"
            keys = [mr_key]
            keys.extend(
                f"{mr_key}:{region.resource_key.redis_suffix()}"
                for region in materialized
            )
            try:
                self._redis_client.delete(*keys)
            except Exception as e:
                logger.warning(
                    "PeerAgent %s: Memory region Redis cleanup warning for %s: %s",
                    self.alias,
                    mr_name,
                    e,
                )

        with self._mr_info_cache_lock:
            stale_cache_keys = [
                key
                for key in self._mr_info_cache
                if len(key) >= 2 and key[0] == self.alias and key[1] == mr_name
            ]
            for key in stale_cache_keys:
                self._mr_info_cache.pop(key, None)

        self._publish_memory_keys()
        return True

    def _materialize_all_regions_for_key(self, key: RdmaResourceKey) -> None:
        with self._regions_lock:
            names = list(self._logical_regions.keys())
        for name in names:
            self._materialize_region(name, key)

    def _materialize_region(
        self, mr_name: str, key: RdmaResourceKey
    ) -> MaterializedMemoryRegion:
        cache_key = (mr_name, key)
        with self._regions_lock:
            existing = self._materialized_regions.get(cache_key)
            if existing is not None:
                return existing
            region = self._logical_regions.get(mr_name)
            if region is None:
                raise RuntimeError(f"Local memory region '{mr_name}' is not registered")

        _, pool = self._get_context_and_pool(key)
        handler = pool.register_memory_region(
            region.ptr, region.length + region.offset, mr_name
        )
        mr_info = pool.mr_info()[mr_name]

        # Write directly to Redis (p2p, no HTTP)
        prefix = self._redis_prefix()
        mr_key = f"{prefix}mr:{self.alias}:{mr_name}"
        mr_key_specific = f"{mr_key}:{key.redis_suffix()}"

        mr_data = {
            "agent_name": self.alias,
            "mr_name": mr_name,
            "device": key.device,
            "ib_port": key.ib_port,
            "link_type": key.link_type,
            "addr": int(mr_info["addr"]),
            "length": int(mr_info["length"]),
            "rkey": int(mr_info["rkey"]),
            "lkey": 0,
        }

        self._redis_client.set(mr_key, json.dumps(mr_data))
        self._redis_client.set(mr_key_specific, json.dumps(mr_data))

        materialized = MaterializedMemoryRegion(mr_name, key, handler, mr_data)
        with self._regions_lock:
            self._materialized_regions[cache_key] = materialized
        return materialized

    def get_mr_info(
        self,
        peer_alias: str,
        mr_name: str,
        resource_key: Optional[RdmaResourceKey] = None,
        *,
        validate_cache: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Get remote memory region info (p2p via Redis, local cache).

        By default this is a cache-first lookup. Transfer paths pass
        ``validate_cache=True`` so the sender refreshes or invalidates its local
        cache against Redis immediately before resolving the remote handle.
        """
        cache_key = (
            peer_alias,
            mr_name,
            resource_key.redis_suffix() if resource_key else "",
        )

        with self._mr_info_cache_lock:
            cached = self._mr_info_cache.get(cache_key)
            if cached is not None and not validate_cache:
                return cached

        mr_info = self._read_mr_info_from_redis(peer_alias, mr_name, resource_key)
        if mr_info is None:
            with self._mr_info_cache_lock:
                self._mr_info_cache.pop(cache_key, None)
            return None

        with self._mr_info_cache_lock:
            if cached is not None and cached == mr_info:
                return self._mr_info_cache[cache_key]
            self._mr_info_cache[cache_key] = mr_info
        return mr_info

    def _read_mr_info_from_redis(
        self,
        peer_alias: str,
        mr_name: str,
        resource_key: Optional[RdmaResourceKey] = None,
    ) -> Optional[Dict[str, Any]]:
        prefix = self._redis_prefix()
        mr_key = f"{prefix}mr:{peer_alias}:{mr_name}"
        if resource_key is not None:
            specific_key = f"{mr_key}:{resource_key.redis_suffix()}"
            mr_info_str = self._redis_client.get(specific_key)
            if not mr_info_str:
                mr_info_str = self._redis_client.get(mr_key)
        else:
            mr_info_str = self._redis_client.get(mr_key)

        if not mr_info_str:
            return None

        try:
            return json.loads(mr_info_str)
        except json.JSONDecodeError:
            return None

    def _register_remote_memory_region(
        self,
        peer_alias: str,
        mr_name: str,
        mr_info: Dict[str, Any],
        endpoint: Optional[RDMAEndpoint] = None,
    ) -> int:
        """Register remote memory region."""
        if endpoint is None:
            endpoint = self._get_endpoint(peer_alias)
        return endpoint.register_remote_memory_region(mr_name, mr_info)

    def get_handle(
        self,
        mr_name: str,
        peer_alias: Optional[str] = None,
        resource_key: Optional[RdmaResourceKey] = None,
        endpoint: Optional[RDMAEndpoint] = None,
    ) -> int:
        """Resolve local or peer MR name to a handle.

        With ``peer_alias=None`` this materializes a local memory region and
        returns its local handle. With ``peer_alias`` set, it reads the peer's
        published MR info from Redis and registers the remote region on the
        selected endpoint if needed.

        Raises:
            RuntimeError: if the peer has not published the named MR yet.
                Callers who expect to race the publisher should poll
                ``get_mr_info`` directly rather than catching this.
        """
        if peer_alias is None or peer_alias == self.alias:
            key = resource_key or self._default_local_resource_key()
            return self._materialize_region(mr_name, key).handler

        mr_info = self.get_mr_info(
            peer_alias,
            mr_name,
            resource_key=resource_key,
            validate_cache=True,
        )
        if mr_info is None:
            raise RuntimeError(
                f"Remote MR '{mr_name}' from peer '{peer_alias}' is not "
                "published yet. Ensure the peer called "
                "register_memory_region before requesting its handle."
            )
        return self._register_remote_memory_region(
            peer_alias, mr_name, mr_info, endpoint=endpoint
        )

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------
    def _get_connection(
        self,
        peer_alias: str,
        local_device: Optional[str] = None,
        peer_device: Optional[str] = None,
        ib_port: Optional[int] = None,
        qp_num: Optional[int] = None,
    ) -> DirectedConnection:
        matches = []
        with self._connections_lock:
            for _, conn in sorted(self._connections.items()):
                if conn.peer_alias != peer_alias:
                    continue
                if local_device is not None and conn.local_key.device != local_device:
                    continue
                if peer_device is not None and conn.peer_key.device != peer_device:
                    continue
                if ib_port is not None and conn.local_key.ib_port != int(ib_port):
                    continue
                if qp_num is not None and conn.qp_num != int(qp_num):
                    continue
                matches.append(conn)
        if not matches:
            raise RuntimeError(
                f"Connection for {peer_alias} not found. "
                "Call connect_to(peer, ...) first."
            )
        return matches[0]

    def _get_endpoint(
        self,
        peer_alias: str,
        local_device: Optional[str] = None,
        peer_device: Optional[str] = None,
        ib_port: Optional[int] = None,
        qp_num: Optional[int] = None,
    ) -> RDMAEndpoint:
        """Return the established RDMAEndpoint selected by a directed topology."""
        conn = self._get_connection(
            peer_alias,
            local_device=local_device,
            peer_device=peer_device,
            ib_port=ib_port,
            qp_num=qp_num,
        )
        if local_device is not None and conn.local_key.device != local_device:
            raise RuntimeError(
                f"Endpoint for {peer_alias} uses local device "
                f"{conn.local_key.device}, not {local_device}"
            )
        if peer_device is not None and conn.peer_key.device != peer_device:
            raise RuntimeError(
                f"Endpoint for {peer_alias} uses peer device "
                f"{conn.peer_key.device}, not {peer_device}"
            )
        if ib_port is not None and conn.local_key.ib_port != int(ib_port):
            raise RuntimeError(
                f"Endpoint for {peer_alias} uses ib_port "
                f"{conn.local_key.ib_port}, not {ib_port}"
            )
        if qp_num is not None and conn.qp_num != int(qp_num):
            raise RuntimeError(
                f"Endpoint for {peer_alias} uses qp_num {conn.qp_num}, not {qp_num}"
            )

        if conn.endpoint is not None:
            return conn.endpoint

        with self._endpoints_lock:
            endpoint = self._endpoints.get(conn.conn_id)
            if endpoint is None:
                raise RuntimeError(
                    f"Endpoint for {peer_alias} not found. "
                    "Call connect_to(peer, ...).wait() first."
                )
            if conn.endpoint is None:
                _, pool = self._get_context_and_pool(conn.local_key)
                conn.attach_endpoint(endpoint, pool)
            return endpoint

    def _is_named_io_assign(self, value: Any) -> bool:
        return (
            isinstance(value, (list, tuple))
            and bool(value)
            and isinstance(value[0], str)
        )

    def _is_named_io_batch(self, assign: Any) -> bool:
        if self._is_named_io_assign(assign):
            return True
        return (
            isinstance(assign, (list, tuple))
            and bool(assign)
            and self._is_named_io_assign(assign[0])
        )

    def _iter_named_io_assign(self, assign: Any):
        if self._is_named_io_assign(assign):
            yield assign
            return
        yield from assign

    def _endpoint_assign(
        self, conn: DirectedConnection, endpoint: RDMAEndpoint, assign
    ):
        endpoint_assign = []
        for item in self._iter_named_io_assign(assign):
            if len(item) == 4:
                local_region, local_offset, remote_offset, length = item
                remote_region = local_region
            elif len(item) == 5:
                local_region, remote_region, local_offset, remote_offset, length = item
            else:
                raise TypeError(
                    "named RDMA assign must be "
                    "(region, local_offset, remote_offset, length) or "
                    "(local_region, remote_region, local_offset, remote_offset, length)"
                )
            local_handle = self.get_handle(
                str(local_region), resource_key=conn.local_key
            )
            remote_handle = self.get_handle(
                str(remote_region),
                conn.peer_alias,
                resource_key=conn.peer_key,
                endpoint=endpoint,
            )
            endpoint_assign.append(
                (
                    local_handle,
                    remote_handle,
                    int(remote_offset),
                    int(local_offset),
                    int(length),
                )
            )
        return endpoint_assign

    def _maybe_endpoint_assign(
        self, conn: DirectedConnection, endpoint: RDMAEndpoint, assign
    ):
        if self._is_named_io_batch(assign):
            return self._endpoint_assign(conn, endpoint, assign)
        return assign

    # One-shot I/O forwarders. Named batches use PeerAgent memory-key
    # resolution; numeric assignments are passed through to the raw endpoint.
    def read(
        self,
        peer_alias: str,
        assign,
        stream=None,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ):
        conn = self._get_connection(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        )
        endpoint = self._get_endpoint(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        )
        return endpoint.read(
            self._maybe_endpoint_assign(conn, endpoint, assign), stream
        )

    def write(
        self,
        peer_alias: str,
        assign,
        stream=None,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ):
        conn = self._get_connection(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        )
        endpoint = self._get_endpoint(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        )
        return endpoint.write(
            self._maybe_endpoint_assign(conn, endpoint, assign), stream
        )

    def write_with_imm(
        self,
        peer_alias: str,
        assign,
        imm_data: int = 0,
        stream=None,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ):
        conn = self._get_connection(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        )
        endpoint = self._get_endpoint(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        )
        return endpoint.write_with_imm(
            self._maybe_endpoint_assign(conn, endpoint, assign), imm_data, stream
        )

    def send(
        self,
        peer_alias: str,
        chunk,
        stream=None,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ):
        return self._get_endpoint(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        ).send(chunk, stream)

    def recv(
        self,
        peer_alias: str,
        chunk,
        stream=None,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ):
        return self._get_endpoint(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        ).recv(chunk, stream)

    def imm_recv(
        self,
        peer_alias: str,
        stream=None,
        *,
        local_nic: Optional[str] = None,
        remote_nic: Optional[str] = None,
    ):
        return self._get_endpoint(
            peer_alias, local_device=local_nic, peer_device=remote_nic
        ).imm_recv(stream)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """Shutdown and clean up."""
        if self._shutdown_called:
            return
        self._shutdown_called = True

        logger.info("PeerAgent %s: Shutting down...", self.alias)

        self._stop_event.set()
        if self._mailbox:
            self._mailbox.stop()

        if self._event_thread:
            self._event_thread.join(timeout=1)

        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=1)

        # Drain the eager-endpoint-creation pool. Workers here don't block
        # on anything external, so shutdown(wait=True) returns promptly.
        try:
            self._endpoint_creation_pool.shutdown(wait=True)
        except Exception as e:
            logger.warning(
                "PeerAgent %s: endpoint-pool shutdown warning: %s", self.alias, e
            )

        # Clean up exchange keys to prevent stale QP info
        prefix = f"{self._redis_key_prefix}:" if self._redis_key_prefix else ""
        if self._redis_client is not None:
            with self._endpoints_lock:
                with self._connections_lock:
                    endpoint_peers = {
                        conn_id: self._connections[conn_id].peer_alias
                        for conn_id in self._endpoints
                        if conn_id in self._connections
                    }
                for conn_id, peer in endpoint_peers.items():
                    exchange_key_out = f"{prefix}exchange:{self.alias}:{peer}"
                    exchange_key_in = f"{prefix}exchange:{peer}:{self.alias}"
                    exchange_key_out_conn = (
                        f"{prefix}exchange:{self.alias}:{peer}:{conn_id}"
                    )
                    try:
                        self._redis_client.delete(
                            exchange_key_out, exchange_key_in, exchange_key_out_conn
                        )
                    except Exception as e:
                        logger.warning(
                            "PeerAgent %s: Exchange key cleanup warning: %s",
                            self.alias,
                            e,
                        )
                self._endpoints.clear()
        else:
            with self._endpoints_lock:
                self._endpoints.clear()

        with self._connected_peers_lock:
            self._connected_peers.clear()
        with self._notified_peers_lock:
            self._notified_peers.clear()

        # Clean up stream mailbox
        if self._redis_client is not None:
            stream_key = f"{prefix}stream:{self.alias}"
            try:
                self._redis_client.delete(stream_key)
            except Exception as e:
                logger.warning(
                    "PeerAgent %s: Stream cleanup warning: %s", self.alias, e
                )

            # Clean up topology spec
            spec_key = f"{prefix}spec:topology:{self.alias}"
            try:
                self._redis_client.delete(spec_key)
            except Exception as e:
                logger.warning("PeerAgent %s: Spec cleanup warning: %s", self.alias, e)

            # Clean up MR keys (all memory regions registered by this agent)
            mr_pattern = f"{prefix}mr:{self.alias}:*"
            try:
                mr_keys = list(
                    self._redis_client.scan_iter(match=mr_pattern, count=100)
                )
                if mr_keys:
                    self._redis_client.delete(*mr_keys)
            except Exception as e:
                logger.warning("PeerAgent %s: MR cleanup warning: %s", self.alias, e)

        try:
            self._client.cleanup_peer(self.alias)
            logger.info("PeerAgent %s: Cleanup OK", self.alias)
        except Exception as e:
            logger.warning("PeerAgent %s: Cleanup API warning: %s", self.alias, e)

        logger.info("PeerAgent %s: Shutdown complete", self.alias)

    def __enter__(self) -> "PeerAgent":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.shutdown()
        return False

    def __del__(self) -> None:
        if not self._shutdown_called:
            try:
                self.shutdown()
            except Exception as e:
                logger.warning("PeerAgent %s: Warning in __del__: %s", self.alias, e)


def start_peer_agent(
    nanoctrl_url: str = "http://127.0.0.1:3000",
    alias: Optional[str] = None,
    device: Optional[str] = None,
    scope: Optional[str] = None,
) -> PeerAgent:
    """
    Start a peer agent (convenience function).

    Args:
        nanoctrl_url: Control plane URL
        alias: (Optional) Agent name. If None, requests unique name from NanoCtrl.
        device: RDMA device
        scope: Scope string for multi-tenant isolation (used as Redis key prefix).

    Returns:
        PeerAgent instance

    Use connect_to(peer_alias, ...).wait() before data-plane I/O.
    """
    return PeerAgent(
        nanoctrl_url=nanoctrl_url,
        alias=alias,
        device=device,
        scope=scope,
    )
