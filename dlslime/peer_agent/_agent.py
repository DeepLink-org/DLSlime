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
from ._mailbox import StreamMailbox
from ._obs import _tlog


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


class PeerAgent:
    """PeerAgent manages RDMA connections via declarative topology reconciliation."""

    def __init__(
        self,
        alias: Optional[str] = None,
        server_url: str = "http://127.0.0.1:3000",
        device: Optional[str] = None,
        ib_port: int = 1,
        link_type: Optional[str] = None,
        qp_num: int = 1,
        scope: Optional[str] = None,
    ):
        """
        Initialize a PeerAgent.

        Args:
            alias: (Optional) Agent name. If None, requests unique name from NanoCtrl.
            server_url: URL of the control plane server (NanoCtrl)
            device: RDMA device name (e.g., "mlx5_0"), if None, auto-select
            ib_port: InfiniBand port number
            link_type: Optional compatibility override. Prefer topology discovery.
            qp_num: Number of queue pairs per endpoint
            scope: Scope string for multi-tenant isolation (used as Redis key prefix).
        """
        self.server_url = server_url
        self.redis_address: Optional[str] = None
        self.alias: str = alias or ""
        self.device = device
        self.ib_port = ib_port
        self.link_type = link_type
        self.qp_num = qp_num

        # Build Redis key prefix from scope parameter
        self.redis_key_prefix = scope or ""

        # NanoCtrl HTTP client
        self._client = NanoCtrlClient(server_url, scope=self.redis_key_prefix or None)

        import socket

        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        self.address = local_ip

        # Local topology is discovered before registration and published through
        # NanoCtrl/Redis. RDMA resources are created lazily per selected
        # (device, port, link_type) instead of being bound to the whole agent.
        self._local_resource = self._discover_local_resource(
            preferred_device=device,
            preferred_ib_port=ib_port,
            preferred_link_type=link_type,
        )
        self._resource_cache: Dict[str, Dict[str, Any]] = {}
        self._resource_cache_lock = threading.Lock()

        self._endpoints: Dict[str, RDMAEndpoint] = {}
        self._endpoints_lock = (
            threading.Lock()
        )  # Protects _endpoints for concurrent reconcile
        self._connected_peers: Set[str] = set()
        self._connected_peers_lock = threading.Lock()
        # Notified whenever _connected_peers changes, so `wait_for_peers`
        # can block on a condition instead of polling. Shares the existing
        # lock so is_peer_connected / mark_peer_connected call sites are
        # unchanged.
        self._connected_peers_cond = threading.Condition(self._connected_peers_lock)

        # Prevents duplicate in-flight connect attempts for the same peer.
        self._connecting_peers: Set[str] = set()
        self._connecting_peers_lock = threading.Lock()

        # Peers we've already XADDed our qp_ready to. Used to suppress
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
        # set_desired_topology. RDMAEndpoint() allocates QPs and takes
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
        self.redis_client: Optional[redis.Redis] = None

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
        return f"{self.redis_key_prefix}:" if self.redis_key_prefix else ""

    def _agent_key(self, alias: str) -> str:
        return f"{self._redis_prefix()}agent:{alias}"

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

        print(f"PeerAgent: Registering with control plane at {self.server_url}")

        for attempt in range(max_retries):
            try:
                result = self._register_peer_with_nanoctrl()

                # Extract allocated name from response
                if "name" in result:
                    self.alias = result["name"]
                    print(f"PeerAgent: Registered with name: {self.alias}")
                else:
                    raise RuntimeError("NanoCtrl did not return agent name")

                if "redis_address" not in result:
                    raise RuntimeError("NanoCtrl did not return redis_address")

                self.redis_address = result["redis_address"]
                redis_host, redis_port = self.redis_address.split(":")
                self.redis_client = redis.Redis(
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
                    print(
                        f"PeerAgent {self.alias} registration failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    print(
                        f"PeerAgent {self.alias} registration failed after {max_retries} attempts"
                    )
                    raise

    def _register_peer_with_nanoctrl(self) -> Dict[str, Any]:
        """Call NanoCtrlClient.register_peer across installed client versions."""
        params = inspect.signature(self._client.register_peer).parameters
        kwargs: Dict[str, Any] = {
            "alias": self.alias or None,
            "address": self.address,
        }

        if "resource" in params:
            kwargs["resource"] = self._local_resource

        if {"device", "ib_port", "link_type", "name_prefix"} & set(params):
            key = self._first_usable_resource_key(
                self._local_resource,
                device=self.device,
                ib_port=self.ib_port,
                link_type=self.link_type,
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
        if self.redis_client is None or not self.alias:
            return
        memory_keys = sorted(self._logical_regions.keys())
        self._local_resource["memory_keys"] = memory_keys
        key = self._agent_key(self.alias)
        try:
            self.redis_client.hset(
                key,
                mapping={
                    "addr": self.address,
                    "resource": json.dumps(self._local_resource),
                    "topology": json.dumps(self._local_resource),
                    "memory_keys": json.dumps(memory_keys),
                    "updated_at": str(int(time.time())),
                },
            )
        except Exception as e:
            print(f"PeerAgent {self.alias}: Resource publish warning: {e}")

    def query_active_agent(self) -> List[str]:
        """Return active peer aliases in the same scope from Redis."""
        if self.redis_client is None:
            return []
        prefix = self._redis_prefix()
        pattern = f"{prefix}agent:*"
        key_prefix = f"{prefix}agent:"
        active: List[str] = []
        for key in self.redis_client.scan_iter(match=pattern, count=200):
            ttl = self.redis_client.ttl(key)
            if ttl == 0 or ttl == -2:
                continue
            active.append(str(key).removeprefix(key_prefix))
        return sorted(active)

    def query_resource(self, peer_alias: str) -> Optional[Dict[str, Any]]:
        """Read a peer's published topology/resource JSON from Redis."""
        if peer_alias == self.alias:
            return self._local_resource
        with self._resource_cache_lock:
            cached = self._resource_cache.get(peer_alias)
            if cached is not None:
                return cached
        if self.redis_client is None:
            return None
        record = self.redis_client.hgetall(self._agent_key(peer_alias))
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

    def query_mem_keys(self, peer_alias: Optional[str] = None) -> List[str]:
        """Return local or peer logical memory region names."""
        if peer_alias is None or peer_alias == self.alias:
            with self._regions_lock:
                return sorted(self._logical_regions.keys())
        if self.redis_client is None:
            return []
        record = self.redis_client.hgetall(self._agent_key(peer_alias))
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
        for key in self.redis_client.scan_iter(match=pattern, count=200):
            suffix = str(key).removeprefix(mr_prefix)
            keys.append(suffix.split(":", 1)[0])
        return sorted(set(keys))

    def _start_cleanup_listener(self) -> None:
        """Listen for cleanup events from peers (NanoCtrl pushes to inbox)."""
        # Flush any stale events left by a previous run so we don't act on them.
        inbox_key = f"{self.redis_key_prefix}:inbox:{self.alias}"
        self.redis_client.delete(inbox_key)

        def event_loop():
            while not self._stop_event.is_set():
                try:
                    result = self.redis_client.blpop(inbox_key, timeout=1)
                    if result:
                        _, event_str = result
                        event = json.loads(event_str)
                        if event.get("type") == "cleanup":
                            peer = event.get("peer")
                            print(f"PeerAgent {self.alias}: Cleanup from peer {peer}")
                            with self._endpoints_lock:
                                if peer in self._endpoints:
                                    # Force-complete any blocked RDMA waits before
                                    # dropping the endpoint reference. Otherwise a
                                    # thread blocked in imm_recv()/wait() can hang
                                    # forever after the remote peer exits.
                                    endpoint = self._endpoints[peer]
                                    if hasattr(endpoint, "shutdown"):
                                        endpoint.shutdown()
                                    with self._connected_peers_lock:
                                        self._connected_peers.discard(peer)
                                        self._connected_peers_cond.notify_all()
                                    self.clear_peer_notified(peer)
                                    del self._endpoints[peer]
                                    print(
                                        f"PeerAgent {self.alias}: Removed endpoint for {peer}"
                                    )
                except redis.exceptions.ConnectionError:
                    time.sleep(0.1)
                except Exception as e:
                    print(f"PeerAgent {self.alias}: Cleanup listener error: {e}")
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

        prefix = f"{self.redis_key_prefix}:" if self.redis_key_prefix else ""
        patterns = [
            f"{prefix}exchange:{self.alias}:*",
            f"{prefix}exchange:*:{self.alias}",
        ]
        try:
            keys_to_delete: list[str] = []
            for pattern in patterns:
                keys_to_delete.extend(
                    list(self.redis_client.scan_iter(match=pattern, count=200))
                )
            if keys_to_delete:
                self.redis_client.delete(*keys_to_delete)
        except Exception as e:
            print(f"PeerAgent {self.alias}: Exchange cleanup warning: {e}")

    def _cleanup_stale_mr_keys(self) -> None:
        """Delete stale Redis MR keys registered by a previous run of this alias.

        RPC mailboxes are process-local buffers. If a previous process crashed
        before shutdown, peers may discover the old addr/rkey from Redis and
        attempt RDMA writes into dead memory, which surfaces as local/remote
        access errors on the first RPC.
        """
        if not self.alias:
            return

        prefix = f"{self.redis_key_prefix}:" if self.redis_key_prefix else ""
        pattern = f"{prefix}mr:{self.alias}:*"
        try:
            keys_to_delete = list(self.redis_client.scan_iter(match=pattern, count=200))
            if keys_to_delete:
                self.redis_client.delete(*keys_to_delete)
        except Exception as e:
            print(f"PeerAgent {self.alias}: MR cleanup warning: {e}")

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
                        print(
                            f"PeerAgent {self.alias}: Heartbeat returned not_found, "
                            f"re-registering..."
                        )
                        self._register()
                except Exception as e:
                    print(f"PeerAgent {self.alias}: Heartbeat failed: {e}")

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
        peer_resource = self.query_resource(peer_alias)
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
        fallback_link = link_type or self.link_type or fallback_local.link_type
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
            existing = self._connections.get(peer_alias)
            if existing is not None:
                if local_key is not None and (
                    existing.local_key != local_key
                    or (peer_key is not None and existing.peer_key != peer_key)
                    or (qp_num is not None and existing.qp_num != qp_num)
                ):
                    raise RuntimeError(
                        f"Multiple directed connections to {peer_alias} are not "
                        "supported by the first PeerAgent implementation. "
                        "Use query_endpoint(peer, local_device, peer_device, ...) "
                        "to select an established data-plane endpoint."
                    )
                return existing

            if local_key is None:
                local_key = self._first_usable_resource_key(
                    self._local_resource, ib_port=self.ib_port, link_type=self.link_type
                )
            if peer_key is None:
                peer_key = RdmaResourceKey(
                    local_key.device,
                    local_key.ib_port,
                    local_key.link_type,
                )
            conn = DirectedConnection(
                self,
                peer_alias,
                local_key,
                peer_key,
                qp_num or self.qp_num,
                profile=profile,
            )
            self._connections[peer_alias] = conn
            return conn

    def _connection_meta(self, peer_alias: str) -> Dict[str, Any]:
        conn = self._get_or_create_connection(peer_alias)
        return {
            "conn_id": conn.conn_id,
            "src": self.alias,
            "dst": peer_alias,
            "src_device": conn.local_key.device,
            "dst_device": conn.peer_key.device,
            "ib_port": conn.local_key.ib_port,
            "qp_num": conn.qp_num,
            "link_type": conn.local_key.link_type,
            "profile": conn.profile,
        }

    def ensure_connection_from_meta(
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
            qp_num=int(meta.get("qp_num") or self.qp_num),
            profile=str(meta.get("profile") or "default"),
        )

    def ensure_local_endpoint_created(self, peer_alias: str) -> RDMAEndpoint:
        """
        Idempotent: create endpoint for peer if not exists.
        Returns the endpoint (existing or newly created). Thread-safe.

        The RDMAEndpoint constructor allocates QPs, which takes ~10-15 ms
        per call. We release `_endpoints_lock` during construction so that
        concurrent pre-creation of many endpoints (see set_desired_topology)
        actually runs in parallel — otherwise holding the lock would
        serialize the QP allocations and defeat the purpose.
        """
        # Fast path: lookup under lock.
        with self._endpoints_lock:
            ep = self._endpoints.get(peer_alias)
            if ep is not None:
                conn = self._get_or_create_connection(peer_alias)
                if conn.endpoint is None:
                    _, pool = self._get_context_and_pool(conn.local_key)
                    conn.attach_endpoint(ep, pool)
                return ep

        conn = self._get_or_create_connection(peer_alias)
        _, pool = self._get_context_and_pool(conn.local_key)
        self._materialize_all_regions_for_key(conn.local_key)

        # Slow path: construct outside the lock. Another thread may also be
        # constructing for the same peer; we handle that with a double-check.
        new_ep = RDMAEndpoint(
            pool=pool,
            num_qp=conn.qp_num,
        )

        with self._endpoints_lock:
            existing = self._endpoints.get(peer_alias)
            if existing is not None:
                # Lost the race. `new_ep` goes out of scope; its destructor
                # releases the QPs we allocated. Cheap in absolute terms
                # (~14 ms once) and rare in practice.
                return existing
            self._endpoints[peer_alias] = new_ep
            conn.attach_endpoint(new_ep, pool)
            return new_ep

    def get_connected_peers(self) -> Set[str]:
        """Return set of peer aliases we've successfully connected to."""
        with self._connected_peers_lock:
            return set(self._connected_peers)

    def is_peer_connected(self, peer_alias: str) -> bool:
        with self._connected_peers_lock:
            return peer_alias in self._connected_peers

    def mark_peer_connected(self, peer_alias: str) -> None:
        with self._connections_lock:
            conn = self._connections.get(peer_alias)
            if conn is not None:
                conn.mark_connected()
        with self._connected_peers_lock:
            self._connected_peers.add(peer_alias)
            self._connected_peers_cond.notify_all()

    def has_notified_peer(self, peer_alias: str) -> bool:
        """True iff we've already sent our qp_ready to this peer during the
        current session. Suppresses the redundant outbound qp_ready that
        would otherwise fire on every qp_ready we *receive*."""
        with self._notified_peers_lock:
            return peer_alias in self._notified_peers

    def mark_peer_notified(self, peer_alias: str) -> None:
        with self._notified_peers_lock:
            self._notified_peers.add(peer_alias)

    def clear_peer_notified(self, peer_alias: str) -> None:
        """Allow the next handshake cycle to re-notify — called when a
        peer disconnects / is cleaned up so a reconnect works."""
        with self._notified_peers_lock:
            self._notified_peers.discard(peer_alias)

    # ------------------------------------------------------------------
    # Topology
    # ------------------------------------------------------------------
    def set_desired_topology(
        self,
        peer_alias: Optional[str] = None,
        local_device: Optional[str] = None,
        peer_device: Optional[str] = None,
        ib_port: Optional[int] = 1,
        qp_num: Optional[int] = 1,
        min_bw: Optional[str] = None,
        target_peers: Optional[List[str]] = None,
    ):
        """Declare directed connection(s) and start async rendezvous.

        New form:
            set_desired_topology(peer_alias, local_device=None, peer_device=None,
                                 ib_port=1, qp_num=1)

        Old list/keyword form is accepted for examples and benches while they
        migrate.
        """
        if target_peers is not None:
            for p in target_peers:
                self.set_desired_topology(
                    p,
                    local_device=local_device,
                    peer_device=peer_device,
                    ib_port=ib_port,
                    qp_num=qp_num,
                    min_bw=min_bw,
                )
            return None
        if isinstance(peer_alias, list):
            for p in peer_alias:
                self.set_desired_topology(
                    p,
                    local_device=local_device,
                    peer_device=peer_device,
                    ib_port=ib_port,
                    qp_num=qp_num,
                    min_bw=min_bw,
                )
            return None
        if peer_alias is None:
            raise TypeError("set_desired_topology() requires peer_alias")

        _tlog(f"{self.alias}: set_desired_topology({peer_alias}) ENTER")
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
            qp_num=qp_num or self.qp_num,
        )

        # Pre-create the local endpoint asynchronously. DirectedConnection is
        # only internal state; callers use wait_for_peers() and query_endpoint().
        future = self._endpoint_creation_pool.submit(
            self.ensure_local_endpoint_created, peer_alias
        )

        def _mark_failed_on_error(f):
            try:
                f.result()
            except Exception as e:
                conn.mark_failed()
                print(f"PeerAgent {self.alias}: endpoint pre-create failed: {e}")

        future.add_done_callback(_mark_failed_on_error)

        result = self._client.set_desired_topology(
            self.alias,
            target_peers=[peer_alias],
            min_bw=min_bw,
        )
        _tlog(
            f"{self.alias}: set_desired_topology HTTP RTT "
            f"+{(time.perf_counter() - t0) * 1000:.3f}ms"
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"set_desired_topology failed: {result}")

        return None

    def query(self) -> Dict[str, Dict[str, Any]]:
        """Query all registered peer agents."""
        agents = self._client.query_peers()
        return {agent["name"]: agent for agent in agents}

    def wait_for_peers(self, peers: List[str], timeout_sec: float = 60.0) -> None:
        """
        Block until all specified peers are connected.
        Useful for tests / sync points after set_desired_topology.

        Event-driven: mark_peer_connected notifies the condition variable,
        so the wakeup latency is whatever threading.Condition adds on top
        of the reconciler's mark — no polling interval floor.
        """
        _tlog(f"{self.alias}: wait_for_peers({peers}) ENTER")
        t0 = time.perf_counter()
        deadline = time.monotonic() + timeout_sec
        with self._connected_peers_cond:
            while True:
                missing = [p for p in peers if p not in self._connected_peers]
                if not missing:
                    _tlog(
                        f"{self.alias}: wait_for_peers({peers}) DONE "
                        f"+{(time.perf_counter() - t0) * 1000:.3f}ms"
                    )
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timeout waiting for peers {peers}. "
                        f"Connected: {sorted(self._connected_peers)}"
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
            self._local_resource, ib_port=self.ib_port, link_type=self.link_type
        )

    def _publish_memory_keys(self) -> None:
        if self.redis_client is None or not self.alias:
            return
        with self._regions_lock:
            memory_keys = sorted(self._logical_regions.keys())
        try:
            self.redis_client.hset(
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
            print(f"PeerAgent {self.alias}: Memory key publish warning: {e}")

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

        self.redis_client.set(mr_key, json.dumps(mr_data))
        self.redis_client.set(mr_key_specific, json.dumps(mr_data))

        materialized = MaterializedMemoryRegion(mr_name, key, handler, mr_data)
        with self._regions_lock:
            self._materialized_regions[cache_key] = materialized
        return materialized

    def get_local_handle(
        self, mr_name: str, resource_key: Optional[RdmaResourceKey] = None
    ) -> int:
        key = resource_key or self._default_local_resource_key()
        return self._materialize_region(mr_name, key).handler

    def get_mr_info(
        self,
        peer_alias: str,
        mr_name: str,
        resource_key: Optional[RdmaResourceKey] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get remote memory region info (p2p via Redis, persistent cache).

        MR info is immutable after registration, so cache never expires.
        Architecture: Redis (source of truth) → PeerAgent cache (0µs hot path).
        """
        cache_key = (
            peer_alias,
            mr_name,
            resource_key.redis_suffix() if resource_key else "",
        )

        # Check cache first (fast path: 0µs)
        with self._mr_info_cache_lock:
            if cache_key in self._mr_info_cache:
                return self._mr_info_cache[cache_key]

        # Cache miss: read directly from Redis (p2p, no HTTP)
        prefix = self._redis_prefix()
        mr_key = f"{prefix}mr:{peer_alias}:{mr_name}"
        if resource_key is not None:
            specific_key = f"{mr_key}:{resource_key.redis_suffix()}"
            mr_info_str = self.redis_client.get(specific_key)
            if not mr_info_str:
                mr_info_str = self.redis_client.get(mr_key)
        else:
            mr_info_str = self.redis_client.get(mr_key)

        if not mr_info_str:
            return None

        try:
            mr_info = json.loads(mr_info_str)
        except json.JSONDecodeError:
            return None

        # Store in cache (persistent, no expiration)
        with self._mr_info_cache_lock:
            self._mr_info_cache[cache_key] = mr_info

        return mr_info

    def register_remote_memory_region(
        self,
        peer_alias: str,
        mr_name: str,
        mr_info: Dict[str, Any],
        endpoint: Optional[RDMAEndpoint] = None,
    ) -> int:
        """Register remote memory region."""
        if endpoint is None:
            endpoint = self.query_endpoint(peer_alias)
        return endpoint.register_remote_memory_region(mr_name, mr_info)

    def get_remote_handle(
        self,
        peer_alias: str,
        mr_name: str,
        resource_key: Optional[RdmaResourceKey] = None,
        endpoint: Optional[RDMAEndpoint] = None,
    ) -> int:
        """Resolve a peer's published MR name to a local remote-MR handle.

        One-shot convenience for the common pattern of
        ``get_mr_info`` + ``register_remote_memory_region``. Both underlying
        steps are idempotent (info is cached; the remote pool returns the
        existing handle on repeat registration), so calling this repeatedly
        is cheap.

        Raises:
            RuntimeError: if the peer has not published the named MR yet.
                Callers who expect to race the publisher should poll
                ``get_mr_info`` directly rather than catching this.
        """
        mr_info = self.get_mr_info(peer_alias, mr_name, resource_key=resource_key)
        if mr_info is None:
            raise RuntimeError(
                f"Remote MR '{mr_name}' from peer '{peer_alias}' is not "
                "published yet. Ensure the peer called "
                "register_memory_region before requesting its handle."
            )
        return self.register_remote_memory_region(
            peer_alias, mr_name, mr_info, endpoint=endpoint
        )

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------
    def _get_connection(self, peer_alias: str) -> DirectedConnection:
        with self._connections_lock:
            conn = self._connections.get(peer_alias)
        if conn is None:
            raise RuntimeError(
                f"Connection for {peer_alias} not found. "
                "Call set_desired_topology(peer, ...) first."
            )
        return conn

    def query_endpoint(
        self,
        peer_alias: str,
        local_device: Optional[str] = None,
        peer_device: Optional[str] = None,
        ib_port: Optional[int] = None,
        qp_num: Optional[int] = None,
    ) -> RDMAEndpoint:
        """Return the established RDMAEndpoint selected by a directed topology."""
        conn = self._get_connection(peer_alias)
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
            endpoint = self._endpoints.get(peer_alias)
            if endpoint is None:
                raise RuntimeError(
                    f"Endpoint for {peer_alias} not found. "
                    "Call set_desired_topology(peer, ...), then wait_for_peers([...])."
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
            local_handle = self.get_local_handle(str(local_region), conn.local_key)
            remote_handle = self.get_remote_handle(
                conn.peer_alias,
                str(remote_region),
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
    def read(self, peer_alias: str, assign, stream=None):
        conn = self._get_connection(peer_alias)
        endpoint = self.query_endpoint(peer_alias)
        return endpoint.read(
            self._maybe_endpoint_assign(conn, endpoint, assign), stream
        )

    def write(self, peer_alias: str, assign, stream=None):
        conn = self._get_connection(peer_alias)
        endpoint = self.query_endpoint(peer_alias)
        return endpoint.write(
            self._maybe_endpoint_assign(conn, endpoint, assign), stream
        )

    def write_with_imm(
        self,
        peer_alias: str,
        assign,
        imm_data: int = 0,
        stream=None,
    ):
        conn = self._get_connection(peer_alias)
        endpoint = self.query_endpoint(peer_alias)
        return endpoint.write_with_imm(
            self._maybe_endpoint_assign(conn, endpoint, assign), imm_data, stream
        )

    def send(self, peer_alias: str, chunk, stream=None):
        return self.query_endpoint(peer_alias).send(chunk, stream)

    def recv(self, peer_alias: str, chunk, stream=None):
        return self.query_endpoint(peer_alias).recv(chunk, stream)

    def imm_recv(self, peer_alias: str, stream=None):
        return self.query_endpoint(peer_alias).imm_recv(stream)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """Shutdown and clean up."""
        if self._shutdown_called:
            return
        self._shutdown_called = True

        print(f"PeerAgent {self.alias}: Shutting down...")

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
            print(f"PeerAgent {self.alias}: endpoint-pool shutdown warning: {e}")

        # Clean up exchange keys to prevent stale QP info
        prefix = f"{self.redis_key_prefix}:" if self.redis_key_prefix else ""
        if self.redis_client is not None:
            with self._endpoints_lock:
                for peer in list(self._endpoints.keys()):
                    exchange_key_out = f"{prefix}exchange:{self.alias}:{peer}"
                    exchange_key_in = f"{prefix}exchange:{peer}:{self.alias}"
                    try:
                        self.redis_client.delete(exchange_key_out, exchange_key_in)
                    except Exception as e:
                        print(
                            f"PeerAgent {self.alias}: Exchange key cleanup warning: {e}"
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
        if self.redis_client is not None:
            stream_key = f"{prefix}stream:{self.alias}"
            try:
                self.redis_client.delete(stream_key)
            except Exception as e:
                print(f"PeerAgent {self.alias}: Stream cleanup warning: {e}")

            # Clean up topology spec
            spec_key = f"{prefix}spec:topology:{self.alias}"
            try:
                self.redis_client.delete(spec_key)
            except Exception as e:
                print(f"PeerAgent {self.alias}: Spec cleanup warning: {e}")

            # Clean up MR keys (all memory regions registered by this agent)
            mr_pattern = f"{prefix}mr:{self.alias}:*"
            try:
                mr_keys = list(self.redis_client.scan_iter(match=mr_pattern, count=100))
                if mr_keys:
                    self.redis_client.delete(*mr_keys)
            except Exception as e:
                print(f"PeerAgent {self.alias}: MR cleanup warning: {e}")

        try:
            self._client.cleanup_peer(self.alias)
            print(f"PeerAgent {self.alias}: Cleanup OK")
        except Exception as e:
            print(f"PeerAgent {self.alias}: Cleanup API warning: {e}")

        print(f"PeerAgent {self.alias}: Shutdown complete")

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
                print(f"PeerAgent {self.alias}: Warning in __del__: {e}")


def start_peer_agent(
    alias: Optional[str] = None,
    server_url: str = "http://127.0.0.1:3000",
    device: Optional[str] = None,
    ib_port: int = 1,
    link_type: Optional[str] = None,
    qp_num: int = 1,
    scope: Optional[str] = None,
) -> PeerAgent:
    """
    Start a peer agent (convenience function).

    Args:
        alias: (Optional) Agent name. If None, requests unique name from NanoCtrl.
        server_url: Control plane URL
        device: RDMA device
        ib_port: InfiniBand port
        link_type: Optional compatibility override. Prefer topology discovery.
        qp_num: Number of queue pairs
        scope: Scope string for multi-tenant isolation (used as Redis key prefix).

    Returns:
        PeerAgent instance

    Use set_desired_topology(peer_alias, ...) to declare a directed connection,
    then wait_for_peers([...]) before data-plane I/O.
    """
    return PeerAgent(
        alias=alias,
        server_url=server_url,
        device=device,
        ib_port=ib_port,
        link_type=link_type,
        qp_num=qp_num,
        scope=scope,
    )
