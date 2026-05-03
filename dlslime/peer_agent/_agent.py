"""PeerAgent: declarative control-plane client + I/O facade.

Owns lifecycle (register, heartbeat, shutdown), per-peer RDMA endpoint
state, MR registration, and the convenience facade over the per-endpoint
I/O primitives. Handshake protocol lives in ``_mailbox.py``.
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set

try:
    import httpx
    import redis
except ImportError as e:
    raise ImportError(
        "PeerAgent requires 'httpx' and 'redis' packages. "
        "Install them with: pip install httpx redis"
    ) from e

from nanoctrl import NanoCtrlClient

from dlslime import available_nic, RDMAContext, RDMAEndpoint, RDMAMemoryPool
from ._mailbox import StreamMailbox
from ._obs import _tlog


class PeerAgent:
    """PeerAgent manages RDMA connections via declarative topology reconciliation."""

    def __init__(
        self,
        alias: Optional[str] = None,
        server_url: str = "http://127.0.0.1:3000",
        redis_address: str = "127.0.0.1:6379",
        device: Optional[str] = None,
        ib_port: int = 1,
        link_type: str = "RoCE",
        qp_num: int = 1,
        name_prefix: str = "agent",
        scope: Optional[str] = None,
    ):
        """
        Initialize a PeerAgent.

        Args:
            alias: (Optional) Agent name. If None, requests unique name from NanoCtrl.
            server_url: URL of the control plane server (NanoCtrl)
            redis_address: Redis server address (host:port)
            device: RDMA device name (e.g., "mlx5_0"), if None, auto-select
            ib_port: InfiniBand port number
            link_type: Link type ("RoCE", "InfiniBand", etc.)
            qp_num: Number of queue pairs per endpoint
            name_prefix: Prefix for auto-generated names (default: "agent")
            scope: Scope string for multi-tenant isolation (used as Redis key prefix).
        """
        self.server_url = server_url
        self.redis_address = redis_address
        self.alias: str = alias or ""  # May be None, will be set during registration
        self.name_prefix = name_prefix
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

        # RDMA
        if self.device is None:
            devices = available_nic()
            if not devices:
                raise RuntimeError("No RDMA devices available")
            self.device = devices[0]

        self._rdma_context = RDMAContext()
        self._rdma_context.init(self.device, self.ib_port, self.link_type)
        self._memory_pool = RDMAMemoryPool(self._rdma_context)

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

        # Redis
        redis_host, redis_port = redis_address.split(":")
        self.redis_client = redis.Redis(
            host=redis_host, port=int(redis_port), decode_responses=True
        )

        self._stop_event = threading.Event()
        self._shutdown_called = False

        # Event listener for cleanup only (legacy inbox)
        self._event_thread: Optional[threading.Thread] = None

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
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._start_heartbeat()

    # ------------------------------------------------------------------
    # Registration / lifecycle
    # ------------------------------------------------------------------
    def _register(self) -> None:
        """Register this agent with the control plane (also allocates name if not provided)."""
        max_retries = 5
        retry_delay = 1.0

        print(f"PeerAgent: Registering with control plane at {self.server_url}")

        for attempt in range(max_retries):
            try:
                result = self._client.register_peer(
                    alias=self.alias or None,
                    device=self.device,
                    ib_port=self.ib_port,
                    link_type=self.link_type,
                    address=self.address,
                    name_prefix=self.name_prefix,
                )

                # Extract allocated name from response
                if "name" in result:
                    self.alias = result["name"]
                    print(f"PeerAgent: Registered with name: {self.alias}")
                else:
                    raise RuntimeError("NanoCtrl did not return agent name")

                if "redis_address" in result:
                    server_redis_address = result["redis_address"]
                    if server_redis_address != self.redis_address:
                        self.redis_address = server_redis_address
                        redis_host, redis_port = self.redis_address.split(":")
                        self.redis_client = redis.Redis(
                            host=redis_host, port=int(redis_port), decode_responses=True
                        )
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
                return ep

        # Slow path: construct outside the lock. Another thread may also be
        # constructing for the same peer; we handle that with a double-check.
        new_ep = RDMAEndpoint(
            pool=self._memory_pool,
            num_qp=self.qp_num,
        )

        with self._endpoints_lock:
            existing = self._endpoints.get(peer_alias)
            if existing is not None:
                # Lost the race. `new_ep` goes out of scope; its destructor
                # releases the QPs we allocated. Cheap in absolute terms
                # (~14 ms once) and rare in practice.
                return existing
            self._endpoints[peer_alias] = new_ep
            return new_ep

    def get_connected_peers(self) -> Set[str]:
        """Return set of peer aliases we've successfully connected to."""
        with self._connected_peers_lock:
            return set(self._connected_peers)

    def is_peer_connected(self, peer_alias: str) -> bool:
        with self._connected_peers_lock:
            return peer_alias in self._connected_peers

    def mark_peer_connected(self, peer_alias: str) -> None:
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
        target_peers: List[str],
        min_bw: Optional[str] = None,
    ) -> None:
        """
        Set desired topology via control plane. NanoCtrl saves to Redis.
        Reconciler will converge to this state.

        Topology is always symmetric: declaring "I want to talk to B"
        implies "B is in a topology with me" because RC RDMA requires
        bilateral QP handshake.

        Args:
            target_peers: List of peer agent aliases to connect to
            min_bw: Optional min bandwidth hint (e.g. "100Gbps"), reserved
        """
        _tlog(f"{self.alias}: set_desired_topology({target_peers}) ENTER")
        t0 = time.perf_counter()
        spec: Dict[str, Any] = {"target_peers": target_peers}
        if min_bw is not None:
            spec["min_bw"] = min_bw
        if self.redis_key_prefix:
            spec["scope"] = self.redis_key_prefix

        # Fire RDMAEndpoint construction in parallel *before* the HTTP RPC
        # returns. By the time NanoCtrl has fanned out connect_peer events
        # and they arrive on our mailbox, every target's endpoint already
        # exists — the listener's ensure_local_endpoint_created becomes a
        # dict lookup instead of a 14-ms QP allocation.
        #
        # We don't wait for the futures: the NanoCtrl RPC round-trip
        # (~10 ms) usually overlaps enough of the construction that it's
        # effectively free. The double-check in ensure_local_endpoint_created
        # keeps the listener safe even if an incoming message races the
        # pre-creation worker.
        eager_futures = [
            self._endpoint_creation_pool.submit(self.ensure_local_endpoint_created, p)
            for p in target_peers
        ]

        result = self._client.set_desired_topology(
            self.alias,
            target_peers=target_peers,
            min_bw=min_bw,
        )
        _tlog(
            f"{self.alias}: set_desired_topology HTTP RTT "
            f"+{(time.perf_counter() - t0) * 1000:.3f}ms"
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"set_desired_topology failed: {result}")

        # Block until eager endpoint creation finishes. After the HTTP RTT
        # most/all are already done, so this typically adds sub-millisecond.
        # We still want to wait so that has_peer_connected() / connect_to()
        # callers see a stable state.
        t_join = time.perf_counter()
        for f in eager_futures:
            try:
                f.result(timeout=30.0)
            except Exception as e:
                print(
                    f"PeerAgent {self.alias}: eager endpoint pre-creation failed: {e}"
                )
        _tlog(
            f"{self.alias}: set_desired_topology eager-endpoints-wait "
            f"+{(time.perf_counter() - t_join) * 1000:.3f}ms"
        )

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
        """Register local memory region (p2p via Redis, no control plane)."""
        handler = self._memory_pool.register_memory_region(
            ptr, length + offset, mr_name
        )
        mr_info = self._memory_pool.mr_info()[mr_name]

        # Write directly to Redis (p2p, no HTTP)
        prefix = f"{self.redis_key_prefix}:" if self.redis_key_prefix else ""
        mr_key = f"{prefix}mr:{self.alias}:{mr_name}"

        mr_data = {
            "agent_name": self.alias,
            "mr_name": mr_name,
            "addr": int(mr_info["addr"]),
            "length": int(mr_info["length"]),
            "rkey": int(mr_info["rkey"]),
            "lkey": 0,
        }

        self.redis_client.set(mr_key, json.dumps(mr_data))
        return handler

    def get_mr_info(self, peer_alias: str, mr_name: str) -> Optional[Dict[str, Any]]:
        """Get remote memory region info (p2p via Redis, persistent cache).

        MR info is immutable after registration, so cache never expires.
        Architecture: Redis (source of truth) → PeerAgent cache (0µs hot path).
        """
        cache_key = (peer_alias, mr_name)

        # Check cache first (fast path: 0µs)
        with self._mr_info_cache_lock:
            if cache_key in self._mr_info_cache:
                return self._mr_info_cache[cache_key]

        # Cache miss: read directly from Redis (p2p, no HTTP)
        prefix = f"{self.redis_key_prefix}:" if self.redis_key_prefix else ""
        mr_key = f"{prefix}mr:{peer_alias}:{mr_name}"
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
    ) -> int:
        """Register remote memory region."""
        with self._endpoints_lock:
            if peer_alias not in self._endpoints:
                raise RuntimeError(f"Endpoint for {peer_alias} not initialized")
            endpoint = self._endpoints[peer_alias]
        return endpoint.register_remote_memory_region(mr_name, mr_info)

    def get_remote_handle(self, peer_alias: str, mr_name: str) -> int:
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
        mr_info = self.get_mr_info(peer_alias, mr_name)
        if mr_info is None:
            raise RuntimeError(
                f"Remote MR '{mr_name}' from peer '{peer_alias}' is not "
                "published yet. Ensure the peer called "
                "register_memory_region before requesting its handle."
            )
        return self.register_remote_memory_region(peer_alias, mr_name, mr_info)

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------
    def get_endpoint(self, peer_alias: str) -> RDMAEndpoint:
        """Get RDMA endpoint for peer (must be connected)."""
        with self._endpoints_lock:
            if peer_alias not in self._endpoints:
                raise RuntimeError(
                    f"Endpoint for {peer_alias} not found. "
                    "Ensure set_desired_topology([...]) includes this peer and wait for reconciliation."
                )
            return self._endpoints[peer_alias]

    # One-shot I/O forwarders (convenience facade over get_endpoint().op()).
    #
    # Cost: one dict lookup + one Python lock acquire per call; O(1) per
    # submission, not O(n) per assignment. Hot loops should still cache
    # the endpoint once (ep = agent.get_endpoint(peer)) and call
    # ep.<op>(...) directly.
    def read(self, peer_alias: str, assign, stream=None):
        return self.get_endpoint(peer_alias).read(assign, stream)

    def write(self, peer_alias: str, assign, stream=None):
        return self.get_endpoint(peer_alias).write(assign, stream)

    def write_with_imm(self, peer_alias: str, assign, imm_data: int = 0, stream=None):
        return self.get_endpoint(peer_alias).write_with_imm(assign, imm_data, stream)

    def send(self, peer_alias: str, chunk, stream=None):
        return self.get_endpoint(peer_alias).send(chunk, stream)

    def recv(self, peer_alias: str, chunk, stream=None):
        return self.get_endpoint(peer_alias).recv(chunk, stream)

    def imm_recv(self, peer_alias: str, stream=None):
        return self.get_endpoint(peer_alias).imm_recv(stream)

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
        with self._endpoints_lock:
            for peer in list(self._endpoints.keys()):
                exchange_key_out = f"{prefix}exchange:{self.alias}:{peer}"
                exchange_key_in = f"{prefix}exchange:{peer}:{self.alias}"
                try:
                    self.redis_client.delete(exchange_key_out, exchange_key_in)
                except Exception as e:
                    print(f"PeerAgent {self.alias}: Exchange key cleanup warning: {e}")
            self._endpoints.clear()

        with self._connected_peers_lock:
            self._connected_peers.clear()
        with self._notified_peers_lock:
            self._notified_peers.clear()

        # Clean up stream mailbox
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
    address: Optional[str] = None,
    device: Optional[str] = None,
    ib_port: int = 1,
    link_type: str = "RoCE",
    qp_num: int = 1,
    name_prefix: str = "agent",
    scope: Optional[str] = None,
) -> PeerAgent:
    """
    Start a peer agent (convenience function).

    Args:
        alias: (Optional) Agent name. If None, requests unique name from NanoCtrl.
        server_url: Control plane URL
        address: Redis address
        device: RDMA device
        ib_port: InfiniBand port
        link_type: Link type
        qp_num: Number of queue pairs
        name_prefix: Prefix for auto-generated names (default: "agent")
        scope: Scope string for multi-tenant isolation (used as Redis key prefix).

    Returns:
        PeerAgent instance

    Use set_desired_topology(target_peers=[...]) to declare which peers to connect to.
    """
    redis_address = address if address is not None else "127.0.0.1:6379"
    return PeerAgent(
        alias=alias,
        server_url=server_url,
        redis_address=redis_address,
        device=device,
        ib_port=ib_port,
        link_type=link_type,
        qp_num=qp_num,
        name_prefix=name_prefix,
        scope=scope,
    )
