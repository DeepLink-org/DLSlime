"""
PeerAgent: Control plane client for DLSlime RDMA connection management.

Event-driven architecture using Redis Streams:
- Each agent has a stream mailbox: stream:{agent_id}
- NanoCtrl pushes connection commands to agent streams
- Agent listens to stream and initiates p2p bootstrap
- QP info exchange via Redis (exchange:{sender}:{receiver})
- No polling, pure message-driven
"""

import json
import threading
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set

try:
    import redis
    import requests
except ImportError as e:
    raise ImportError(
        "PeerAgent requires 'requests' and 'redis' packages. "
        "Install them with: pip install requests redis"
    ) from e

from dlslime import available_nic, RDMAContext, RDMAEndpoint, RDMAMemoryPool


def create_redis_prefix(server_url: str) -> str:
    """Create Redis key prefix from NanoCtrl server URL for data isolation."""
    # Remove protocol and sanitize (e.g., http://10.102.97.179:3000 -> nano_10_102_97_179_3000)
    sanitized = (
        server_url.replace("http://", "")
        .replace("https://", "")
        .replace(":", "_")
        .replace("/", "_")
        .replace(".", "_")
        .replace("-", "_")
    )
    return f"nano_{sanitized}"


class StreamMailbox:
    """
    Redis Streams-based mailbox for receiving connection commands from NanoCtrl.
    Pure event-driven, no polling. Listens to stream:{agent_alias}.
    """

    def __init__(self, agent: "PeerAgent", stream_block_ms: int = 100):
        """
        Args:
            agent: PeerAgent instance
            stream_block_ms: XREAD block timeout in milliseconds (default 100ms)
        """
        self._agent = agent
        self._stream_block_ms = stream_block_ms
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the stream listener thread."""
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()
        print(
            f"StreamMailbox {self._agent.alias}: Started listening to stream "
            f"(block={self._stream_block_ms}ms)"
        )

    def stop(self) -> None:
        """Stop the stream listener."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        print(f"StreamMailbox {self._agent.alias}: Stopped")

    def _listen_loop(self) -> None:
        """Main event loop: blocking XREAD on agent's stream mailbox."""
        prefix = (
            f"{self._agent.redis_key_prefix}:" if self._agent.redis_key_prefix else ""
        )
        stream_key = f"{prefix}stream:{self._agent.alias}"

        # Start from beginning (0-0 = all messages in stream)
        # Safe because cleanup removes stream on shutdown
        last_id = "0-0"

        print(
            f"StreamMailbox {self._agent.alias}: Listening to {stream_key} (from beginning)"
        )

        while not self._stop_event.is_set():
            try:
                # XREAD with blocking
                result = self._agent.redis_client.xread(
                    {stream_key: last_id},
                    block=self._stream_block_ms,
                    count=100,  # Batch up to 100 messages
                )

                if not result:
                    continue  # Timeout, no messages

                # Process messages
                for _, messages in result:
                    for msg_id, fields in messages:
                        try:
                            self._handle_message(fields)
                        except Exception as e:
                            print(
                                f"StreamMailbox {self._agent.alias}: Error handling message: {e}"
                            )
                            import traceback

                            traceback.print_exc()
                        last_id = msg_id

            except redis.exceptions.ConnectionError:
                time.sleep(0.1)
            except Exception as e:
                print(f"StreamMailbox {self._agent.alias}: Error in listen loop: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(0.1)

    def _handle_message(self, fields: Dict[str, str]) -> None:
        """Handle incoming stream message."""
        msg_type = fields.get("type")

        if msg_type == "connect_peer":
            # Command from NanoCtrl: connect to specific peer
            peer = fields.get("peer")
            if peer:
                print(
                    f"StreamMailbox {self._agent.alias}: Received connect_peer -> {peer}"
                )
                self._try_connect_peer(peer)
            else:
                print(
                    f"StreamMailbox {self._agent.alias}: connect_peer missing 'peer' field"
                )

        elif msg_type == "qp_ready":
            # Notification from peer: their QP info is ready
            peer = fields.get("peer")
            if peer:
                print(
                    f"StreamMailbox {self._agent.alias}: Received qp_ready from {peer}"
                )
                self._try_connect_peer(peer)
            else:
                print(
                    f"StreamMailbox {self._agent.alias}: qp_ready missing 'peer' field"
                )

        else:
            print(
                f"StreamMailbox {self._agent.alias}: Unknown message type: {msg_type}"
            )

    def _try_connect_peer(self, peer: str) -> None:
        """
        Attempt symmetric rendezvous with peer.
        Idempotent: safe to call multiple times.
        """
        # Skip if already connected
        if self._agent.is_peer_connected(peer):
            return

        # A. Create local endpoint and get QP info
        endpoint = self._agent.ensure_local_endpoint_created(peer)
        my_qp_info = endpoint.endpoint_info()

        # B. Publish our QP info to exchange key
        prefix = (
            f"{self._agent.redis_key_prefix}:" if self._agent.redis_key_prefix else ""
        )
        exchange_key_out = f"{prefix}exchange:{self._agent.alias}:{peer}"
        self._agent.redis_client.set(
            exchange_key_out,
            json.dumps(my_qp_info, default=str),
        )

        # C. Notify peer via their stream that our QP info is ready
        peer_stream_key = f"{prefix}stream:{peer}"
        try:
            self._agent.redis_client.xadd(
                peer_stream_key,
                {
                    "type": "qp_ready",
                    "peer": self._agent.alias,
                    "timestamp": str(time.time()),
                },
                maxlen=1000,
                approximate=True,
            )
        except Exception as e:
            print(f"StreamMailbox {self._agent.alias}: Failed to notify {peer}: {e}")

        # D. Try to fetch peer's QP info (non-blocking)
        exchange_key_in = f"{prefix}exchange:{peer}:{self._agent.alias}"
        peer_qp_info_str = self._agent.redis_client.get(exchange_key_in)

        if peer_qp_info_str is None:
            # Peer hasn't published yet; they will notify us when ready
            return

        try:
            peer_qp_info = json.loads(peer_qp_info_str)
        except json.JSONDecodeError:
            print(
                f"StreamMailbox {self._agent.alias}: Failed to parse QP info from {peer}"
            )
            return

        # E. Complete RDMA handshake
        endpoint.connect(peer_qp_info)
        self._agent.mark_peer_connected(peer)
        print(f"Link Established: {self._agent.alias} <-> {peer}")


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
        self.alias = alias  # May be None, will be set during registration
        self.name_prefix = name_prefix
        self.device = device
        self.ib_port = ib_port
        self.link_type = link_type
        self.qp_num = qp_num

        # Build Redis key prefix from scope parameter
        self.redis_key_prefix = scope or ""

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

        # Start StreamMailbox (event-driven connection management)
        self._mailbox = StreamMailbox(self, stream_block_ms=100)
        self._mailbox.start()

        # Start cleanup event listener
        self._start_cleanup_listener()

        time.sleep(0.1)

    def _register(self) -> None:
        """Register this agent with the control plane (also allocates name if not provided)."""
        max_retries = 5
        retry_delay = 1.0

        print(f"PeerAgent: Registering with control plane at {self.server_url}")

        for attempt in range(max_retries):
            try:
                # Build request - omit alias if None (NanoCtrl will generate)
                request_data = {
                    "device": self.device,
                    "ib_port": self.ib_port,
                    "link_type": self.link_type,
                    "address": self.address,
                    "name_prefix": self.name_prefix,
                }
                if self.alias is not None:
                    request_data["alias"] = self.alias
                if self.redis_key_prefix:
                    request_data["scope"] = self.redis_key_prefix

                response = requests.post(
                    f"{self.server_url}/start_peer_agent",
                    json=request_data,
                    timeout=10,
                )
                response.raise_for_status()
                result = response.json()

                # Extract allocated name from response
                if "name" in result:
                    self.alias = result["name"]
                    if request_data.get("alias"):
                        print(f"PeerAgent: Registered with provided name: {self.alias}")
                    else:
                        print(
                            f"PeerAgent: Registered with allocated name: {self.alias}"
                        )
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
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError,
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

        def event_loop():
            inbox_key = f"{self.redis_key_prefix}:inbox:{self.alias}"
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
                                    with self._connected_peers_lock:
                                        self._connected_peers.discard(peer)
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

    def ensure_local_endpoint_created(self, peer_alias: str) -> RDMAEndpoint:
        """
        Idempotent: create endpoint for peer if not exists.
        Returns the endpoint (existing or newly created). Thread-safe.
        """
        with self._endpoints_lock:
            if peer_alias not in self._endpoints:
                endpoint = RDMAEndpoint(
                    pool=self._memory_pool,
                    num_qp=self.qp_num,
                )
                self._endpoints[peer_alias] = endpoint
            return self._endpoints[peer_alias]

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

    def set_desired_topology(
        self,
        target_peers: List[str],
        min_bw: Optional[str] = None,
        symmetric: bool = False,
    ) -> None:
        """
        Set desired topology via control plane. NanoCtrl saves to Redis.
        Reconciler will converge to this state.

        Args:
            target_peers: List of peer agent aliases to connect to
            min_bw: Optional min bandwidth hint (e.g. "100Gbps"), reserved
            symmetric: If True, NanoCtrl also merges this agent into each target's spec.
                Required when only one side initiates (e.g. decode -> prefill migration).
        """
        spec: Dict[str, Any] = {"target_peers": target_peers}
        if min_bw is not None:
            spec["min_bw"] = min_bw
        if symmetric:
            spec["symmetric"] = True
        if self.redis_key_prefix:
            spec["scope"] = self.redis_key_prefix
        response = requests.post(
            f"{self.server_url}/v1/desired_topology/{self.alias}",
            json=spec,
            timeout=5,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "ok":
            raise RuntimeError(f"set_desired_topology failed: {result}")

    def query(self) -> Dict[str, Dict[str, Any]]:
        """Query all registered peer agents."""
        query_data = {}
        if self.redis_key_prefix:
            query_data["scope"] = self.redis_key_prefix
        response = requests.post(
            f"{self.server_url}/query",
            json=query_data,
            timeout=5,
        )
        response.raise_for_status()
        agents = response.json()
        return {agent["name"]: agent for agent in agents}

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

    def get_endpoint(self, peer_alias: str) -> RDMAEndpoint:
        """Get RDMA endpoint for peer (must be connected)."""
        with self._endpoints_lock:
            if peer_alias not in self._endpoints:
                raise RuntimeError(
                    f"Endpoint for {peer_alias} not found. "
                    "Ensure set_desired_topology([...]) includes this peer and wait for reconciliation."
                )
            return self._endpoints[peer_alias]

    def wait_for_peers(self, peers: List[str], timeout_sec: float = 60.0) -> None:
        """
        Block until all specified peers are connected.
        Useful for tests / sync points after set_desired_topology.
        """
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            connected = self.get_connected_peers()
            missing = [p for p in peers if p not in connected]
            if not missing:
                return
            time.sleep(0.5)
        raise TimeoutError(
            f"Timeout waiting for peers {peers}. "
            f"Connected: {self.get_connected_peers()}"
        )

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
            cleanup_data = {"agent_name": self.alias}
            if self.redis_key_prefix:
                cleanup_data["scope"] = self.redis_key_prefix
            response = requests.post(
                f"{self.server_url}/cleanup",
                json=cleanup_data,
                timeout=5,
            )
            response.raise_for_status()
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
