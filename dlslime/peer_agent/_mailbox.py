"""StreamMailbox: Redis-Streams listener + symmetric rendezvous handshake.

Wire protocol (stream:{agent_alias}):

- ``{type: connect_peer, peer: <alias>}`` — pushed by NanoCtrl when the
  reconciler decides the local agent should connect to ``peer``.
  NanoCtrl does not know the peer's QP info, so receiving this triggers
  the outbound ``qp_ready`` path below and waits for the peer's reply.

- ``{type: qp_ready, peer: <alias>, qp_info: <JSON>}`` — pushed by a
  peer once it has allocated QPs and wants us to complete the handshake.
  The QP info is embedded so we can ``endpoint.connect()`` without a
  Redis GET round-trip.

The listener runs on a single background thread. Heavy work
(`RDMAEndpoint(...)` allocation, QP state transitions) happens inline
on that thread, so per-message cost directly affects the scale-out tail.
``PeerAgent`` pre-creates endpoints eagerly from ``set_desired_topology``
to keep the listener fast path a dict lookup.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

import redis

from ._obs import _tlog

if TYPE_CHECKING:
    from ._agent import PeerAgent


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
        # Delete any stale stream messages from a previous run before listening.
        prefix = (
            f"{self._agent.redis_key_prefix}:" if self._agent.redis_key_prefix else ""
        )
        stream_key = f"{prefix}stream:{self._agent.alias}"
        self._agent.redis_client.delete(stream_key)

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

        # Start from the beginning of the (now-freshly-cleared) stream.
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

                _tlog(
                    f"{self._agent.alias}: XREAD returned "
                    f"{sum(len(m) for _, m in result)} msg(s)"
                )

                # Process messages
                for _, messages in result:
                    for msg_id, fields in messages:
                        _tlog(
                            f"{self._agent.alias}: dispatch msg_id={msg_id} "
                            f"type={fields.get('type')} peer={fields.get('peer')}"
                        )
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
            # Command from NanoCtrl: connect to specific peer.
            # NanoCtrl does not know the peer's RDMA QP info, so we fall
            # through to the outbound-notify path: publish our QP to the
            # peer, wait for their qp_ready (which carries their QP info)
            # to complete the handshake.
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
            # Notification from peer: their QP info is ready, and (new
            # protocol) embedded directly in this message. Old-protocol
            # peers omit the qp_info field; we fall back to the Redis
            # exchange-key GET in _try_connect_peer_inner for interop.
            peer = fields.get("peer")
            if peer:
                qp_info_str = fields.get("qp_info")
                peer_qp_info: Optional[Dict[str, Any]] = None
                conn_meta: Dict[str, Any] = {}
                if qp_info_str:
                    try:
                        peer_qp_info = json.loads(qp_info_str)
                    except json.JSONDecodeError:
                        print(
                            f"StreamMailbox {self._agent.alias}: "
                            f"malformed qp_info from {peer}, falling back to GET"
                        )
                meta_str = fields.get("conn_meta")
                if meta_str:
                    try:
                        conn_meta = json.loads(meta_str)
                    except json.JSONDecodeError:
                        print(
                            f"StreamMailbox {self._agent.alias}: "
                            f"malformed conn_meta from {peer}, using defaults"
                        )
                print(
                    f"StreamMailbox {self._agent.alias}: Received qp_ready from {peer}"
                    f"{' (with embedded qp_info)' if peer_qp_info else ''}"
                )
                self._try_connect_peer(
                    peer, peer_qp_info=peer_qp_info, conn_meta=conn_meta
                )
            else:
                print(
                    f"StreamMailbox {self._agent.alias}: qp_ready missing 'peer' field"
                )

        else:
            print(
                f"StreamMailbox {self._agent.alias}: Unknown message type: {msg_type}"
            )

    def _try_connect_peer(
        self,
        peer: str,
        peer_qp_info: Optional[Dict[str, Any]] = None,
        conn_meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Attempt symmetric rendezvous with peer.
        Idempotent: safe to call multiple times.
        """
        # Skip if already connected or a connection attempt is in progress.
        if self._agent.is_peer_connected(peer):
            return
        with self._agent._connecting_peers_lock:
            if peer in self._agent._connecting_peers:
                return
            self._agent._connecting_peers.add(peer)

        try:
            self._try_connect_peer_inner(peer, peer_qp_info, conn_meta or {})
        finally:
            # Always release the in-flight guard. A rendezvous attempt may return
            # early when the peer has not published its QP info yet; later
            # connect_peer / qp_ready messages must be allowed to retry.
            with self._agent._connecting_peers_lock:
                self._agent._connecting_peers.discard(peer)

    def _try_connect_peer_inner(
        self,
        peer: str,
        peer_qp_info: Optional[Dict[str, Any]] = None,
        conn_meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Core rendezvous logic (called with connecting-guard held).

        Fast path: if `peer_qp_info` is provided (extracted from the
        peer's qp_ready stream message), use it directly — no Redis GET.

        Slow/interop path: if `peer_qp_info` is None, publish our QP info
        to the Redis exchange key and GET the peer's, as in the original
        protocol. Keeps this version interoperable with older peers that
        don't embed QP info in their stream messages.
        """
        t_enter = time.perf_counter()
        _tlog(
            f"{self._agent.alias}: _try_connect_peer_inner({peer}) "
            f"has_qp_info={peer_qp_info is not None}"
        )

        if conn_meta:
            self._agent.ensure_connection_from_meta(peer, conn_meta)

        # A. Create local endpoint and get QP info
        endpoint = self._agent.ensure_local_endpoint_created(peer)
        my_qp_info = endpoint.endpoint_info()
        my_conn_meta = self._agent._connection_meta(peer)
        _tlog(
            f"{self._agent.alias}: [A] ensure_endpoint+endpoint_info "
            f"+{(time.perf_counter() - t_enter) * 1000:.3f}ms"
        )

        prefix = (
            f"{self._agent.redis_key_prefix}:" if self._agent.redis_key_prefix else ""
        )

        # B. Notify the peer that our QP info is ready — once. Carrying the
        # QP info in the message itself lets the peer complete their half
        # of the handshake with zero Redis GETs. The has_notified_peer
        # guard prevents a qp_ready → qp_ready → qp_ready ping-pong when
        # both sides are racing through this method.
        if not self._agent.has_notified_peer(peer):
            t_b = time.perf_counter()
            peer_stream_key = f"{prefix}stream:{peer}"
            try:
                self._agent.redis_client.xadd(
                    peer_stream_key,
                    {
                        "type": "qp_ready",
                        "peer": self._agent.alias,
                        "qp_info": json.dumps(my_qp_info, default=str),
                        "conn_meta": json.dumps(my_conn_meta, default=str),
                        "timestamp": str(time.time()),
                    },
                    maxlen=1000,
                    approximate=True,
                )
                self._agent.mark_peer_notified(peer)
            except Exception as e:
                print(
                    f"StreamMailbox {self._agent.alias}: Failed to notify {peer}: {e}"
                )
            _tlog(
                f"{self._agent.alias}: [B] XADD qp_ready->{peer} "
                f"+{(time.perf_counter() - t_b) * 1000:.3f}ms"
            )
        else:
            _tlog(f"{self._agent.alias}: [B] skip XADD (already notified {peer})")

        # C. Resolve peer's QP info. Fast path: we already have it from the
        # incoming qp_ready message. Interop path: fall back to the Redis
        # exchange-key protocol for older peers.
        if peer_qp_info is None:
            t_c = time.perf_counter()
            exchange_key_out = f"{prefix}exchange:{self._agent.alias}:{peer}"
            self._agent.redis_client.set(
                exchange_key_out,
                json.dumps(my_qp_info, default=str),
            )
            exchange_key_in = f"{prefix}exchange:{peer}:{self._agent.alias}"
            peer_qp_info_str = self._agent.redis_client.get(exchange_key_in)
            _tlog(
                f"{self._agent.alias}: [C-interop] SET+GET exchange "
                f"+{(time.perf_counter() - t_c) * 1000:.3f}ms "
                f"got={'yes' if peer_qp_info_str else 'None'}"
            )
            if peer_qp_info_str is None:
                # Peer hasn't published yet; their qp_ready will trigger us.
                _tlog(
                    f"{self._agent.alias}: _try_connect_peer_inner({peer}) "
                    f"RETURN (peer not published yet) "
                    f"total={(time.perf_counter() - t_enter) * 1000:.3f}ms"
                )
                return
            try:
                peer_qp_info = json.loads(peer_qp_info_str)
            except json.JSONDecodeError:
                print(
                    f"StreamMailbox {self._agent.alias}: "
                    f"Failed to parse exchange-key QP info from {peer}"
                )
                return

        # D. Complete RDMA handshake
        t_d = time.perf_counter()
        endpoint.connect(peer_qp_info)
        _tlog(
            f"{self._agent.alias}: [D] endpoint.connect({peer}) "
            f"+{(time.perf_counter() - t_d) * 1000:.3f}ms"
        )
        self._agent.mark_peer_connected(peer)
        _tlog(
            f"{self._agent.alias}: [E] mark_peer_connected({peer}) DONE "
            f"total={(time.perf_counter() - t_enter) * 1000:.3f}ms"
        )
        print(f"Link Established: initiator={self._agent.alias} target={peer}")
