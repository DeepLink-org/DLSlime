"""Channel — bidirectional tagged message primitive over RDMA.

Wire format per message:
    ┌──────────┬────────────────────┐
    │ tag (u32)│ payload (bytes)    │
    └──────────┴────────────────────┘
    imm_data = total bytes (tag + payload)
"""

import ctypes

from dlslime import _slime_c
from .buffer import GrowableBuffer

TAG_SIZE = 4  # u32 tag header

# Tag-space conventions (high bits of the u32 tag):
#   REPLY_BIT = reply message
#   CHUNK_BIT = chunked transfer
#   LAST_BIT  = final chunk in a chunked transfer
#
# These flags must be independent. Reusing REPLY_BIT|CHUNK_BIT as LAST_BIT
# makes a reply chunk indistinguishable from a last chunk.
REPLY_BIT = 1 << 29
ACK_BIT = 1 << 28
CHUNK_BIT = 1 << 30
LAST_BIT = 1 << 31
FLAG_MASK = REPLY_BIT | ACK_BIT | CHUNK_BIT | LAST_BIT


class Channel:
    """One peer ↔ peer bidirectional link.

    A Channel owns two buffers:
      - send_buf: local staging area, content is RDMA-written to remote recv_buf
      - recv_buf: our mailbox, the remote peer RDMA-writes into it
    """

    def __init__(
        self,
        endpoint: _slime_c.RDMAEndpoint,
        send_buf: GrowableBuffer,
        recv_buf: GrowableBuffer,
        remote_handler: int,
    ):
        self._ep = endpoint
        self._send = send_buf
        self._recv = recv_buf
        self._remote_handler = remote_handler
        # Large payloads need chunking for robustness, but too-small chunks turn
        # every RPC into a stop-and-wait protocol with very poor throughput.
        # Default to at most 32MB and never exceed the mailbox capacity.
        self._chunk_size = min(32 * 1024 * 1024, self.size)

    # ── Zero-copy access ─────────────────────────────

    @property
    def ptr(self) -> int:
        """Payload write pointer (skip the 4-byte tag header)."""
        return self._send.ptr + TAG_SIZE

    @property
    def size(self) -> int:
        """Usable payload capacity in bytes."""
        return self._send.capacity - TAG_SIZE

    def ensure_send_capacity(self, payload_bytes: int):
        """Grow send buffer if needed."""
        self._send.ensure_capacity(TAG_SIZE + payload_bytes)

    # ── Send ─────────────────────────────────────────

    def send(self, tag: int, nbytes: int):
        """Blocking send [tag|payload] to remote mailbox."""
        self._post_write(tag, nbytes).wait()

    def send_async(self, tag: int, nbytes: int):
        """Non-blocking send.  Returns ``SlimeReadWriteFuture``."""
        return self._post_write(tag, nbytes)

    def _post_write(self, tag: int, nbytes: int):
        if self._send.handler < 0:
            raise RuntimeError(
                "Local send buffer not registered with RDMA. "
                "Ensure GrowableBuffer was created with endpoint= or pool=."
            )
        if self._remote_handler < 0:
            raise RuntimeError(
                "Remote recv buffer not registered. "
                "Ensure the peer has published its MR via register_memory_region()."
            )
        ctypes.c_uint32.from_address(self._send.ptr).value = tag
        total = TAG_SIZE + nbytes
        return self._ep.write_with_imm(
            [(self._send.handler, self._remote_handler, 0, 0, total)], total
        )

    # ── Recv ─────────────────────────────────────────

    def recv(self) -> tuple[int, int, int]:
        """Block until a message arrives.

        Returns:
            ``(tag, payload_ptr, payload_nbytes)``

        Raises:
            ConnectionError: if a zero-byte completion is received, which
                indicates the remote side has shut down its endpoint.
            RuntimeError: if the incoming message exceeds recv buffer capacity.
        """
        future = self._ep.imm_recv()
        future.wait()
        total = future.imm_data()
        if total < TAG_SIZE:
            raise ConnectionError(
                f"RDMA channel closed (received {total}-byte completion, "
                f"expected at least {TAG_SIZE})."
            )
        if total > self._recv.capacity:
            raise RuntimeError(
                f"Incoming message ({total} B) exceeds recv buffer capacity "
                f"({self._recv.capacity} B). Increase the initial buffer size."
            )
        tag = ctypes.c_uint32.from_address(self._recv.ptr).value
        return tag, self._recv.ptr + TAG_SIZE, total - TAG_SIZE

    def recv_message(self) -> tuple[int, int, int, object | None]:
        """Receive one logical message, reassembling chunked payloads if needed.

        Returns:
            ``(base_tag, payload_ptr, payload_nbytes, keepalive)``

        ``keepalive`` is ``None`` for non-chunked messages. For chunked messages
        it owns the backing buffer so callers can safely use ``payload_ptr`` for
        the duration of their processing.
        """
        tag, ptr, nbytes = self._recv_non_ack()
        if not (tag & CHUNK_BIT):
            return tag, ptr, nbytes, None

        base_tag, data = self._recv_chunked_from_first(tag, ptr, nbytes)
        keepalive = ctypes.create_string_buffer(data)
        return base_tag, ctypes.addressof(keepalive), len(data), keepalive

    # ── Chunked send (large payloads) ────────────────

    def send_chunked(
        self, tag: int, data: bytes | memoryview, chunk_size: int = 4 * 1024 * 1024
    ):
        """Send large payload in chunks.  Receiver reassembles via ``recv_chunked``."""
        mv = memoryview(data) if isinstance(data, bytes) else data
        base_tag = tag & ~FLAG_MASK
        for offset in range(0, len(mv), chunk_size):
            chunk = mv[offset : offset + chunk_size]
            n = len(chunk)
            self.ensure_send_capacity(n)
            ctypes.memmove(self.ptr, bytes(chunk), n)
            is_last = offset + chunk_size >= len(mv)
            chunk_tag = tag | CHUNK_BIT
            if is_last:
                chunk_tag |= LAST_BIT
            self.send(chunk_tag, n)
            if not is_last:
                self._wait_for_ack(base_tag)

    def recv_chunked(self) -> tuple[int, bytes]:
        """Receive a chunked stream produced by ``send_chunked``.

        Returns:
            ``(base_tag, assembled_bytes)``
        """
        parts: list[bytes] = []
        while True:
            tag, ptr, nbytes = self.recv()
            parts.append(bytes((ctypes.c_char * nbytes).from_address(ptr)))
            if tag & LAST_BIT:
                base_tag = tag & ~FLAG_MASK
                return base_tag, b"".join(parts)
            # else it's a CHUNK_BIT — keep reading

    def _recv_chunked_from_first(
        self, tag: int, ptr: int, nbytes: int
    ) -> tuple[int, bytes]:
        """Continue assembling a chunked message after the first chunk is read."""
        base_tag = tag & ~FLAG_MASK
        parts = [bytes((ctypes.c_char * nbytes).from_address(ptr))]
        if tag & LAST_BIT:
            return base_tag, b"".join(parts)

        self._send_ack(base_tag)

        while True:
            next_tag, next_ptr, next_nbytes = self._recv_non_ack()
            parts.append(bytes((ctypes.c_char * next_nbytes).from_address(next_ptr)))
            if (next_tag & ~FLAG_MASK) != base_tag:
                raise RuntimeError(
                    f"Chunk stream tag mismatch: expected {base_tag:#x}, got {next_tag:#x}"
                )
            if next_tag & LAST_BIT:
                return base_tag, b"".join(parts)
            self._send_ack(base_tag)

    def _send_ack(self, base_tag: int) -> None:
        self.send(base_tag | ACK_BIT, 0)

    def _wait_for_ack(self, base_tag: int) -> None:
        while True:
            tag, _, _ = self.recv()
            if tag & ACK_BIT:
                if (tag & ~FLAG_MASK) != base_tag:
                    raise RuntimeError(
                        f"Chunk ACK tag mismatch: expected {base_tag:#x}, got {tag:#x}"
                    )
                return
            raise RuntimeError(f"Expected chunk ACK, got tag={tag:#x}")

    def _recv_non_ack(self) -> tuple[int, int, int]:
        while True:
            tag, ptr, nbytes = self.recv()
            if tag & ACK_BIT:
                continue
            return tag, ptr, nbytes
