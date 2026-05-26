"""Channel — bookkeeping wrapper around the registered RDMA buffers.

The C++ ``RpcSession`` (dlslime/csrc/rpc/rpc_session.{h,cpp}) reads the
endpoint, send/recv buffer pointers + handlers, remote handler, slot_size
and slot_count from this object. All datapath operations (slot semaphore,
header packing, send/recv WRs, slot_id imm framing) live in C++.

Wire format and constants are defined in ``rpc_constants.h`` and
mirrored here for Python-side introspection only.
"""

from .buffer import GrowableBuffer

# Bump on any wire-incompatible change so cross-version pairs fail fast.
# Mirrored from dlslime/csrc/rpc/rpc_constants.h.
WIRE_VERSION = 3

HEADER_SIZE = 16  # u32 tag, u32 total_len, u64 request_id

# IB imm_data is a 32-bit field on the wire; pybind binds it as int32_t,
# so the largest slot_count we can ever encode is 2**31. In practice
# slot counts are O(16) and we cap at 1<<16 to keep things sane.
MAX_SLOT_COUNT = 1 << 16


class Channel:
    """One peer ↔ peer bidirectional link.

    A Channel owns two buffers:
      - send_buf: local staging area, content is RDMA-written to remote recv_buf
      - recv_buf: our slotted mailbox, the remote peer RDMA-writes into it

    The Python side keeps the ``GrowableBuffer`` objects alive for the
    lifetime of the C++ ``RpcSession``; the C++ session borrows their
    pointers and MR handlers.
    """

    def __init__(
        self,
        endpoint,
        send_buf: GrowableBuffer,
        recv_buf: GrowableBuffer,
        remote_handler: int,
        *,
        slot_size: int | None = None,
        slot_count: int = 1,
    ):
        self._ep = endpoint
        self._send = send_buf
        self._recv = recv_buf
        self._remote_handler = remote_handler
        self._slot_size = slot_size or recv_buf.capacity
        self._slot_count = slot_count
        if self._slot_count < 1 or self._slot_count > MAX_SLOT_COUNT:
            raise ValueError(
                f"slot_count must be in [1, {MAX_SLOT_COUNT}], got {slot_count}"
            )
        if self._slot_size < HEADER_SIZE + 1:
            raise ValueError(
                f"slot_size {self._slot_size} too small for header ({HEADER_SIZE}B)"
            )
        if self._slot_size * self._slot_count > recv_buf.capacity:
            raise ValueError(
                f"recv_buf capacity {recv_buf.capacity} < slot_size*slot_count "
                f"({self._slot_size}*{self._slot_count})"
            )
        self._slot_payload_capacity = self._slot_size - HEADER_SIZE

    @property
    def ptr(self) -> int:
        """Payload write pointer (skip the wire header)."""
        return self._send.ptr + HEADER_SIZE

    @property
    def size(self) -> int:
        """Usable payload capacity in bytes."""
        return self._slot_payload_capacity

    @property
    def slot_count(self) -> int:
        return self._slot_count

    @property
    def slot_size(self) -> int:
        return self._slot_size
