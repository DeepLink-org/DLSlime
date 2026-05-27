"""Adapter that exposes a ``TcpEndpoint`` with the same surface PeerAgent
already calls on ``RDMAEndpoint`` (``send/recv/read/write``, plus
``endpoint_info``/``connect``/``register_memory_region``). The C++
``TcpEndpoint`` uses ``async_*`` names and accepts an extra ``timeout_ms``
kwarg; this thin shim collapses those differences without copying the
underlying object.

``write_with_imm`` and ``imm_recv`` are RDMA-specific (write-with-immediate-
data) and have no TCP analogue; they raise ``NotImplementedError`` so the
peer agent's I/O facade can keep its uniform call shape and still surface a
clear error if a caller tries to use them on a TCP connection.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from dlslime import TcpEndpoint


class TcpEndpointAdapter:
    """Wrap a ``TcpEndpoint`` to match the ``RDMAEndpoint`` method surface."""

    def __init__(self, endpoint: TcpEndpoint) -> None:
        self._endpoint = endpoint

    @property
    def raw(self) -> TcpEndpoint:
        return self._endpoint

    def endpoint_info(self) -> Dict[str, Any]:
        return self._endpoint.endpoint_info()

    def connect(self, peer_info: Dict[str, Any]) -> None:
        self._endpoint.connect(peer_info)

    def is_connected(self) -> bool:
        return self._endpoint.is_connected()

    def shutdown(self) -> None:
        self._endpoint.shutdown()

    def mr_info(self) -> Dict[str, Any]:
        return self._endpoint.mr_info()

    def register_memory_region(
        self, name: str, ptr: int, offset: int, length: int
    ) -> int:
        return self._endpoint.register_memory_region(name, ptr, offset, length)

    def register_remote_memory_region(self, name: str, mr_info: Dict[str, Any]) -> int:
        return self._endpoint.register_remote_memory_region(name, mr_info)

    # ------------------------------------------------------------------
    # Two-sided primitives. ``stream`` is an RDMA/CUDA concept and is
    # accepted only for signature parity; TCP ignores it.
    # ------------------------------------------------------------------
    def send(self, chunk, stream: Optional[Any] = None):
        return self._endpoint.async_send(chunk)

    def recv(self, chunk, stream: Optional[Any] = None):
        return self._endpoint.async_recv(chunk)

    # ------------------------------------------------------------------
    # One-sided primitives.
    # ------------------------------------------------------------------
    def read(self, assign, stream: Optional[Any] = None):
        return self._endpoint.async_read(self._normalize_assign(assign))

    def write(self, assign, stream: Optional[Any] = None):
        return self._endpoint.async_write(self._normalize_assign(assign))

    def write_with_imm(self, assign, imm_data: int = 0, stream: Optional[Any] = None):
        raise NotImplementedError(
            "TCP transport does not support write_with_imm; this is RDMA-only."
        )

    def imm_recv(self, stream: Optional[Any] = None):
        raise NotImplementedError(
            "TCP transport does not support imm_recv; this is RDMA-only."
        )

    @staticmethod
    def _normalize_assign(assign):
        # The C++ async_read/async_write bindings expect a list of tuples.
        # PeerAgent's RDMA path passes either a single tuple or a list.
        if isinstance(assign, tuple):
            return [assign]
        return assign
