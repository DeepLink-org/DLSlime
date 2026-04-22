"""proxy() — caller-side transparent handle + channel factory."""

import ctypes
import threading
from typing import Any

from .buffer import GrowableBuffer
from .channel import Channel
from .registry import MethodRegistry

MR_NAME = "rpc:mailbox"

# Global channel cache keyed by (agent_id, peer_alias).
_channel_cache: dict[tuple[int, str], Channel] = {}
_cache_lock = threading.Lock()


def _get_or_create_channel(agent, peer_alias: str) -> Channel:
    """Return (or lazily create) the Channel to *peer_alias*.

    Channel creation:
      1. Get the RDMA endpoint that PeerAgent already set up.
      2. Allocate a send buffer (per-peer) and a recv buffer (shared name).
      3. Publish our recv buffer as MR ``"rpc:mailbox"`` to Redis.
      4. Discover the peer's ``"rpc:mailbox"`` MR from Redis.
    """
    key = (id(agent), peer_alias)
    with _cache_lock:
        if key in _channel_cache:
            return _channel_cache[key]

    endpoint = agent.get_endpoint(peer_alias)
    buf_size = getattr(agent, "_rpc_buffer_size", 32_000_000)

    send_buf = GrowableBuffer(
        buf_size, endpoint=endpoint, name=f"rpc:send:{peer_alias}"
    )
    recv_buf = GrowableBuffer(buf_size, pool=agent._memory_pool, name=MR_NAME)

    # Publish our recv buffer so the peer can target-address it.
    agent.register_memory_region(MR_NAME, recv_buf.ptr, 0, recv_buf.capacity)

    # Discover peer's recv buffer via Redis MR exchange.
    remote_mr = _wait_for_mr(agent, peer_alias, MR_NAME)
    remote_handler = endpoint.register_remote_memory_region(
        f"rpc:remote:{peer_alias}", remote_mr
    )

    ch = Channel(endpoint, send_buf, recv_buf, remote_handler)
    with _cache_lock:
        _channel_cache[key] = ch
    return ch


def _wait_for_mr(agent, peer_alias: str, mr_name: str, timeout: float = 30.0):
    """Poll Redis for a remote MR until it appears (or timeout)."""
    import time

    deadline = time.monotonic() + timeout
    while True:
        mr = agent.get_mr_info(peer_alias, mr_name)
        if mr is not None:
            return mr
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"Timeout waiting for MR '{mr_name}' from peer '{peer_alias}'. "
                f"Ensure the peer has created its proxy/serve."
            )
        time.sleep(0.05)


# ── Future ───────────────────────────────────────────


class RpcFuture:
    """Blocks on ``wait()`` until the remote side sends a reply."""

    def __init__(self, ch: Channel, method_info):
        self._ch = ch
        self._method = method_info

    def wait(self) -> Any:
        tag, ptr, nbytes, _keepalive = self._ch.recv_message()
        if self._method.raw:
            buf = (ctypes.c_char * nbytes).from_address(ptr)
            return bytes(buf)
        return self._method.deserialize_result(ptr, nbytes)


# ── Proxy ────────────────────────────────────────────


class _Proxy:
    """Dynamic proxy: attribute access → serialise → RDMA send → Future."""

    def __init__(self, ch: Channel, registry: MethodRegistry):
        object.__setattr__(self, "_ch", ch)
        object.__setattr__(self, "_registry", registry)

    def __getattr__(self, name: str):
        registry = object.__getattribute__(self, "_registry")
        ch = object.__getattribute__(self, "_ch")

        method_info = registry.by_name.get(name)
        if method_info is None:
            raise AttributeError(
                f"No RPC method '{name}'. " f"Available: {list(registry.by_name)}"
            )

        if method_info.raw:

            def caller(data: bytes):
                """Raw mode: *data* is the pre-serialized payload (e.g. flatbuffer bytes)."""
                n = len(data)
                chunk_size = getattr(ch, "_chunk_size", 4 * 1024 * 1024)
                if n > chunk_size:
                    ch.send_chunked(method_info.tag, data, chunk_size=chunk_size)
                else:
                    ch.ensure_send_capacity(n)
                    ctypes.memmove(ch.ptr, data, n)
                    ch.send(method_info.tag, n)
                return RpcFuture(ch, method_info)

        else:

            def caller(*args):
                n = method_info.serialize_args(args, ch.ptr, ch.size)
                ch.send(method_info.tag, n)
                return RpcFuture(ch, method_info)

        return caller


def proxy(agent, peer_alias: str, service_cls: type) -> _Proxy:
    """Create a transparent RPC proxy for a remote service.

    Usage::

        w = proxy(agent, "worker:0", WorkerService)
        future = w.add(1, 2)
        result = future.wait()      # blocks until reply arrives
    """
    ch = _get_or_create_channel(agent, peer_alias)
    registry = MethodRegistry(service_cls)
    return _Proxy(ch, registry)


def wait_all(futures: list[RpcFuture]) -> list[Any]:
    """Wait for all futures, return results in order."""
    return [f.wait() for f in futures]
