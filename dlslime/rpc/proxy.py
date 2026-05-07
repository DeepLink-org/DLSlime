"""proxy() — caller-side transparent handle + channel factory.

The runtime is the C++ ``RpcSession`` exposed by ``_slime_c.rpc``. There is
no Python-side runtime any more: build with ``BUILD_RPC=ON`` to use this
module.
"""

import os
import pickle
import threading
import time
from typing import Any

from dlslime import _slime_c
from dlslime.logging import get_logger
from .buffer import GrowableBuffer
from .channel import Channel
from .errors import RemoteRpcError, RpcTimeoutError
from .registry import MethodRegistry

logger = get_logger("rpc")

MR_PREFIX = "rpc:mailbox"

# C++ backend availability is decided at import time. Build flag is
# propagated from CMake via _BUILD_RPC, but we also defensively check
# that the submodule actually exists in case of partial builds.
_CPP_AVAILABLE = bool(getattr(_slime_c, "_BUILD_RPC", False)) and hasattr(
    _slime_c, "rpc"
)


def _require_cpp() -> None:
    if not _CPP_AVAILABLE:
        raise RuntimeError(
            "SlimeRPC requires the C++ session, but _slime_c was built "
            "without BUILD_RPC. Rebuild with BUILD_RPC=ON."
        )


# Channel/runtime caches keyed by (agent.alias, peer_alias). Using alias
# instead of id(agent) avoids stale collisions when an agent is GC'd and
# its address is reused by a fresh PeerAgent.
_channel_cache: dict[tuple[str, str], Channel] = {}
_cache_lock = threading.Lock()
_runtime_cache: dict[int, "_CppRuntime"] = {}
_runtime_lock = threading.Lock()


def _channel_cache_key(agent, peer_alias: str) -> tuple[str, str]:
    alias = getattr(agent, "alias", None)
    if not alias:
        raise RuntimeError("RPC channel creation requires agent.alias to be set")
    return (alias, peer_alias)


def _get_or_create_channel(agent, peer_alias: str) -> Channel:
    """Return (or lazily create) the Channel to *peer_alias*.

    Channel creation:
      1. Get the RDMA endpoint that PeerAgent already set up.
      2. Allocate a send buffer (per-peer) and a slotted recv buffer.
      3. Publish our recv buffer as MR ``rpc:mailbox:<local>:<peer>``.
      4. Discover the peer's ``rpc:mailbox:<peer>:<local>`` MR.
    """
    key = _channel_cache_key(agent, peer_alias)
    with _cache_lock:
        if key in _channel_cache:
            return _channel_cache[key]

    peer_connections = agent.get_connections().get(peer_alias, {})
    if len(peer_connections) != 1:
        raise ValueError(
            f"RPC channel for peer {peer_alias!r} requires exactly 1 connection, "
            f"got {len(peer_connections)}. Established connections: "
            f"{list(peer_connections.keys())}"
        )
    connection = next(iter(peer_connections.values()))
    endpoint = connection.endpoint
    if endpoint is None or not connection.is_connected():
        raise ValueError(
            f"RPC channel for peer {peer_alias!r} requires a connected endpoint. "
            f"Connection {connection.conn_id} is {connection.state!r}."
        )
    buf_size = int(getattr(agent, "_rpc_buffer_size", 32_000_000))
    slot_count = int(getattr(agent, "_rpc_max_inflight", 0) or 0)
    if slot_count <= 0:
        slot_count = int(os.environ.get("SLIME_RPC_MAX_INFLIGHT", "16"))
    if slot_count < 1:
        slot_count = 1

    local_alias, _ = key
    local_mr_name = _mailbox_mr_name(local_alias, peer_alias)
    remote_mr_name = _mailbox_mr_name(peer_alias, local_alias)

    send_buf = GrowableBuffer(
        buf_size, endpoint=endpoint, name=f"rpc:send:{peer_alias}"
    )
    recv_buf = GrowableBuffer(
        buf_size * slot_count, pool=endpoint.get_pool(), name=local_mr_name
    )

    recv_buf.handler = agent.register_memory_region(
        local_mr_name, recv_buf.ptr, 0, recv_buf.capacity
    )

    remote_mr = _wait_for_mr(agent, peer_alias, remote_mr_name)
    remote_handler = endpoint.register_remote_memory_region(
        f"rpc:remote:{peer_alias}", remote_mr
    )

    ch = Channel(
        endpoint,
        send_buf,
        recv_buf,
        remote_handler,
        slot_size=buf_size,
        slot_count=slot_count,
    )
    with _cache_lock:
        _channel_cache[key] = ch
    return ch


def _mailbox_mr_name(local_alias: str, peer_alias: str) -> str:
    return f"{MR_PREFIX}:{local_alias}:{peer_alias}"


def _wait_for_mr(agent, peer_alias: str, mr_name: str, timeout: float = 30.0):
    """Poll Redis for a remote MR until it appears (or timeout)."""
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


class _CppFuture:
    """Adapter around a C++ ``rpc.ClientFuture`` exposing the Python API."""

    __slots__ = ("_method", "_native", "_runtime")

    def __init__(self, runtime: "_CppRuntime", method_info, native):
        self._method = method_info
        self._native = native
        self._runtime = runtime

    def done(self) -> bool:
        return self._native.done()

    def wait(self, timeout: float | None = None) -> Any:
        # ``wait()`` releases the GIL inside C++; the timed path uses
        # wait_for() which returns None on timeout (vs the bytes payload
        # on success). We translate that to the Python-side
        # ``RpcTimeoutError`` to match the legacy runtimes.
        if timeout is None:
            data = self._native.wait()
        else:
            data = self._native.wait_for(float(timeout))
            if data is None:
                # Detach so the slot is reclaimed when (if) the reply
                # eventually arrives.
                self._runtime._detach(self._native.request_id)
                raise RpcTimeoutError(
                    f"RPC '{self._method.name}' timed out after {timeout}s"
                )
        # ``data`` is already a ``bytes`` object handed up from the C++
        # session. ``bytes(data)`` would silently re-copy the buffer,
        # which costs another N bytes per RPC at multi-MB payloads.
        if self._native.is_remote_exception:
            raise _decode_remote_exception(data)
        if self._method.raw:
            return data
        return self._method.deserialize_result_bytes(data)


class _CppRuntime:
    """Thin wrapper around the C++ ``RpcSession``.

    Two modes, picked at construction time:

      * **Single-flight (no pump)** — ``slot_count == 1`` and the session
        is *only* used as a client (``for_server=False``). ``call()``
        drives recv inline on the caller thread, so there is no
        cross-thread cv-wakeup nor GIL handoff per RPC.

      * **Pump-backed** — ``slot_count > 1`` *or* the session must
        accept inbound requests (``for_server=True``). The C++ session
        spins one OS thread that demultiplexes replies into per-future
        condition variables and pushes inbound requests into an inbox.
    """

    def __init__(self, ch: Channel, for_server: bool = False):
        _require_cpp()
        # Sanity: the channel buffers MUST have been registered already.
        if ch._send.handler < 0 or ch._recv.handler < 0 or ch._remote_handler < 0:
            raise RuntimeError(
                "Cannot create C++ RpcSession: channel buffers not yet "
                "registered (handler < 0)."
            )
        self._ch = ch
        # Pump is required when:
        #   * we may receive inbound requests (server side), or
        #   * slot_count > 1, where the recv pump is the only thing
        #     that can demux interleaved replies into the right
        #     ClientFuture.
        # Otherwise (slot_count == 1 client-only) we run the no-pump
        # single-flight path: recv is driven inline by call().
        start_pump = bool(for_server) or ch._slot_count > 1
        self._has_pump = start_pump
        # The Python Channel still holds the GrowableBuffers alive; we
        # only borrow their pointers/handlers for the C++ session.
        self._session = _slime_c.rpc.RpcSession(
            endpoint=ch._ep,
            local_send_handler=ch._send.handler,
            local_send_ptr=ch._send.ptr,
            local_recv_handler=ch._recv.handler,
            local_recv_ptr=ch._recv.ptr,
            remote_recv_handler=ch._remote_handler,
            slot_size=ch._slot_size,
            slot_count=ch._slot_count,
            start_recv_pump=start_pump,
        )

    def call(self, method_info, payload: bytes) -> _CppFuture:
        if len(payload) > self._ch.size:
            raise RuntimeError(
                "RPC payload exceeds the per-inflight slot capacity. "
                f"Payload is {len(payload)} B, slot payload limit is "
                f"{self._ch.size} B."
            )
        # bytes objects implement the buffer protocol; the C++ side does
        # the memcpy into the registered send buffer.
        native = self._session.call(method_info.tag, payload)
        return _CppFuture(self, method_info, native)

    def call_inplace(self, method_info, writer) -> _CppFuture:
        """Zero-copy send: ``writer(send_ptr, send_cap) -> nbytes`` fills
        the registered send buffer directly. No intermediate ``bytes``.

        Only valid for methods declared with ``@method(raw=True,
        inplace=True)``; the matching server handler also runs in
        inplace mode.
        """
        native = self._session.call_inplace(method_info.tag, writer)
        return _CppFuture(self, method_info, native)

    def _detach(self, request_id: int) -> None:
        self._session.detach_request(request_id)

    @property
    def session(self):
        """Expose the underlying C++ session (used by service.py)."""
        return self._session

    def close(self) -> None:
        self._session.close()


def _decode_remote_exception(data: bytes) -> RemoteRpcError:
    try:
        info = pickle.loads(data)
    except BaseException as exc:
        return RemoteRpcError(f"<failed to decode remote exception: {exc!r}>")
    if isinstance(info, dict):
        return RemoteRpcError(
            info.get("message", "<no message>"),
            type_name=info.get("type"),
            traceback=info.get("traceback"),
        )
    return RemoteRpcError(str(info))


def _get_or_create_runtime(ch: Channel, for_server: bool = False) -> _CppRuntime:
    """Return (or lazily create) the C++ runtime bound to *ch*.

    *for_server* must be True when the caller intends to use the runtime
    to *receive* inbound requests (``service.serve``). It is safe to pass
    False (the default) for client-only call sites.

    A previously-cached client-only runtime is upgraded to a pump-enabled
    one if a server later asks for it; the wrapper used for client calls
    is recreated to point at the new session.
    """
    _require_cpp()
    key = id(ch)
    with _runtime_lock:
        runtime = _runtime_cache.get(key)
        if runtime is not None and for_server and not runtime._has_pump:
            # Upgrade a cached no-pump runtime if a server now wants to
            # use this same channel id. In practice this rarely fires:
            # client and server normally use distinct channels
            # (driver→worker vs worker→driver).
            logger.warning(
                "SlimeRPC: upgrading single-flight runtime to pump-backed "
                "mode because a server is now attaching to channel id=%d",
                key,
            )
            try:
                runtime.close()
            except Exception:
                pass
            runtime = None

        if runtime is None:
            runtime = _CppRuntime(ch, for_server=for_server)
            logger.info(
                "SlimeRPC: created C++ runtime (slot_count=%d, pump=%s)",
                ch.slot_count,
                runtime._has_pump,
            )
            _runtime_cache[key] = runtime
        return runtime


# ── Proxy ────────────────────────────────────────────


class _Proxy:
    """Dynamic proxy: attribute access → serialise → RDMA send → Future."""

    def __init__(self, ch: Channel, registry: MethodRegistry):
        object.__setattr__(self, "_ch", ch)
        object.__setattr__(self, "_runtime", _get_or_create_runtime(ch))
        object.__setattr__(self, "_registry", registry)

    def __getattr__(self, name: str):
        registry = object.__getattribute__(self, "_registry")
        runtime = object.__getattribute__(self, "_runtime")

        method_info = registry.by_name.get(name)
        if method_info is None:
            raise AttributeError(
                f"No RPC method '{name}'. " f"Available: {list(registry.by_name)}"
            )

        if method_info.raw:
            if method_info.inplace:

                def caller(writer):
                    """Raw inplace mode: *writer* fills the send buffer.

                    Signature: ``writer(send_ptr: int, send_cap: int) -> int``
                    where the return value is the number of bytes written.
                    """
                    return runtime.call_inplace(method_info, writer)

            else:

                def caller(data: bytes):
                    """Raw mode: *data* is the pre-serialized payload."""
                    return runtime.call(method_info, bytes(data))

        else:

            def caller(*args):
                return runtime.call(method_info, method_info.serialize_args_bytes(args))

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


def wait_all(futures, timeout: float | None = None) -> list[Any]:
    """Wait for all futures, return results in order."""
    return [f.wait(timeout) for f in futures]
