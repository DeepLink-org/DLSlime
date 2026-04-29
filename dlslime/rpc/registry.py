"""@method decorator and MethodRegistry.

The decorator marks functions as RPC-callable.  ``MethodRegistry``
collects them from a service class and assigns stable, deterministic
tags (sorted by method name).

Serialization uses ``pickle`` by default.  For hot-path methods you
can bypass this entirely and write directly into ``channel.ptr``
from C++ (e.g. NanoInfra's ``serialize``/``deserialize``).
"""

import ctypes
import inspect
import pickle
from dataclasses import dataclass
from typing import Any, Callable

_RPC_MARKER = "_slime_rpc"


# ── Decorator ────────────────────────────────────────


def method(fn=None, *, streaming=False, raw=False, parallel=False, inplace=False):
    """Mark a function as an RPC method.

    Args:
        streaming: If ``True``, the method receives chunked data via
                   ``Channel.recv_chunked()``. (Disabled in slotted-mailbox
                   protocol; reserved for future revisions.)
        raw:       If ``True``, skip auto-serialization. The decorated
                   method is invoked with the service instance plus
                   ``(channel, ptr, nbytes)`` for the request payload.
                   In raw mode, the method should return ``bytes`` /
                   ``memoryview`` for the reply (or ``None`` for an
                   empty ack).
        parallel:  If ``True``, this handler is reentrant and may run
                   concurrently with other ``parallel=True`` handlers
                   on the same service instance. The default (``False``)
                   forces serial execution under a per-service lock,
                   which is the only safe option for stateful objects
                   like PyTorch model runners or NCCL workers.
        inplace:   If ``True`` (requires ``raw=True``), the handler is
                   invoked as ``(self, req_ptr, req_nbytes, resp_ptr,
                   resp_cap) -> int`` and writes the reply directly into
                   the registered send buffer at ``resp_ptr``, returning
                   the number of bytes written. The C++ session posts
                   the WR without an intermediate ``bytes`` allocation
                   or memcpy out of Python heap -- the zero-copy reply
                   path. Note: while the handler runs, the session's
                   send-mutex is held, so concurrent senders on the
                   same session block until the handler returns. Use
                   only for handlers that finish in O(microseconds).
    """
    if inplace and not raw:
        raise ValueError("@method(inplace=True) requires raw=True")

    def decorator(f):
        setattr(
            f,
            _RPC_MARKER,
            {
                "streaming": streaming,
                "raw": raw,
                "parallel": parallel,
                "inplace": inplace,
            },
        )
        return f

    if fn is not None:  # @method without parens
        return decorator(fn)
    return decorator  # @method(streaming=True)


# ── MethodInfo ───────────────────────────────────────


@dataclass
class MethodInfo:
    tag: int
    name: str
    fn: Callable
    streaming: bool
    raw: bool
    parallel: bool = False
    inplace: bool = False

    # -- Serialization helpers (used when raw=False) --

    def serialize_args(self, args: tuple, ptr: int, size: int) -> int:
        data = self.serialize_args_bytes(args)
        if len(data) > size:
            raise ValueError(
                f"RPC '{self.name}' args ({len(data)} B) exceed "
                f"buffer capacity ({size} B)"
            )
        ctypes.memmove(ptr, data, len(data))
        return len(data)

    def serialize_args_bytes(self, args: tuple) -> bytes:
        return pickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL)

    def deserialize_args(self, ptr: int, nbytes: int) -> tuple:
        buf = (ctypes.c_char * nbytes).from_address(ptr)
        return pickle.loads(bytes(buf))

    def deserialize_args_bytes(self, data: bytes) -> tuple:
        return pickle.loads(data)

    def serialize_result(self, result: Any, ptr: int, size: int) -> int:
        data = self.serialize_result_bytes(result)
        if len(data) > size:
            raise ValueError(
                f"RPC '{self.name}' result ({len(data)} B) exceed "
                f"buffer capacity ({size} B)"
            )
        ctypes.memmove(ptr, data, len(data))
        return len(data)

    def serialize_result_bytes(self, result: Any) -> bytes:
        return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)

    def deserialize_result(self, ptr: int, nbytes: int) -> Any:
        buf = (ctypes.c_char * nbytes).from_address(ptr)
        return pickle.loads(bytes(buf))

    def deserialize_result_bytes(self, data: bytes) -> Any:
        return pickle.loads(data)


# ── Registry ─────────────────────────────────────────


class MethodRegistry:
    """Collect ``@method`` functions from a service class.

    Tags are assigned as consecutive integers in *sorted name order*,
    guaranteeing the same registry on both sides as long as both import
    the same class definition.
    """

    def __init__(self, service_cls: type):
        self.by_tag: dict[int, MethodInfo] = {}
        self.by_name: dict[str, MethodInfo] = {}

        members = sorted(
            (name, fn)
            for name, fn in inspect.getmembers(
                service_cls, predicate=inspect.isfunction
            )
            if hasattr(fn, _RPC_MARKER)
        )
        for tag, (name, fn) in enumerate(members):
            meta = getattr(fn, _RPC_MARKER)
            info = MethodInfo(
                tag=tag,
                name=name,
                fn=fn,
                streaming=meta["streaming"],
                raw=meta["raw"],
                parallel=meta.get("parallel", False),
                inplace=meta.get("inplace", False),
            )
            self.by_tag[tag] = info
            self.by_name[name] = info
