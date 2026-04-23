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


def method(fn=None, *, streaming=False, raw=False):
    """Mark a function as an RPC method.

    Args:
        streaming: If ``True``, the method receives chunked data via
                   ``Channel.recv_chunked()``.
        raw:       If ``True``, skip auto-serialization. The decorated
                   method is invoked with the service instance plus
                   ``(channel, ptr, nbytes)`` for the request payload.
                   In raw mode, the method is responsible for producing
                   the reply directly via the provided ``channel``
                   buffer instead of returning a value for automatic
                   serialization.
    """

    def decorator(f):
        setattr(f, _RPC_MARKER, {"streaming": streaming, "raw": raw})
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

    # -- Serialization helpers (used when raw=False) --

    def serialize_args(self, args: tuple, ptr: int, size: int) -> int:
        data = pickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL)
        if len(data) > size:
            raise ValueError(
                f"RPC '{self.name}' args ({len(data)} B) exceed "
                f"buffer capacity ({size} B)"
            )
        ctypes.memmove(ptr, data, len(data))
        return len(data)

    def deserialize_args(self, ptr: int, nbytes: int) -> tuple:
        buf = (ctypes.c_char * nbytes).from_address(ptr)
        return pickle.loads(bytes(buf))

    def serialize_result(self, result: Any, ptr: int, size: int) -> int:
        data = pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
        if len(data) > size:
            raise ValueError(
                f"RPC '{self.name}' result ({len(data)} B) exceed "
                f"buffer capacity ({size} B)"
            )
        ctypes.memmove(ptr, data, len(data))
        return len(data)

    def deserialize_result(self, ptr: int, nbytes: int) -> Any:
        buf = (ctypes.c_char * nbytes).from_address(ptr)
        return pickle.loads(bytes(buf))


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
            )
            self.by_tag[tag] = info
            self.by_name[name] = info
