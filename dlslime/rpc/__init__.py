"""SlimeRPC — typed RDMA RPC built on PeerAgent.

Public API::

    from dlslime.rpc import (
        method, serve, serve_once, proxy, wait_all,
        RpcError, RemoteRpcError, RpcTimeoutError, SoftRnrMonitor,
    )

    # Define
    class MyService:
        @method
        def add(self, a, b): return a + b

    # Worker side (default: serial dispatch, max_workers=1)
    serve(agent, MyService(), peer="engine:0")

    # Caller side
    w = proxy(agent, "worker:0", MyService)
    result = w.add(1, 2).wait(timeout=30.0)

Internals kept importable (for typing / advanced use) but NOT in
``__all__``:
    - ``Channel``, ``WIRE_VERSION`` -- users never construct a Channel
      directly; the C++ session reads it. Import from
      ``dlslime.rpc.channel`` if you need the type.
    - ``read_rnr_counter`` -- low-level sysfs reader; ``SoftRnrMonitor``
      is the normal interface. Import from ``dlslime.rpc.rnr`` if you
      need the one-shot helper.
"""

from .channel import Channel, WIRE_VERSION  # noqa: F401
from .errors import RemoteRpcError, RpcError, RpcTimeoutError
from .proxy import proxy, wait_all
from .registry import method
from .rnr import read_rnr_counter, SoftRnrMonitor  # noqa: F401
from .service import serve, serve_once

__all__ = [
    "RemoteRpcError",
    "RpcError",
    "RpcTimeoutError",
    "SoftRnrMonitor",
    "method",
    "proxy",
    "serve",
    "serve_once",
    "wait_all",
]
