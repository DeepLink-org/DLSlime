"""SlimeRPC — typed RDMA RPC built on PeerAgent.

Public API::

    from dlslime.rpc import (
        method, serve, serve_once, proxy, wait_all, Channel,
        WIRE_VERSION, RpcError, RemoteRpcError, RpcTimeoutError,
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
"""

from .channel import Channel, WIRE_VERSION
from .errors import RemoteRpcError, RpcError, RpcTimeoutError
from .proxy import proxy, wait_all
from .registry import method
from .rnr import read_rnr_counter, SoftRnrMonitor
from .service import serve, serve_once

__all__ = [
    "Channel",
    "RemoteRpcError",
    "RpcError",
    "RpcTimeoutError",
    "SoftRnrMonitor",
    "WIRE_VERSION",
    "method",
    "proxy",
    "read_rnr_counter",
    "serve",
    "serve_once",
    "wait_all",
]
