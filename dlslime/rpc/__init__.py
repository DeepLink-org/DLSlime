"""SlimeRPC — typed RDMA RPC built on PeerAgent.

Public API (5 symbols)::

    from dlslime.rpc import method, serve, proxy, wait_all, Channel

    # Define
    class MyService:
        @method
        def add(self, a, b): return a + b

    # Worker side
    serve(agent, MyService(), peer="engine:0")

    # Caller side
    w = proxy(agent, "worker:0", MyService)
    result = w.add(1, 2).wait()
"""

from .channel import Channel
from .proxy import proxy, wait_all
from .registry import method
from .service import serve, serve_once

__all__ = ["method", "serve", "serve_once", "proxy", "wait_all", "Channel"]
