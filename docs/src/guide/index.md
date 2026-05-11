# Guide

The guide section explains how to use DLSlime as service infrastructure rather
than a one-off transfer library.

## Topics

- [Deployment](deployment.md): run NanoCtrl, Redis, DLSlimeCache, and examples in a predictable layout.
- [Endpoint API](endpoint-api.md): use the low-level RDMA endpoint surface directly.
- [PeerAgent API](peeragent-api.md): use control-plane discovery, named memory regions, and service-friendly I/O.
- [SlimeRPC](slimerpc.md): define Python services and call them through PeerAgent-backed RDMA RPC.
- [DLSlimeCache Service](dlslime-cache.md): service lifecycle and client flow.
- [SlimeRPC Benchmark](benchmark-rpc.md): benchmark SlimeRPC against Ray.
