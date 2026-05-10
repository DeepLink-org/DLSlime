# DLSlime

<div class="hero">
  <div>
    <h1>Composable communication runtime for AI services</h1>
    <p>
      DLSlime is a PeerAgent-centered communication and microservice toolkit for
      distributed AI systems. It lets applications adopt endpoint transfers,
      PeerAgent coordination, SlimeRPC, and DLSlimeCache one layer at a time.
    </p>
  </div>
</div>

## Start Here

<div class="feature-grid">
  <a href="installation/"><strong>Installation</strong><br>Install from PyPI or build optional transports from source.</a>
  <a href="quickstart/"><strong>Quickstart</strong><br>Run endpoint, PeerAgent, cache, and RPC examples.</a>
  <a href="guide/"><strong>Guide</strong><br>Use DLSlime components in service-shaped applications.</a>
  <a href="design/architecture/"><strong>Architecture</strong><br>Understand how PeerAgent, NanoCtrl, and endpoints fit together.</a>
</div>

## Core Layers

| Layer        | Use it when                                                                                        |
| ------------ | -------------------------------------------------------------------------------------------------- |
| Endpoint API | You already control peer placement and metadata exchange.                                          |
| PeerAgent    | You want connection setup, memory-region discovery, and stale-state cleanup handled by DLSlime.    |
| DLSlimeCache | Multiple PeerAgent clients need a shared RDMA-backed cache service.                                |
| SlimeRPC     | Application logic should call Python services while keeping transport coordination inside DLSlime. |

## Typical Service Flow

1. A service starts and registers itself with NanoCtrl as a generic entity.
2. Each service attaches to a PeerAgent instead of managing transport state directly.
3. PeerAgents register resource records and memory regions with NanoCtrl.
4. Clients discover services by `kind` and `scope`.
5. Endpoint objects issue the actual transfer through RDMA, NVLink, Ascend Direct, or the selected backend.
