# Deployment

This page collects the current deployment shape for a single-node DLSlime
control-plane setup. It is intentionally conservative: the cache service is
ephemeral unless a higher-level system adds persistence or replication policy.

## Components

| Component       | Role                                                              |
| --------------- | ----------------------------------------------------------------- |
| Redis           | Coordination backend used by NanoCtrl                             |
| NanoCtrl        | Service governance, discovery, and PeerAgent metadata             |
| DLSlime service | PeerAgent-backed service such as DLSlimeCache or SlimeRPC         |
| Client process  | Application process using Endpoint, PeerAgent, cache, or RPC APIs |

## Local Runbook

```bash
nanoctrl start

dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 \
  --port 8765 \
  --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765
```

## Health Checks

```bash
curl http://127.0.0.1:8765/healthz
curl http://127.0.0.1:8765/stats
curl http://127.0.0.1:8765/peer-agent
```

## Operational Notes

- Keep `scope` distinct across tests, tenants, and independent jobs.
- Prefer explicit host and port settings for repeatable deployment scripts.
- Treat DLSlimeCache V0 as an in-memory service; persistence and replication
  should be owned by the deployment layer until those features exist in the
  service itself.
