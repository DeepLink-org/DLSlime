# DLSlimeCache Service

DLSlimeCache is a small service that owns a preallocated memory region, exposes
it through a composed PeerAgent, and records assignment manifests so clients can
read cached bytes back through the existing DLSlime endpoint path.

## Lifecycle

```bash
dlslime-cache start
dlslime-cache status
dlslime-cache stop
```

Data mode requires preallocated memory:

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G
```

## Client Flow

```python
from dlslime.cache import CacheClient

client = CacheClient(url="http://127.0.0.1:8765", peer_agent=agent)
server = client.connect_to_server()

stored = client.store(assignments)
queried = client.query(stored["peer_agent_id"], stored["version"])
deleted = client.delete(stored["peer_agent_id"], stored["version"])
```

See the full design notes in [DLSlimeCache](../design/dlslime-cache.md).
