# NanoCtrl

NanoCtrl is the control plane for DLSlime microservice governance. It provides a
Redis-backed registry for discoverable services, heartbeat-based liveness,
scope-based isolation, PeerAgent coordination, RDMA memory-region lookup, and
declarative peer topology management.

NanoCtrl intentionally treats services generically. A cache service, RPC
worker, prefill service, decode service, router, or future DLSlime component all
register through the same entity lifecycle API and put service-specific fields
inside JSON metadata.

## Responsibilities

| Area                    | Responsibility                                                        |
| ----------------------- | --------------------------------------------------------------------- |
| Service registry        | Register, discover, heartbeat, and unregister generic entities        |
| Microservice governance | Track service kind, endpoint, metadata, resource hints, and TTL       |
| Scope isolation         | Partition Redis keys by scope so multiple jobs can share one NanoCtrl |
| PeerAgent control       | Register PeerAgents, publish resource records, and clean stale state  |
| RDMA metadata           | Store and query memory-region metadata for remote access              |
| Topology control        | Store desired PeerAgent topology and push connection intents          |
| Redis access            | Expose the Redis address that remote DLSlime clients should use       |

## Concepts

### Entity

An entity is a discoverable object managed by NanoCtrl. The default
`entity_type` is `service`.

```json
{
  "entity_type": "service",
  "entity_id": "cache:0",
  "kind": "cache",
  "endpoint": {
    "host": "127.0.0.1",
    "port": 8765,
    "protocol": "http"
  },
  "metadata": {
    "p2p_host": "127.0.0.1",
    "p2p_port": 8765
  }
}
```

NanoCtrl stores the entity as JSON and does not interpret the contents of
`endpoint`, `metadata`, or `resource` beyond preserving them for discovery.

### Kind

`kind` is the service category used for discovery and filtering. Examples:

- `cache`
- `rpc-worker`
- `prefill`
- `decode`
- `router`

### Scope

`scope` is an optional namespace. When present, all Redis keys are prefixed with
`{scope}:`. This lets multiple jobs, tests, or applications share one NanoCtrl
and Redis instance without colliding.

## Prerequisites

- Redis server
- Rust toolchain when building NanoCtrl from source
- Python 3.8+ when using the `nanoctrl` Python client package

## Build

```bash
cd NanoCtrl
cargo build --release

# Build a Python wheel that includes the Rust nanoctrl binary
maturin build --release
```

## Run

```bash
# Default: Redis at redis://127.0.0.1:6379
cargo run --release -- server

# Specify Redis explicitly
cargo run --release -- server --redis-url redis://your-redis-host:6379

# Or use the environment
export NANOCTRL_REDIS_URL=redis://your-redis-host:6379
cargo run --release -- server
```

The HTTP server listens on `http://0.0.0.0:3000` by default.

## Background Lifecycle

NanoCtrl includes a service-style CLI:

```bash
pip install -e NanoCtrl/

nanoctrl start
nanoctrl status
nanoctrl stop
```

Useful options:

```bash
nanoctrl start --redis-url redis://your-redis-host:6379 --log-file /tmp/nanoctrl.log
nanoctrl server --host 0.0.0.0 --port 3000
nanoctrl stop --force
```

Runtime metadata is stored under `$NANOCTRL_RUNTIME_DIR`, or
`$XDG_RUNTIME_DIR/nanoctrl`, or `/tmp/nanoctrl`.

## API

### Service Registry

| Endpoint                | Description                                         |
| ----------------------- | --------------------------------------------------- |
| `POST /register`        | Register or refresh a generic entity                |
| `POST /unregister`      | Remove a registered entity                          |
| `POST /heartbeat`       | Refresh an entity TTL                               |
| `POST /get_entity_info` | Fetch one entity by type and ID                     |
| `POST /list_entities`   | List entities, optionally filtered by type and kind |

### PeerAgent and RDMA

| Endpoint                              | Description                                       |
| ------------------------------------- | ------------------------------------------------- |
| `POST /start_peer_agent`              | Register a PeerAgent and return Redis information |
| `POST /query`                         | List PeerAgents visible in the scope              |
| `POST /cleanup`                       | Clean PeerAgent state and notify peers            |
| `POST /v1/desired_topology/:agent_id` | Set desired PeerAgent topology                    |
| `POST /register_mr`                   | Register a memory region                          |
| `POST /get_mr_info`                   | Query a remote memory region                      |

### Utility

| Endpoint                  | Description                                        |
| ------------------------- | -------------------------------------------------- |
| `POST /get_redis_address` | Return the Redis address remote clients should use |
| `GET /`                   | Health check                                       |

## Entity API Examples

### Register a Cache Service

```bash
curl http://127.0.0.1:3000/register \
  -H "Content-Type: application/json" \
  -d '{
    "entity_type": "service",
    "entity_id": "cache:0",
    "kind": "cache",
    "endpoint": {
      "host": "127.0.0.1",
      "port": 8765,
      "protocol": "http"
    },
    "metadata": {
      "p2p_host": "127.0.0.1",
      "p2p_port": 8765
    },
    "scope": "job-a"
  }'
```

### Register a ZMQ Service

```bash
curl http://127.0.0.1:3000/register \
  -H "Content-Type: application/json" \
  -d '{
    "entity_id": "prefill:0",
    "kind": "prefill",
    "endpoint": {
      "host": "10.0.0.2",
      "port": 6001,
      "protocol": "zmq"
    },
    "metadata": {
      "world_size": 8,
      "num_blocks": 15000,
      "peer_addrs": ["10.0.0.2:5000"]
    }
  }'
```

`entity_type` defaults to `service` when omitted.

### Heartbeat

```bash
curl http://127.0.0.1:3000/heartbeat \
  -H "Content-Type: application/json" \
  -d '{
    "entity_type": "service",
    "entity_id": "cache:0",
    "scope": "job-a"
  }'
```

### Discover Cache Services

```bash
curl http://127.0.0.1:3000/list_entities \
  -H "Content-Type: application/json" \
  -d '{
    "entity_type": "service",
    "kind": "cache",
    "scope": "job-a"
  }'
```

Response:

```json
{
  "status": "ok",
  "entities": [
    {
      "entity_type": "service",
      "entity_id": "cache:0",
      "id": "cache:0",
      "kind": "cache",
      "endpoint": {
        "host": "127.0.0.1",
        "port": 8765,
        "protocol": "http"
      },
      "metadata": {
        "p2p_host": "127.0.0.1",
        "p2p_port": 8765
      },
      "resource": null
    }
  ]
}
```

## Python Client

```python
from nanoctrl import NanoCtrlClient

client = NanoCtrlClient("127.0.0.1:3000", scope="job-a")
client.check_connection()

def register_cache() -> bool:
    return client.register(
        "cache:0",
        "cache",
        endpoint={"host": "127.0.0.1", "port": 8765, "protocol": "http"},
        metadata={"p2p_host": "127.0.0.1", "p2p_port": 8765},
    )

if register_cache():
    client.start_heartbeat(on_not_found=register_cache)

cache = client.get_entity_info("cache:0")
caches = client.list_entities(kind="cache")

client.stop()
```

### Client Methods

| Method                                                                                          | Description                                     |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `register(entity_id, kind, endpoint=None, metadata=None, resource=None, entity_type="service")` | Register a generic entity                       |
| `unregister()`                                                                                  | Unregister the entity registered by this client |
| `heartbeat()`                                                                                   | Refresh the entity TTL                          |
| `start_heartbeat(interval=15.0, on_not_found=None, name="nanoctrl-hb")`                         | Start background heartbeat                      |
| `stop_heartbeat(timeout=2.0)`                                                                   | Stop background heartbeat                       |
| `stop(timeout=2.0)`                                                                             | Stop heartbeat and unregister                   |
| `get_entity_info(entity_id, entity_type="service")`                                             | Query one entity                                |
| `list_entities(entity_type="service", kind=None)`                                               | List entities                                   |
| `get_redis_url()`                                                                               | Resolve Redis URL through NanoCtrl              |

PeerAgent helpers are also available on the same client:

- `register_peer(...)`
- `cleanup_peer(agent_name)`
- `heartbeat_peer(agent_name)`
- `set_desired_topology(...)`
- `query_peers()`

## DLSlimeCache Registration

`dlslime-cache start --ctrl ...` registers itself as a generic service:

```json
{
  "entity_type": "service",
  "entity_id": "cache:0",
  "kind": "cache",
  "endpoint": {
    "host": "<advertised-host>",
    "port": 8765,
    "protocol": "http"
  },
  "metadata": {
    "p2p_host": "<advertised-host>",
    "p2p_port": 8765
  }
}
```

Clients discover it with `list_entities(kind="cache")` or
`get_entity_info("cache:0")`.

## Redis Key Structure

With scope:

```text
{scope}:{entity_type}:{entity_id}
{scope}:nano_meta:{entity_type}_revision
{scope}:nano_events:{entity_type}_update
{scope}:agent:{agent_id}
{scope}:mr:{agent_id}:{mr_name}
{scope}:spec:topology:{agent_id}
{scope}:stream:{agent_id}
{scope}:inbox:{agent_id}
```

Without scope, the same keys are used without the prefix, for example
`service:cache:0`.

## Environment Variables

| Variable               | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| `NANOCTRL_REDIS_URL`   | Redis URL used by NanoCtrl                                   |
| `NANOCTRL_RUST_LOG`    | Rust log level, default `info`                               |
| `REDIS_PUBLIC_ADDRESS` | Optional public Redis `host:port` returned to remote clients |
| `NANOCTRL_SCOPE`       | Optional client-side scope used by DLSlime services          |

## Notes

- NanoCtrl is stateless; Redis is the source of truth.
- Registered entities expire if they stop heartbeating.
- Service-specific fields belong in `metadata`; NanoCtrl should not grow
  kind-specific schemas.
- Use `scope` for tests, multiple jobs, and multi-tenant deployments.
