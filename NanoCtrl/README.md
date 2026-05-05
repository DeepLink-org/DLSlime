# NanoCtrl Control Plane

Control plane server for NanoInfra distributed LLM inference. NanoCtrl is stateless and supports multiple scopes sharing the same instance for service discovery and engine management.

## Features

- **Stateless Design**: Supports multiple scopes (sessions) sharing the same instance
- **Engine Management**: Register, heartbeat, and discover prefill/decode engines
- **RDMA Connection Management**: Manage peer agents and RDMA connections for KV cache migration
- **Redis-backed**: All state stored in Redis for scalability

## Prerequisites

- Redis server running
- Rust toolchain (only needed when building from source)

## Building

```bash
cd NanoCtrl
cargo build --release

# Build a Python wheel that includes the Rust nanoctrl binary
maturin build --release
```

## Configuration

NanoCtrl supports CLI arguments and environment variables:

- `NANOCTRL_REDIS_URL` - Redis connection URL
- `NANOCTRL_RUST_LOG` - Log level (default: `info`)

## Running

```bash
# Default: Redis at 127.0.0.1:6379
cargo run --release -- server

# Or specify a Redis URL
cargo run --release -- server --redis-url redis://your-redis-host:6379

# Or override via environment variables
export NANOCTRL_REDIS_URL=redis://your-redis-host:6379
cargo run --release -- server
```

The server will listen on `http://0.0.0.0:3000` by default.

### Background service style (`nanoctrl start/status/stop`)

NanoCtrl provides a Rust CLI similar to Ray's service lifecycle commands.

```bash
# Install NanoCtrl package and bundled nanoctrl binary
pip install -e NanoCtrl/

# Start in background
nanoctrl start

# Custom Redis / log
nanoctrl start --redis-url redis://your-redis-host:6379 --log-file /tmp/nanoctrl.log

# Run server in the foreground
nanoctrl server --host 0.0.0.0 --port 3000

# Check status (PID + health check)
nanoctrl status

# Stop gracefully
nanoctrl stop

# Force stop if needed
nanoctrl stop --force
```

Notes:

- `nanoctrl start` launches the same Rust binary in the background and records runtime metadata.
- Runtime metadata is stored under `$NANOCTRL_RUNTIME_DIR` (or `$XDG_RUNTIME_DIR/nanoctrl`, default `/tmp/nanoctrl`).
- `nanoctrl status` uses runtime metadata address by default; can be overridden with `--address`.

**Distributed deployment**: When engines run on remote nodes, they need to connect to Redis. If Redis runs on the master node, set:

```bash
export NANOCTRL_REDIS_URL=redis://127.0.0.1:6379   # NanoCtrl connects to local Redis
```

## Scope Support

NanoCtrl is stateless and supports multiple scopes (sessions) sharing the same instance. Scope is determined by clients via `NANOCTRL_SCOPE` environment variable or passed in API requests.

- **Scope isolation**: Each scope has its own Redis key namespace (`{scope}:*`)
- **Multi-tenancy**: Multiple sessions can coexist without interference
- **Client-side scope**: Clients (NanoRoute, EngineServer, peer_agent) set scope via `NANOCTRL_SCOPE` env var

## API Endpoints

### Engine Management

- `POST /register_engine` - Register a new engine (prefill/decode/hybrid)
- `POST /unregister_engine` - Unregister an engine
- `POST /heartbeat_engine` - Refresh engine TTL (heartbeat)
- `POST /get_engine_info` - Get engine information by ID
- `POST /list_engines` - List all registered engines (optionally filtered by scope)

### RDMA Connection Management

- `POST /start_peer_agent` - Register a peer agent for RDMA connections
- `POST /query` - Query all registered peer agents
- `POST /v1/desired_topology/:agent_id` - Set desired topology for declarative connection management
- `POST /register_mr` - Register a memory region
- `POST /get_mr_info` - Get remote memory region info
- `POST /cleanup` - Cleanup agent resources

### Utility

- `POST /get_redis_address` - Get Redis address (resolves localhost to public IP for remote clients)
- `GET /` - Health check endpoint

## API Details

### Register Engine

```bash
POST /register_engine
Content-Type: application/json

{
  "engine_id": "prefill-0",
  "role": "prefill",  # "prefill", "decode", or "hybrid"
  "world_size": 8,
  "num_blocks": 15000,
  "host": "127.0.0.1",
  "port": 6001,
  "peer_addrs": ["<prefill-engine-ip>:5000"],
  "p2p_host": "127.0.0.1",  # optional
  "p2p_port": 5000,         # optional
  "scope": "my-session"      # optional, for multi-tenant isolation
}
```

### List Engines

```bash
POST /list_engines
Content-Type: application/json

{
  "scope": "my-session"  # optional, filter by scope
}
```

Response:

```json
{
  "status": "ok",
  "engines": [
    {
      "id": "prefill-0",
      "role": "prefill",
      "world_size": 8,
      "num_blocks": 15000,
      "host": "127.0.0.1",
      "port": 6001,
      "zmq_address": "tcp://127.0.0.1:6001"
    }
  ]
}
```

### Heartbeat Engine

```bash
POST /heartbeat_engine
Content-Type: application/json

{
  "engine_id": "prefill-0",
  "scope": "my-session"  # optional
}
```

## Environment Variables

- `NANOCTRL_RUST_LOG` - Log level (default: `info`)
- `NANOCTRL_REDIS_URL` - Redis connection URL (default: `redis://127.0.0.1:6379`)
- `REDIS_PUBLIC_ADDRESS` - For distributed setup: IP:port that remote workers use to reach Redis

## Python Client (`nanoctrl`)

NanoCtrl ships a lightweight Python client package for engine lifecycle management.

### Installation

```bash
pip install -e NanoCtrl/
# or via the root meta-package
pip install ".[nanoctrl]"
```

### `NanoCtrlClient`

```python
from nanoctrl import NanoCtrlClient

client = NanoCtrlClient(
    address="<nanoctrl-ip>:3000",   # host:port or http://host:port
    scope="my-session",            # optional, for multi-tenant isolation
)

# Register an engine
client.register(engine_id="prefill-0", extra={
    "role": "prefill",
    "world_size": 8,
    "num_blocks": 15000,
    "host": "10.0.0.2",
    "port": 6001,
})

# Start background heartbeat (daemon thread, 15s interval)
client.start_heartbeat(
    interval=15.0,
    on_not_found=lambda: client.register(engine_id="prefill-0", extra={...}),
)

# Query engine info
info = client.get_engine_info("prefill-0")

# Get Redis URL (for distributed setup)
redis_url = client.get_redis_url()

# Cleanup: stop heartbeat + unregister
client.stop()
```

### API Reference

| Method                                          | Description                                                                                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `register(engine_id, extra)`                    | POST `/register_engine` — register with NanoCtrl                                                                                      |
| `unregister()`                                  | POST `/unregister_engine` — remove engine registration                                                                                |
| `heartbeat()`                                   | POST `/heartbeat_engine` — returns `"ok"`, `"not_found"`, or `"error"`                                                                |
| `get_redis_url()`                               | POST `/get_redis_address` — returns `redis://host:port`                                                                               |
| `get_engine_info(engine_id)`                    | POST `/get_engine_info` — returns engine info dict                                                                                    |
| `start_heartbeat(interval, on_not_found, name)` | Start background heartbeat thread; calls `on_not_found` callback when NanoCtrl responds `not_found` (useful for auto re-registration) |
| `stop_heartbeat(timeout)`                       | Stop the heartbeat thread                                                                                                             |
| `stop(timeout)`                                 | Stop heartbeat + unregister (safe to call multiple times)                                                                             |

### RDMA Peer Agent Client

For RDMA connection management, see `DLSlime/dlslime/peer_agent.py` which uses the `/start_peer_agent`, `/query`, and `/register_mr` endpoints.

## Redis Key Structure

NanoCtrl uses the following Redis key patterns (with optional scope prefix):

- `{scope}:agent:*` - Peer agent registration info
- `{scope}:stream:*` - Agent stream mailboxes (Redis Streams)
- `{scope}:exchange:*` - QP info exchange (sender:receiver)
- `{scope}:spec:topology:*` - Desired topology specifications
- `{scope}:inbox:*` - Legacy inbox for cleanup events
- `{scope}:mr:*` - Memory Region (MR) information
- `{scope}:engine:*` - Engine registration info
- `{scope}:nano_meta:engine_revision` - Engine revision counter
- `{scope}:nano_events:engine_update` - Engine update pub/sub channel

If no scope is provided, keys are used without prefix (e.g., `engine:*` instead of `{scope}:engine:*`).
