# API Reference

This page is the curated API entry point for the current Python surface. The
runtime also exposes C++/pybind symbols from `dlslime._slime_c`; those symbols
depend on the local build configuration and are documented through examples and
design pages until the generated C++ reference is added.

## Python Packages

| Package              | Purpose                                                                  |
| -------------------- | ------------------------------------------------------------------------ |
| `dlslime`            | Top-level package, logging helpers, PeerAgent exports, and C++ bindings. |
| `dlslime.peer_agent` | PeerAgent runtime facade and topology discovery helpers.                 |
| `dlslime.cache`      | DLSlimeCache client, service types, and CLI entry point.                 |
| `dlslime.rpc`        | SlimeRPC service, proxy, channel, registry, and buffer helpers.          |

## CLI

| Command         | Entry point              | Purpose                                            |
| --------------- | ------------------------ | -------------------------------------------------- |
| `dlslime-cache` | `dlslime.cache.cli:main` | Start, inspect, and stop the DLSlimeCache service. |

## Common Imports

```python
from dlslime import PeerAgent, start_peer_agent
from dlslime.cache import CacheClient
from dlslime.logging import get_logger, set_log_level
```

## Reference Pages To Expand

- [Endpoint API](guide/endpoint-api.md)
- [PeerAgent API](guide/peeragent-api.md)
- [DLSlimeCache Service](guide/dlslime-cache.md)
- [Versions](versions.md)
- `dlslime.cache.CacheClient`
- `dlslime.rpc.service`
- `dlslime.rpc.proxy`

The documentation site is configured with `mkdocstrings`; generated API pages
can be added once the public Python docstrings are stable enough to publish.
