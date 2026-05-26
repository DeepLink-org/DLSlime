"""dlslime.ctrl — HTTP client for the dlslime-ctrl control plane.

The Rust ``dlslime-ctrl`` server provides a Redis-backed entity registry and
peer-agent coordination layer. This module is the Python client side and is
used by:

- ``dlslime.peer_agent.PeerAgent`` for peer registration, topology, and
  cleanup;
- ``dlslime.cache`` for service discovery and lifecycle;
- external services (e.g. NanoDeploy engines) that register themselves as
  generic entities.
"""

from __future__ import annotations

__all__ = ["NanoCtrlClient"]


def __getattr__(name: str):
    if name == "NanoCtrlClient":
        from .client import NanoCtrlClient

        return NanoCtrlClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
