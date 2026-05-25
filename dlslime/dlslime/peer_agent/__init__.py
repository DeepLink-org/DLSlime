"""PeerAgent: declarative control plane for DLSlime RDMA connections.

The package is split into three internal modules, all private:

- ``_obs``     — the DLSLIME_TIMING probe and `_tlog` helper.
- ``_mailbox`` — StreamMailbox + handshake protocol (qp_ready / connect_peer).
- ``_agent``   — PeerAgent class, factory, lifecycle, topology, MR, I/O.

Public surface is intentionally just two names: ``PeerAgent`` and
``start_peer_agent``. Import internals only from within the package.
"""

from ._agent import PeerAgent, start_peer_agent

__all__ = ["PeerAgent", "start_peer_agent"]
