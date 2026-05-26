# dlslime

The data-plane Python package of [DLSlime](https://github.com/DeepLink-org/DLSlime).

This wheel bundles:

- **PeerAgent** — a declarative coordination layer over RDMA / NVLink / TCP / Ascend Direct endpoints (`from dlslime import PeerAgent`).
- **NanoCtrlClient** — the HTTP client for the `dlslime-ctrl` control-plane server (`from dlslime.ctrl import NanoCtrlClient`).
- **DLSlimeCache** — a remote-memory cache service built on top of PeerAgent (`from dlslime import cache`).
- Native transports compiled from `dlslime/csrc/` via scikit-build-core + CMake.

The companion control-plane server lives in [`dlslime-ctrl/`](../dlslime-ctrl) and ships as a separate wheel.

See the [top-level README](../README.md) for architecture, design, and usage.
