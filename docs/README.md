# DLSlime Documentation

This directory collects design notes, benchmark guides, platform docs, and the
project roadmap. Keep the root README concise; put deeper operational and
design details here or under `bench/`.

## Quick Links

### Benchmarks

- [Benchmark directory README](../bench/README.md) - Transfer, endpoint, cache, and RPC benchmark entry point
- [SlimeRPC Benchmark](benchmark-rpc.md) - Run and interpret the SlimeRPC vs Ray benchmark

### Design

- [DLSlimeCache](design/dlslime-cache.md) - RDMA cache-service assignment directory
- [PeerAgent Directed Connections](design/peer-agent-directed-connections.md) - Directed PeerAgent connection management
- [Control Plane vs Mooncake](design/control-plane-vs-mooncake.md) - Control-plane comparison notes
- [Endpoint Ownership Model](endpoint-ownership-model.md) - Endpoint memory and completion ownership
- [Endpoint DeviceSignal Refactor](endpoint-device-signal-refactor.md) - Slot-owned completion hazards in RDMAEndpoint

### Platform Support

- [Huawei Ascend](huawei_ascend/README.md) - Ascend Direct integration guide

### Project

- [Roadmap](roadmap.md) - Development roadmap

## Directory Structure

```text
docs/
├── README.md
├── benchmark-rpc.md
├── endpoint-device-signal-refactor.md
├── endpoint-ownership-model.md
├── roadmap.md
├── design/
│   ├── control-plane-vs-mooncake.md
│   ├── dlslime-cache.md
│   └── peer-agent-directed-connections.md
├── huawei_ascend/
│   └── README.md
└── imgs/
    ├── interface.svg
    ├── performance.png
    └── assets/
```

## Contributing

When adding documentation:

1. Put benchmark runbooks under `bench/` unless they are a focused deep dive.
2. Put architecture and design notes under `docs/design/`.
3. Update this index and any relevant README.
4. Keep generated logs, CSVs, and profiler output out of `docs/`.

## See Also

- Main README: [../README.md](../README.md)
- Chinese README: [../README_zh.md](../README_zh.md)
