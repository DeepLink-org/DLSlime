# DLSlime Documentation

DLSlime is a high-performance RDMA-based communication library for distributed deep learning.

## Directory Structure

```
docs/
├── guides/                      # User guides and tutorials
│   ├── quick-start.md          # Getting started with DLSlime v2
│   └── migration-v1-to-v2.md   # Migration guide from v1 to v2
│
├── design/                      # Architecture and design documents
│   └── reactor-design.md       # Reactor pattern design
│
├── implementation/              # Implementation details
│   ├── summary.md              # Implementation overview
│   └── shared-memory-pool.md   # Shared memory pool implementation
│
├── project/                     # Project status and planning
│   └── current-state.md        # Current project state
│
├── control_plane/               # Control plane documentation
│   ├── control_plane.md        # Control plane design
│   └── share_memory.md         # Shared memory management
│
├── benchmark-rpc.md            # SlimeRPC vs Ray RPC benchmark guide
│
├── huawei_ascend/               # Huawei Ascend integration
│   └── README.md               # Ascend platform guide
│
├── lazy_handshake.md           # Lazy handshake protocol
├── rdma_lazy_peer_api.md       # RDMA lazy peer API
└── roadmap.md                  # Project roadmap
```

## Quick Links

### Getting Started

- [Quick Start Guide](guides/quick-start.md) - Get up and running with DLSlime v2
- [Migration Guide](guides/migration-v1-to-v2.md) - Migrate from v1 to v2

### Design & Architecture

- [Reactor Design](design/reactor-design.md) - Event-driven reactor pattern architecture
- [Lazy Handshake](lazy_handshake.md) - Lazy handshake protocol design
- [RDMA Lazy Peer API](rdma_lazy_peer_api.md) - RDMA lazy peer API documentation

### Implementation

- [Implementation Summary](implementation/summary.md) - Overview of implementation
- [Shared Memory Pool](implementation/shared-memory-pool.md) - Memory pool implementation details

### Benchmarking

- [SlimeRPC Benchmark](benchmark-rpc.md) - Run and interpret the SlimeRPC vs Ray RPC benchmark

### Control Plane

- [Control Plane Design](control_plane/control_plane.md) - Control plane architecture
- [Shared Memory Management](control_plane/share_memory.md) - Memory management in control plane

### Platform Support

- [Huawei Ascend](huawei_ascend/README.md) - Integration with Huawei Ascend platform

### Project

- [Current State](project/current-state.md) - Project status and roadmap
- [Roadmap](roadmap.md) - Development roadmap

## Key Features

- **Zero-copy RDMA**: High-performance remote direct memory access
- **Event-driven**: Reactor pattern for scalable I/O
- **Shared memory pool**: Efficient memory management
- **Peer-to-peer mesh**: Automatic peer discovery and connection
- **Lazy handshake**: Deferred connection establishment for efficiency
- **Python bindings**: Easy integration with Python/PyTorch workloads
- **Multi-platform**: Support for standard RDMA and Huawei Ascend

## Contributing

When adding new documentation:

1. Place it in the appropriate subdirectory based on content type
2. Update this README with a link
3. Use clear, descriptive filenames (lowercase with hyphens)
4. Keep the main README.md in the root directory concise

## See Also

- Main README: [../README.md](../README.md)
- Chinese README: [../README_zh.md](../README_zh.md)
