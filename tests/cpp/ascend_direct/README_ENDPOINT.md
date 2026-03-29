# Ascend Direct Endpoint Performance Test

## Overview

`ascend_endpoint_perf.cpp` is the **new test file** that validates the refactored Ascend Direct Transfer Engine with the normalized endpoint interface aligned to DLSlime's RDMAEndpoint.

## Key Differences from `ascend_direct_perf.cpp`

| Aspect | Old (`ascend_direct_perf.cpp`) | New (`ascend_endpoint_perf.cpp`) |
|--------|-------------------------------|----------------------------------|
| **Class** | `AscendDirectContext` | `AscendDirectEndpoint` |
| **Memory Pools** | Single internal map | Separate `AscendLocalMemoryPool` + `AscendRemoteMemoryPool` |
| **Register Local** | `register_memory_region(device_id, addr, length)` | `register_memory_region(mr_key, addr, offset, length)` |
| **Register Remote** | ❌ Not supported | `register_remote_memory_region(mr_key, name, json)` |
| **Read Operation** | `read_batch(batch, host, port)` (batch + sync) | `read(assign_tuple_t, stream)` → `Future` (normalized + async) |
| **Connection** | Implicit in `read_batch()` | Explicit `connect(remote_info)` |
| **Endpoint Exchange** | Manual addr exchange via ZMQ | `endpoint_info()` JSON exchange |
| **Future Support** | ❌ Synchronous only | ✅ Returns `AscendFuture` with `wait()` |
| **Memory Region Keys** | Used `device_id` as key | Uses unique `mr_key` (1000+, 2000+ for remote) |

## New Features Validated

### 1. Separate Memory Pools
```cpp
// Internally creates:
// - AscendLocalMemoryPool (shared across endpoints)
// - AscendRemoteMemoryPool (per-endpoint, namespace isolated)
auto endpoint = std::make_shared<AscendDirectEndpoint>();
```

### 2. Normalized Memory Registration
```cpp
// Local memory (with mr_key)
uint64_t mr_key = 1000 + i;
endpoint->register_memory_region(mr_key, dev_addr, 0, size);

// Remote memory (metadata only)
uint64_t remote_mr_key = 2000 + i;
json remote_mr_info = {{"addr", remote_addr}, {"offset", 0}, {"length", size}};
endpoint->register_remote_memory_region(remote_mr_key, "name", remote_mr_info);
```

### 3. Explicit Connection
```cpp
// Exchange endpoint info via JSON
json local_info = endpoint->endpoint_info();
// ... send/receive via ZMQ ...
endpoint->connect(remote_info);
```

### 4. Future-based Async API
```cpp
// Returns future for async operations
std::vector<assign_tuple_t> assignments = {
    {local_mr_key, remote_mr_key, target_offset, source_offset, length}
};
auto future = endpoint->read(assignments, nullptr);
future->wait();  // Block until complete
```

## Usage

### Build

The test should be built with the updated CMakeLists.txt (Phase 6).

```bash
# Assuming build configuration is updated
mkdir -p build && cd build
cmake .. -DBUILD_TESTS=ON
make ascend_endpoint_perf
```

### Run

**Terminal 1 (Target - Device 2):**
```bash
./bin/ascend_endpoint_perf \
    --mode=target \
    --localhost="10.201.6.51" \
    --local_port=16777 \
    --remote_host="10.201.6.51" \
    --remote_port=16789 \
    --device_id=2
```

**Terminal 2 (Initiator - Device 0):**
```bash
./bin/ascend_endpoint_perf \
    --mode=initiator \
    --localhost="10.201.6.51" \
    --local_port=16789 \
    --remote_host="10.201.6.51" \
    --remote_port=16777 \
    --device_id=0
```

### Expected Output

**Initiator:**
```
=== Ascend Direct Endpoint Perf Test (Initiator) ===
Running on 10.201.6.51:16789
Exchanging endpoint info with 10.201.6.51:16777
Connected to remote endpoint

=== Starting Performance Test ===
Block iteration 0 completed: duration 1234us, block_size 32KB, total_size 1024KB, throughput 12.5 GB/s
Block iteration 1 completed: duration 2345us, block_size 64KB, total_size 2048KB, throughput 15.2 GB/s
...

=== Test Complete, Releasing Resources ===
```

## Test Parameters

Same as `ascend_direct_perf.cpp`:

- `--localhost`: Local host IP
- `--local_port`: Local ZMQ port
- `--remote_host`: Remote host IP
- `--remote_port`: Remote ZMQ port
- `--device_id`: NPU device ID
- `--batch_size`: Number of transfers per iteration (default: 32)
- `--block_size`: Starting block size in bytes (default: 32768)
- `--block_iteration`: Number of doubling iterations (default: 10)
- `--report_unit`: Throughput unit (default: "GB")
- `--report_precision`: Decimal precision (default: 2)

## Architecture Validation

This test validates the Phase 1-2 refactoring:

✅ **Phase 1**: DeviceFuture base class, AscendFuture implementation
✅ **Phase 2**: Separate AscendLocalMemoryPool and AscendRemoteMemoryPool
✅ **Interface Alignment**: Matches RDMAEndpoint signatures
✅ **Multi-peer Ready**: Remote pools isolated per endpoint

## Migration Guide

To migrate from old to new interface:

```cpp
// OLD
AscendDirectContext ctx;
ctx.init(host, port);
ctx.register_memory_region(device_id, addr, length);
ctx.read_batch(batch, remote_host, remote_port);

// NEW
AscendDirectEndpoint endpoint;
endpoint.init(host, port);
endpoint.register_memory_region(mr_key, addr, 0, length);
endpoint.register_remote_memory_region(remote_mr_key, name, remote_mr_info);
endpoint.connect(remote_info);
auto future = endpoint.read(assignments, nullptr);
future->wait();
```

## Next Steps

- **Phase 3**: Simplification review
- **Phase 4**: Python bindings for `AscendDirectEndpoint`
- **Phase 5**: Python test (`p2p_ascend_read.py`)
- **Phase 6**: CMakeLists.txt integration

## Notes

- The old `ascend_direct_perf.cpp` can be kept for backward compatibility testing
- Memory region keys use different ranges (1000+ local, 2000+ remote) to avoid collision
- Endpoint info exchange via JSON enables future extensibility (metadata, capabilities, etc.)
