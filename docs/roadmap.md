## Overview

- Links

DLSlime is dedicated to supporting efficient transmission over a variety of different links, including but not limited to IBVerbs, CUDA IPC, TCP Socket, PCIE, NVShmem, Ascend (Direct) ...

- Transfer Engine

DLSlime provides a flexible and efficient P2P Transfer Engine, enabling AI-workload-aware customized functions such as Prefill-Decode separation and checkpoint transmission.

- Collective Ops

Referring to [DeepEP](https://github.com/deeplink-org/DeepEP.git), DLSlime provides a buffer-based collective communication library that achieves ultra-low latency collective communications.

- Torch Wrapper

To meet the heterogeneous requirements of SPMD programs such as heterogeneous pipeline parallel training, a Torch communication backend is provided.

## Transfer Engine Roadmap

- IBVerbs Transfer Engine
  - ✅ SendRecv Endpoint
  - ✅ RDMA Read/Write Endpoint
- NVShmem
  - ✅ NVShmem Context and Send/Recv Kernel
  - ⚡ support NVShmem put and get wrapper
- TCP Socket
  - ✅ zmq bootstrap
  - ⏳ TCP Socket transfer engine
- CUDA IPC
  - ✅ support CUDAIPC Read/Write Endpoint
- PCIE
  - ⏳ High performance Shared Memory transfer engine
  - ⏳ High performance data offloading
- Ascend
  - ✅ Ascned direct transfer engine
- UB Mesh
  - 💭 Planning

## Collective Ops

- IBVerbs
  - ✅ Send/Recv
  - ⚡ M2N for attention-FFN disaggregation
  - ⏳ AllGather
  - ⏳ AllReduce
  - ⏳ All2All
- NVShmem
  - ⏳ Send/Recv
  - 🚧 AllGather
- CUDA IPC
  - ✅ AllGather
  - ✅ High performance AllGather using CUDA Multi-Mem

## Torch Wrapper

- IBVerbs
  - ✅ Send/Recv
  - ⏳ AllGather
  - ⏳ AllReduce
  - ⏳ All2All
