# DLSlime Transfer Engine

A Peer to Peer RDMA Transfer Engine.

## Install

```bash
### pip install
pip install dlslime==0.0.1.post7

### Build from source
BUILD_NVLINK=<OFF|ON> BUILD_TORCH_PLUGIN=<OFF|ON> pip install -v --no-build-isolation -e .
```

## Usage

- NVLink IPC Read
  - [example](example/python/p2p_nvlink.py)
- RDMA RC Read
  - [example](example/python/p2p_rdma.py)
  - bench (single NIC): [cpp](bench/cpp/transfer_bench.cpp), [python](bench/python/transfer_bench.py)
  - bench (Aggregated Transport): [cpp](bench/cpp/scheduler_bench.cpp)
- RDMA SendRecv (GLOO Wrapper)
  - [example](example/python/sendrecv.py)
  - bench: [python](bench/python/sendrecv_bench.py)

## Cross node performance

### Benchmark

#### NVIDIA

- NVIDIA ConnectX-7 HHHL Adapter Card; 200GbE (default mode) / NDR200 IB; Dual-port QSFP112; PCIe 5.0 x16 with x16 PCIe extension option;
- RoCE v2.

![Throughput](docs/imgs/performance.png)

#### Interconnection between Chip A / Chip B / Chip C

- hardware configs

| Device |                       NIC Model | Bandwidth | PCIe Version | PCIe Lanes |
| :----- | ------------------------------: | --------: | -----------: | ---------: |
| A      | Mellanox ConnectX-7 Lx (MT4129) |  400 Gbps |     PCIe 5.0 |        x16 |
| B      | Mellanox ConnectX-7 Lx (MT4129) |  400 Gbps |     PCIe 5.0 |         x8 |
| C      | Mellanox ConnectX-7 Lx (MT4129) |  200 Gbps |     PCIe 5.0 |        x16 |
| D      | Mellanox ConnectX-7 Lx (MT4129) |  400 Gbps |     PCIe 5.0 |        x16 |

- experiments configs

  - Message Size = 128 MB
  - RDMA RC Read(single NIC)
  - Under affinity scenario
  - RDMA with GPU Direct

- Interconnect bandwidth matrix：(MB/s, demonstrates attainment of the theoretical bound).

| Throughput (MB/s) |        A |        B |        C |        D |
| :---------------- | -------: | -------: | -------: | -------: |
| A                 | 48967.45 | 28686.29 | 24524.29 | 27676.57 |
| B                 | 28915.72 | 28275.85 | 23472.29 | 27234.60 |
| C                 | 24496.14 | 24496.51 | 24513.57 | 24493.89 |
| D                 | 29317.66 | 28683.25 | 24515.30 | 27491.33 |

detailed results: [bench](bench/results)
