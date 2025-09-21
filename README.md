<div align="center">
<p align="center"> <img src="docs/imgs/assets/logo.svg" alt="" width="300"> </p>
</div>

<h1 align="center"> Flexible & Efficient Heterogeneous Transfer Toolkit </h1>
<p align="center">
  <a href="https://join.slack.com/t/dlslime/shared_invite/zt-3e9zvercw-a89KI_Ig8N1UTaol_q6MXg"><img src="docs/imgs/assets/slack.svg" width="16" height="16" style="vertical-align: middle;"> Slack </a> |
  <a href="docs/imgs/assets/wechat_qrcode.jpg"><img src="docs/imgs/assets/wechat.svg" width="16" height="16" style="vertical-align: middle;"> WeChat Group </a>
</p>

## Getting Started

DLSlime offers a set of peer-to-peer communication interfaces. For instance, consider the task of batched slice assignment from a remote tensor to a local tensor. You can accomplish this using the following APIs.

![Assignment Operation](docs/imgs/interface.svg).

Here are some examples of DLSlime interface.

### RDMA RC Mode

- RDMA RC Read (Sync / Async mode)

```
python example/python/p2p_rdma_rc_read.py
```

- RDMA RC Read (Coroutine mode)

```
python example/python/p2p_rdma_rc_read_coroutine.py
```

- RDMA RC Write (Sync / Async mode)

```
python example/python/p2p_rdma_rc_write.py
```

- RDMA RC Write with immediate data (Sync / Async mode)

```
python example/python/p2p_rdma_rc_write_with_imm_data.py
```

- RDMA RC Send/Recv

```
python example/python/p2p_rdma_rc_send_recv.py
```

```
python example/python/p2p_rdma_rc_send_recv_gdr.py
```

- DLSlime torch backend

```
python example/python/p2p_rdma_rc_send_recv_torch.py --rank 0
python example/python/p2p_rdma_rc_send_recv_torch.py --rank 1
```

### NVLink Mode

```
# initiator
python example/python/p2p_nvlink.py --initiator-url "127.0.0.1:6006" --target-url "127.0.0.1:6007" --role initiator
```

```
# target
python example/python/p2p_nvlink.py --initiator-url "127.0.0.1:6006" --target-url "127.0.0.1:6007" --role target
```

### NVShmem Mode

```
# send
python example/python/p2p_nvshmem_ibgda_sendrecv.py --rank 0 --world-size 2
```

```
# recv
python example/python/p2p_nvshmem_ibgda_sendrecv.py --rank 1 --world-size 2
```

> \[!Caution\]
> DLSlime NVShmem transfer engine is in the experimental stage.

## Install

### pip install

```
pip install dlslime==0.0.1.post7
```

> \[!Note\]
> The DLSlime pip version is built with default FLAGS (see Build from source for details).

### Build from source

#### Python

```
git clone https://github.com/deeplink-org/DLSlime.git
FLAG=<ON|OFF> pip install -v --no-build-isolation -e .
```

#### CPP

```
git clone https://github.com/deeplink-org/DLSlime.git
mkdir -p DLSlime/build && cmake -DFLAG=<ON|OFF> ..
```

#### Build flags

The `FLAG` can be

| Flag                 | Description                           | Platform | default |
| :------------------- | :------------------------------------ | :------- | ------: |
| `BUILD_RDMA`         | Build RDMA Transfer Engine            | Hetero   |      ON |
| `BUILD_PYTHON`       | Build Python wrapper                  | Hetero   |      ON |
| `BUILD_NVLINK`       | Build NVLINK Transfer Engine          | GPGPU    |     OFF |
| `BUILD_NVSHMEM`      | Build NVShmem Transfer Engine         | NVIDIA   |     OFF |
| `BUILD_TORCH_PLUGIN` | Build DLSlime as a torch backend      | Hetero   |     OFF |
| `USE_GLOO_BACKEND`   | Use GLOO RDMA Send/Recv torch backend | Hetero   |     OFF |

> \[!Note\]
> Please enable `USE_MECA` when using DLSlime as a torch backend in Metax platform.

## Benchmark

### GDRDMA P2P Read/Write

- Platform: NVIDIA ConnectX-7 HHHL Adapter Card; 200GbE (default mode) / NDR200 IB; Dual-port QSFP112; PCIe 5.0 x16 with x16 PCIe extension option; RoCE v2.

#### #BS=1, #Concurrency=1

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 1 --node-rank 1 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 1 --num-iteration 100 --num-concurrency 1
```

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 1 --node-rank 0 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 1 --num-iteration 100 --num-concurrency 1
```

| Transfer Engine | #Channels | Message Size (bytes) | Batch Size | Num Concurrency | Avg Latency(ms) | Bandwidth(MB/s) |
| --------------- | --------- | -------------------- | ---------- | --------------- | --------------- | --------------- |
| dlslime         | 1         | 2,048                | 1          | 1               | 0.039           | 52              |
| dlslime         | 1         | 4,096                | 1          | 1               | 0.037           | 111             |
| dlslime         | 1         | 8,192                | 1          | 1               | 0.038           | 216             |
| dlslime         | 1         | 16,384               | 1          | 1               | 0.037           | 442             |
| dlslime         | 1         | 32,768               | 1          | 1               | 0.039           | 836             |
| dlslime         | 1         | 65,536               | 1          | 1               | 0.039           | 1689            |
| dlslime         | 1         | 131,072              | 1          | 1               | 0.041           | 3195            |
| dlslime         | 1         | 262,144              | 1          | 1               | 0.043           | 6059            |
| dlslime         | 1         | 524,288              | 1          | 1               | 0.049           | 10689           |
| dlslime         | 1         | 1,048,576            | 1          | 1               | 0.062           | 17012           |
| dlslime         | 1         | 2,097,152            | 1          | 1               | 0.083           | 25154           |
| dlslime         | 1         | 4,194,304            | 1          | 1               | 0.127           | 33112           |
| dlslime         | 1         | 8,388,608            | 1          | 1               | 0.211           | 39797           |
| dlslime         | 1         | 16,777,216           | 1          | 1               | 0.382           | 43893           |
| dlslime         | 1         | 33,554,432           | 1          | 1               | 0.726           | 46244           |
| dlslime         | 1         | 67,108,864           | 1          | 1               | 1.412           | 47518           |
| dlslime         | 1         | 134,217,728          | 1          | 1               | 2.783           | 48235           |

#### #BS=64, #Concurrency=1

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 1 --node-rank 1 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 1
```

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 1 --node-rank 0 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 1
```

| Transfer Engine | #Channels | Message Size (bytes) | Batch Size | Num Concurrency | Avg Latency(ms) | Bandwidth(MB/s) |
| --------------- | --------- | -------------------- | ---------- | --------------- | --------------- | --------------- |
| dlslime         | 1         | 2,048                | 64         | 1               | 0.084           | 1562            |
| dlslime         | 1         | 4,096                | 64         | 1               | 0.082           | 3213            |
| dlslime         | 1         | 8,192                | 64         | 1               | 0.086           | 6095            |
| dlslime         | 1         | 16,384               | 64         | 1               | 0.093           | 11249           |
| dlslime         | 1         | 32,768               | 64         | 1               | 0.115           | 18193           |
| dlslime         | 1         | 65,536               | 64         | 1               | 0.158           | 26542           |
| dlslime         | 1         | 131,072              | 64         | 1               | 0.243           | 34498           |
| dlslime         | 1         | 262,144              | 64         | 1               | 0.414           | 40549           |
| dlslime         | 1         | 524,288              | 64         | 1               | 0.758           | 44248           |
| dlslime         | 1         | 1,048,576            | 64         | 1               | 1.443           | 46510           |
| dlslime         | 1         | 2,097,152            | 64         | 1               | 2.809           | 47782           |
| dlslime         | 1         | 4,194,304            | 64         | 1               | 5.555           | 48327           |
| dlslime         | 1         | 8,388,608            | 64         | 1               | 11.041          | 48624           |
| dlslime         | 1         | 16,777,216           | 64         | 1               | 22.003          | 48798           |
| dlslime         | 1         | 33,554,432           | 64         | 1               | 43.941          | 48872           |
| dlslime         | 1         | 67,108,864           | 64         | 1               | 87.809          | 48912           |
| dlslime         | 1         | 134,217,728          | 64         | 1               | 175.512         | 48942           |

#### #BS=64, #Concurrency=8

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 1 --node-rank 1 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 8
```

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 1 --node-rank 0 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 8
```

| Transfer Engine | #Channels | Message Size (bytes) | Batch Size | Num Concurrency | Avg Latency(ms) | Bandwidth(MB/s) |
| --------------- | --------- | -------------------- | ---------- | --------------- | --------------- | --------------- |
| dlslime         | 1         | 2,048                | 64         | 8               | 0.037           | 3519            |
| dlslime         | 1         | 4,096                | 64         | 8               | 0.038           | 6948            |
| dlslime         | 1         | 8,192                | 64         | 8               | 0.038           | 13758           |
| dlslime         | 1         | 16,384               | 64         | 8               | 0.04            | 26416           |
| dlslime         | 1         | 32,768               | 64         | 8               | 0.057           | 36997           |
| dlslime         | 1         | 65,536               | 64         | 8               | 0.098           | 42618           |
| dlslime         | 1         | 131,072              | 64         | 8               | 0.184           | 45602           |
| dlslime         | 1         | 262,144              | 64         | 8               | 0.356           | 47148           |
| dlslime         | 1         | 524,288              | 64         | 8               | 0.699           | 47975           |
| dlslime         | 1         | 1,048,576            | 64         | 8               | 1.384           | 48478           |
| dlslime         | 1         | 2,097,152            | 64         | 8               | 2.755           | 48709           |
| dlslime         | 1         | 4,194,304            | 64         | 8               | 5.498           | 48823           |
| dlslime         | 1         | 8,388,608            | 64         | 8               | 10.982          | 48884           |
| dlslime         | 1         | 16,777,216           | 64         | 8               | 21.954          | 48908           |
| dlslime         | 1         | 33,554,432           | 64         | 8               | 43.895          | 48923           |
| dlslime         | 1         | 67,108,864           | 64         | 8               | 87.766          | 48936           |
| dlslime         | 1         | 134,217,728          | 64         | 8               | 175.517         | 48940           |

### GDRDMA Aggregated Bandwidth

#### #BS=1, #Concurrency=1

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 8 --node-rank 1 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 1 --num-iteration 100 --num-concurrency 1
```

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 8 --node-rank 0 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 1 --num-iteration 100 --num-concurrency 1
```

| Transfer Engine | #Channels | Message Size (bytes) | Batch Size | Num Concurrency | Avg Latency(ms) | Bandwidth(MB/s) |
| --------------- | --------- | -------------------- | ---------- | --------------- | --------------- | --------------- |
| dlslime         | 8         | 2,048                | 1          | 1               | 0.051           | 157             |
| dlslime         | 8         | 4,096                | 1          | 1               | 0.042           | 768             |
| dlslime         | 8         | 8,192                | 1          | 1               | 0.04            | 1576            |
| dlslime         | 8         | 16,384               | 1          | 1               | 0.054           | 2929            |
| dlslime         | 8         | 32,768               | 1          | 1               | 0.051           | 5713            |
| dlslime         | 8         | 65,536               | 1          | 1               | 0.052           | 11547           |
| dlslime         | 8         | 131,072              | 1          | 1               | 0.055           | 22039           |
| dlslime         | 8         | 262,144              | 1          | 1               | 0.058           | 42313           |
| dlslime         | 8         | 524,288              | 1          | 1               | 0.064           | 74753           |
| dlslime         | 8         | 1,048,576            | 1          | 1               | 0.072           | 127489          |
| dlslime         | 8         | 2,097,152            | 1          | 1               | 0.101           | 184823          |
| dlslime         | 8         | 4,194,304            | 1          | 1               | 0.149           | 246861          |
| dlslime         | 8         | 8,388,608            | 1          | 1               | 0.237           | 299510          |
| dlslime         | 8         | 16,777,216           | 1          | 1               | 0.403           | 340252          |
| dlslime         | 8         | 33,554,432           | 1          | 1               | 0.743           | 364918          |
| dlslime         | 8         | 67,108,864           | 1          | 1               | 1.423           | 378620          |
| dlslime         | 8         | 134,217,728          | 1          | 1               | 2.79            | 384630          |

#### #BS=64, #Concurrency=1

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 8 --node-rank 1 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 1
```

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 8 --node-rank 0 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 1
```

| Transfer Engine | #Channels | Message Size (bytes) | Batch Size | Num Concurrency | Avg Latency(ms) | Bandwidth(MB/s) |
| --------------- | --------- | -------------------- | ---------- | --------------- | --------------- | --------------- |
| dlslime         | 8         | 2,048                | 64         | 1               | 0.091           | 11690           |
| dlslime         | 8         | 4,096                | 64         | 1               | 0.081           | 24403           |
| dlslime         | 8         | 8,192                | 64         | 1               | 0.091           | 45926           |
| dlslime         | 8         | 16,384               | 64         | 1               | 0.098           | 84092           |
| dlslime         | 8         | 32,768               | 64         | 1               | 0.117           | 138696          |
| dlslime         | 8         | 65,536               | 64         | 1               | 0.16            | 206866          |
| dlslime         | 8         | 131,072              | 64         | 1               | 0.241           | 273976          |
| dlslime         | 8         | 262,144              | 64         | 1               | 0.415           | 320008          |
| dlslime         | 8         | 524,288              | 64         | 1               | 0.757           | 353714          |
| dlslime         | 8         | 1,048,576            | 64         | 1               | 1.439           | 372217          |
| dlslime         | 8         | 2,097,152            | 64         | 1               | 2.819           | 381397          |
| dlslime         | 8         | 4,194,304            | 64         | 1               | 5.555           | 386489          |
| dlslime         | 8         | 8,388,608            | 64         | 1               | 11.044          | 388927          |
| dlslime         | 8         | 16,777,216           | 64         | 1               | 22.009          | 390278          |
| dlslime         | 8         | 33,554,432           | 64         | 1               | 43.951          | 390978          |
| dlslime         | 8         | 67,108,864           | 64         | 1               | 87.804          | 391370          |
| dlslime         | 8         | 134,217,728          | 64         | 1               | 175.508         | 391588          |

#### #BS=64, #Concurrency=8

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 8 --node-rank 1 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 8
```

```
torchrun --master-addr 10.130.8.145 --master-port 6006 --nnodes 2 --nproc-per-node 8 --node-rank 0 bench/python/agg_transfer_bench_spmd.py --qp-num 8 --transfer-engine dlslime --batch-size 64 --num-iteration 100 --num-concurrency 8
```

| Transfer Engine | #Channels | Message Size (bytes) | Batch Size | Num Concurrency | Avg Latency(ms) | Bandwidth(MB/s) |
| --------------- | --------- | -------------------- | ---------- | --------------- | --------------- | --------------- |
| dlslime         | 8         | 2,048                | 64         | 8               | 0.036           | 28494           |
| dlslime         | 8         | 4,096                | 64         | 8               | 0.038           | 50860           |
| dlslime         | 8         | 8,192                | 64         | 8               | 0.048           | 104545          |
| dlslime         | 8         | 16,384               | 64         | 8               | 0.041           | 207051          |
| dlslime         | 8         | 32,768               | 64         | 8               | 0.056           | 297354          |
| dlslime         | 8         | 65,536               | 64         | 8               | 0.099           | 337571          |
| dlslime         | 8         | 131,072              | 64         | 8               | 0.185           | 363003          |
| dlslime         | 8         | 262,144              | 64         | 8               | 0.356           | 376743          |
| dlslime         | 8         | 524,288              | 64         | 8               | 0.701           | 383701          |
| dlslime         | 8         | 1,048,576            | 64         | 8               | 1.386           | 387629          |
| dlslime         | 8         | 2,097,152            | 64         | 8               | 2.757           | 389493          |
| dlslime         | 8         | 4,194,304            | 64         | 8               | 5.5             | 390523          |
| dlslime         | 8         | 8,388,608            | 64         | 8               | 10.984          | 391043          |
| dlslime         | 8         | 16,777,216           | 64         | 8               | 21.955          | 391291          |
| dlslime         | 8         | 33,554,432           | 64         | 8               | 43.891          | 391407          |
| dlslime         | 8         | 67,108,864           | 64         | 8               | 87.771          | 391480          |
| dlslime         | 8         | 134,217,728          | 64         | 8               | 175.518         | 391530          |

### Heterogeneous Interconnection​

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
