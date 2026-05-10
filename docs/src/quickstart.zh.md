# 快速开始

## 直接使用 Endpoint

当应用已经自己管理 peer 放置、元数据交换和内存生命周期时，可以直接使用 Endpoint API。

```bash
python examples/python/p2p_rdma_rc_read.py
python examples/python/p2p_rdma_rc_write.py
python examples/python/p2p_rdma_rc_write_with_imm_data.py
python examples/python/p2p_rdma_rc_send_recv_gdr.py
torchrun --nproc_per_node=2 examples/python/p2p_nvlink.py
python examples/python/p2p_ascend_read.py
```

## PeerAgent 到 PeerAgent

当应用希望 DLSlime 管理连接建立和内存区域发现时，可以使用 PeerAgent。

```bash
nanoctrl start
python examples/python/p2p_rdma_rc_read_ctrl_plane.py
```

## DLSlimeCache 服务

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765

dlslime-cache stop
```
