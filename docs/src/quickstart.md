# Quickstart

## Direct Endpoint Access

Use the Endpoint API directly when the application already controls peer
placement, metadata exchange, and memory lifetime.

```bash
python examples/python/p2p_rdma_rc_read.py
python examples/python/p2p_rdma_rc_write.py
python examples/python/p2p_rdma_rc_write_with_imm_data.py
python examples/python/p2p_rdma_rc_send_recv_gdr.py
torchrun --nproc_per_node=2 examples/python/p2p_nvlink.py
python examples/python/p2p_ascend_read.py
```

## PeerAgent-to-PeerAgent Access

Use PeerAgent when the application wants peer-to-peer data movement without
owning connection setup and memory-region discovery.

```bash
nanoctrl start
python examples/python/p2p_rdma_rc_read_ctrl_plane.py
```

## DLSlimeCache Service

```bash
nanoctrl start
dlslime-cache start --ctrl http://127.0.0.1:3000 \
  --host 127.0.0.1 --port 8765 --memory-size 1G

python examples/python/cache_client_example.py --url http://127.0.0.1:8765

dlslime-cache stop
```

## SlimeRPC Service

```bash
nanoctrl start
python examples/python/rpc_example.py --ctrl http://127.0.0.1:3000
```
