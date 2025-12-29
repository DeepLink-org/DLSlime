import os

import torch
import torch.distributed as dist
import xxhash
from dlslime import _slime_c


def run_benchmark():
    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    local_rank = int(os.environ["LOCAL_RANK"])

    torch.cuda.set_device(local_rank)
    device = torch.device(f"cuda:{local_rank}")

    if world_size < 2:
        raise RuntimeError("需要至少 2 个 GPU 才能运行此 P2P 测试")

    ep = _slime_c.NVLinkEndpoint()

    mr_key = xxhash.xxh64_intdigest("buffer")

    if rank == 0:
        tensor = torch.zeros([16], device=device, dtype=torch.uint8)
        role = "Initiator"
    else:
        tensor = torch.ones([16], device=device, dtype=torch.uint8)
        role = "Target"

    ep.register_memory_region(
        mr_key,
        tensor.data_ptr(),
        int(tensor.storage_offset()),
        tensor.numel() * tensor.itemsize,
    )

    local_info = ep.endpoint_info()

    gather_list = [{} for _ in range(world_size)]

    dist.all_gather_object(gather_list, local_info)

    target_rank = 1 - rank  # 简单的 0<->1 映射
    remote_info = gather_list[target_rank]

    print(f"[Rank {rank}] Connecting to Rank {target_rank}...")
    ep.connect(remote_info)

    dist.barrier()

    if rank == 0:

        ep.read(
            [(mr_key, mr_key, 8, 0, 8)],
            None,
        )

        torch.cuda.synchronize()

        print(f"[Rank {rank}] After Read:  {tensor}")

        assert torch.all(tensor[:8] == 1), f"First half check failed: {tensor[:8]}"
        assert torch.all(tensor[8:] == 0), f"Second half check failed: {tensor[8:]}"

        print("run rdma rc write example successful")

    dist.barrier()

    # 清理
    del ep
    dist.destroy_process_group()


if __name__ == "__main__":
    run_benchmark()
