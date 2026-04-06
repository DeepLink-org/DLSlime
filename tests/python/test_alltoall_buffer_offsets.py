import os

import pytest
import torch
import torch.distributed as dist

from dlslime import AllToAllBuffer, KernelImpl


MAX_BS = 128
MSG_SIZE = 128
DTYPE = torch.float16


def _connect_full_mesh(buffer: AllToAllBuffer, group: dist.ProcessGroup) -> None:
    my_handle_info = buffer.get_ipc_handle_info()
    all_handle_infos = [None for _ in range(dist.get_world_size(group=group))]
    dist.all_gather_object(all_handle_infos, my_handle_info, group=group)
    buffer.connect_full_mesh(all_handle_infos)


def _init_dist_or_skip() -> tuple[int, int, int]:
    if not torch.cuda.is_available():
        pytest.skip("CUDA is required for AllToAllBuffer offsets tests.")

    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    torch.cuda.set_device(local_rank)

    env_world_size = int(os.environ.get("WORLD_SIZE", "1"))
    if env_world_size < 2 and not dist.is_initialized():
        pytest.skip(
            "Run this test with at least 2 ranks, for example: "
            "`torchrun --nproc_per_node=2 -m pytest -q tests/python/test_alltoall_buffer_offsets.py -s`."
        )

    if not dist.is_initialized():
        dist.init_process_group("nccl", device_id=torch.device(f"cuda:{local_rank}"))

    return local_rank, dist.get_rank(), dist.get_world_size()


def _init_cuda_or_skip() -> int:
    if not torch.cuda.is_available():
        pytest.skip("CUDA is required for AllToAllBuffer offsets tests.")

    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    torch.cuda.set_device(local_rank)
    return local_rank


def test_alltoall_buffer_basic_supports_non_transpose_offsets():
    local_rank, rank, world_size = _init_dist_or_skip()
    try:
        counts = torch.tensor(
            [(i % MAX_BS) + 1 for i in range(world_size)],
            dtype=torch.int32,
            device="cuda",
        )
        offsets = torch.zeros(world_size + 1, dtype=torch.int32, device="cuda")
        offsets[1:] = torch.cumsum(counts, dim=0)

        buffer_size = world_size * MAX_BS * MSG_SIZE * DTYPE.itemsize
        buffer = AllToAllBuffer(rank, world_size, MAX_BS, buffer_size)
        _connect_full_mesh(buffer, dist.group.WORLD)

        my_count = int(counts[rank].item())
        x = torch.full((my_count, MSG_SIZE), rank + 1, dtype=DTYPE, device=f"cuda:{local_rank}")

        output = buffer.all_to_all(
            x,
            impl=KernelImpl.Basic,
            is_transpose=False,
            offsets=offsets,
        )

        flat_output = output.view(-1, MSG_SIZE)
        for src_rank in range(world_size):
            start = int(offsets[src_rank].item())
            end = int(offsets[src_rank + 1].item())
            assert torch.all(flat_output[start:end] == (src_rank + 1))

        dist.barrier(device_ids=[local_rank])
    finally:
        if dist.is_initialized():
            dist.destroy_process_group()


def test_alltoall_buffer_basic_rejects_transpose_offsets():
    local_rank = _init_cuda_or_skip()

    world_size = max(int(os.environ.get("WORLD_SIZE", "1")), 2)
    rank = min(local_rank, world_size - 1)
    counts = torch.tensor(
        [(i % MAX_BS) + 1 for i in range(world_size)],
        dtype=torch.int32,
        device="cuda",
    )
    offsets = torch.zeros(world_size + 1, dtype=torch.int32, device="cuda")
    offsets[1:] = torch.cumsum(counts, dim=0)

    buffer_size = world_size * MAX_BS * MSG_SIZE * DTYPE.itemsize
    buffer = AllToAllBuffer(rank, world_size, MAX_BS, buffer_size)

    my_count = int(counts[rank].item())
    x = torch.zeros((world_size * my_count, MSG_SIZE), dtype=DTYPE, device=f"cuda:{local_rank}")

    with pytest.raises(RuntimeError, match="non-transpose"):
        buffer.all_to_all(
            x,
            impl=KernelImpl.Basic,
            is_transpose=True,
            offsets=offsets,
        )
