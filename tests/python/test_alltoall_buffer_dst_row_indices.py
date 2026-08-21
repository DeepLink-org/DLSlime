import os

import pytest
import torch
import torch.distributed as dist

from dlslime import AllToAllBuffer, KernelImpl


MAX_BS = 4
LOCAL_ROWS = 2
MSG_SIZE = 128
DTYPE = torch.float16
SENTINEL = -999.0


def _connect_full_mesh(buffer: AllToAllBuffer, group: dist.ProcessGroup) -> None:
    my_handle_info = buffer.get_ipc_handle_info()
    all_handle_infos = [None for _ in range(dist.get_world_size(group=group))]
    dist.all_gather_object(all_handle_infos, my_handle_info, group=group)
    buffer.connect_full_mesh(all_handle_infos)


def _participants(src_rank: int, local_idx: int, world_size: int) -> set[int]:
    return {
        src_rank,
        (src_rank + 1 + local_idx) % world_size,
        (src_rank + 3 + 2 * local_idx) % world_size,
    }


def _tag(src_rank: int, local_idx: int) -> float:
    return float(1 + src_rank * LOCAL_ROWS + local_idx)


def _build_routing(
    rank: int,
    world_size: int,
    device: torch.device,
) -> tuple[torch.Tensor, list[list[float]]]:
    dst_rows = torch.full(
        (world_size, MAX_BS),
        -1,
        dtype=torch.int32,
        device=device,
    )
    expected_by_dst: list[list[float]] = [[] for _ in range(world_size)]

    for dst_rank in range(world_size):
        for src_rank in range(world_size):
            for local_idx in range(LOCAL_ROWS):
                if dst_rank not in _participants(src_rank, local_idx, world_size):
                    continue
                dst_row = len(expected_by_dst[dst_rank])
                expected_by_dst[dst_rank].append(_tag(src_rank, local_idx))
                if src_rank == rank and dst_rank != rank:
                    dst_rows[dst_rank, local_idx] = dst_row

    return dst_rows, expected_by_dst


def _prepare_local_buffer(
    buffer: AllToAllBuffer,
    rank: int,
    world_size: int,
    expected_by_dst: list[list[float]],
) -> torch.Tensor:
    local = buffer.local_buffer.view(DTYPE)[: world_size * MAX_BS * MSG_SIZE]
    local = local.view(world_size * MAX_BS, MSG_SIZE)
    local.fill_(SENTINEL)

    receiver_rows = expected_by_dst[rank]
    row = 0
    for src_rank in range(world_size):
        for local_idx in range(LOCAL_ROWS):
            if rank not in _participants(src_rank, local_idx, world_size):
                continue
            if src_rank == rank:
                local[row].fill_(_tag(src_rank, local_idx))
            row += 1
    assert row == len(receiver_rows)
    return local


def _assert_receiver_output(
    output: torch.Tensor,
    rank: int,
    expected_by_dst: list[list[float]],
) -> None:
    expected_values = expected_by_dst[rank]
    flat = output.view(-1, MSG_SIZE)
    expected = torch.tensor(
        expected_values,
        dtype=DTYPE,
        device=flat.device,
    ).unsqueeze(1).expand(-1, MSG_SIZE)
    assert torch.equal(flat[: len(expected_values)], expected)
    assert torch.all(flat[len(expected_values) :] == SENTINEL)


def test_basic_destination_row_indices_route_non_nested_receiver_layouts():
    if not torch.cuda.is_available():
        pytest.skip("CUDA is required for destination-row routing tests.")

    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    torch.cuda.set_device(local_rank)
    device = torch.device(f"cuda:{local_rank}")
    if not dist.is_initialized():
        dist.init_process_group("nccl", device_id=device)

    rank = dist.get_rank()
    world_size = dist.get_world_size()
    if world_size < 2:
        pytest.skip("Run with at least two ranks.")

    try:
        buffer_size = world_size * MAX_BS * MSG_SIZE * DTYPE.itemsize
        buffer = AllToAllBuffer(rank, world_size, MAX_BS, buffer_size)
        _connect_full_mesh(buffer, dist.group.WORLD)
        dst_rows, expected_by_dst = _build_routing(rank, world_size, device)
        x = torch.stack(
            [
                torch.full((MSG_SIZE,), _tag(rank, local_idx), dtype=DTYPE, device=device)
                for local_idx in range(LOCAL_ROWS)
            ]
        )

        with pytest.raises(RuntimeError, match="cannot be combined with offsets"):
            buffer.all_to_all(
                x,
                impl=KernelImpl.Basic,
                is_transpose=False,
                offsets=torch.arange(world_size + 1, dtype=torch.int32, device=device),
                dst_row_indices=dst_rows,
            )

        _prepare_local_buffer(buffer, rank, world_size, expected_by_dst)
        dist.barrier(device_ids=[local_rank])
        eager_output = buffer.all_to_all(
            x,
            impl=KernelImpl.Basic,
            is_transpose=False,
            dst_row_indices=dst_rows,
        )
        torch.cuda.synchronize(device)
        dist.barrier(device_ids=[local_rank])
        _assert_receiver_output(eager_output, rank, expected_by_dst)

        _prepare_local_buffer(buffer, rank, world_size, expected_by_dst)
        graph = torch.cuda.CUDAGraph()
        for _ in range(3):
            buffer.all_to_all(
                x,
                impl=KernelImpl.Basic,
                is_transpose=False,
                dst_row_indices=dst_rows,
            )
        torch.cuda.synchronize(device)
        dist.barrier(device_ids=[local_rank])
        with torch.cuda.graph(graph):
            graph_output = buffer.all_to_all(
                x,
                impl=KernelImpl.Basic,
                is_transpose=False,
                dst_row_indices=dst_rows,
            )
        torch.cuda.synchronize(device)
        dist.barrier(device_ids=[local_rank])

        _prepare_local_buffer(buffer, rank, world_size, expected_by_dst)
        graph.replay()
        torch.cuda.synchronize(device)
        dist.barrier(device_ids=[local_rank])
        _assert_receiver_output(graph_output, rank, expected_by_dst)
    finally:
        if dist.is_initialized():
            dist.destroy_process_group()
