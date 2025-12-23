import os

import torch
import torch.distributed as dist
from dlslime.buffer.intra.all_to_all_intra_ll_buffer import AllToAllIntraLLBuffer

# 配置信息
MAX_BS = 128
MSG_SIZE = 128
DTYPE = torch.float16


def test_all_to_all_offsets():
    if not torch.cuda.is_available():
        print("CUDA not available, skipping test.")
        return

    # Get rank and set device before init_process_group to avoid warnings
    local_rank = int(os.environ.get("LOCAL_RANK", 0))
    torch.cuda.set_device(local_rank)

    if not dist.is_initialized():
        dist.init_process_group("nccl", device_id=torch.device(f"cuda:{local_rank}"))

    rank = dist.get_rank()
    world_size = dist.get_world_size()

    # Each rank has a different number of messages to send
    counts = torch.tensor(
        [(i % MAX_BS) + 1 for i in range(world_size)], dtype=torch.int32, device="cuda"
    )
    total_messages = counts.sum().item()

    # Calculate offsets: [0, count0, count0+count1, ...]
    offsets = torch.zeros(world_size + 1, dtype=torch.int32, device="cuda")
    offsets[1:] = torch.cumsum(counts, dim=0)

    # Buffer size hint
    buffer_size = AllToAllIntraLLBuffer.get_buffer_size_hint(
        world_size, MAX_BS, MSG_SIZE, DTYPE.itemsize
    )

    buffer = AllToAllIntraLLBuffer(world_size, MAX_BS, rank, world_size, buffer_size)

    # Connect mesh
    buffer.connect_full_mesh(group=dist.group.WORLD)

    # Prepare input data for this rank
    my_count = counts[rank].item()
    x = torch.ones((my_count, MSG_SIZE), dtype=DTYPE, device=f"cuda:{rank}") * (
        rank + 1
    )

    # Test without offsets
    res_no_offsets = buffer.all_to_all_ll(x, is_transpose=False)

    if rank == 0:
        print(f"Result without offsets shape: {res_no_offsets.shape}")
        # Check if rank i's data is at res_no_offsets[i, 0:counts[i]]
        for i in range(world_size):
            expected_val = i + 1
            actual_slice = res_no_offsets[i, : counts[i]]
            assert torch.all(
                actual_slice == expected_val
            ), f"Rank {i} data mismatch without offsets"
            # The rest [counts[i]:max_bs] might be garbage or zero depending on implementation
            # but the point is it's padded.

    dist.barrier(device_ids=[local_rank])

    # --- 1. Test WITH offsets (Packed AllGather) ---
    res_with_offsets = buffer.all_to_all_ll(x, is_transpose=False, offsets=offsets)

    if rank == 0:
        print(f"Testing offsets (MAX_BS={MAX_BS})...")
        print(f"Result with offsets shape: {res_with_offsets.shape}")
        assert res_with_offsets.shape == (total_messages, MSG_SIZE)

        # Verify contiguous data
        for i in range(world_size):
            start = offsets[i].item()
            end = offsets[i + 1].item()
            expected_val = i + 1
            actual_slice = res_with_offsets[start:end]
            assert torch.all(
                actual_slice == expected_val
            ), f"Rank {i} data mismatch with offsets"

        print("Offset verification successful!")

    dist.barrier(device_ids=[local_rank])

    # --- 2. Test WITH offsets AND mask (Detect unnecessary transfers) ---
    # Pre-fill buffer with a "dirty" value (all ones in int8, which is not rank+1 in float16)
    buffer.local_buffer.fill_(0x7F)

    # Create a mask: only even-indexed messages from each rank are allowed
    # Mask shape: (world_size, MAX_BS)
    mask = torch.zeros((world_size, MAX_BS), dtype=torch.int32, device="cuda")
    for i in range(world_size):
        for j in range(MAX_BS):
            if j % 2 == 0:
                mask[i, j] = 1

    # Run with mask
    res_masked = buffer.all_to_all_ll(x, is_transpose=False, offsets=offsets, mask=mask)

    if rank == 0:
        print("Testing mask efficiency...")
        for i in range(world_size):
            start = offsets[i].item()
            end = offsets[i + 1].item()
            expected_val = i + 1

            for msg_idx in range(end - start):
                actual_idx = start + msg_idx
                if msg_idx % 2 == 0:
                    # Should be transferred
                    assert torch.all(
                        res_masked[actual_idx] == expected_val
                    ), f"Rank {i} msg {msg_idx} should have been transferred"
                else:
                    # Should NOT be transferred, should still be the dirty value
                    # We check it's NOT the expected_val
                    assert torch.any(
                        res_masked[actual_idx] != expected_val
                    ), f"Rank {i} msg {msg_idx} should NOT have been transferred (Masked Out)"

        print("Mask verification successful! No unnecessary transfers detected.")

    dist.destroy_process_group()


if __name__ == "__main__":
    test_all_to_all_offsets()
