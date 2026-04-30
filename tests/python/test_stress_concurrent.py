"""Stress test for the free-slot ring back-pressure path.

This test issues more concurrent RDMA reads than ``SLIME_MAX_IO_FIFO_DEPTH``.
Before the PR #70 free-slot ring refactor, this would have either stomped
a still-in-flight slot (silently corrupting completion delivery) or
hung. With the refactor, acquireReadWriteSlot() blocks (spins) on the
free ring until the previous tenant's per-qp callbacks have fired, so
the test should complete and every read's local buffer should match its
corresponding remote window byte-for-byte.

Run directly:

    pytest tests/python/ops/stress_concurrent.py -v

Or drive with a custom depth / op count:

    SLIME_MAX_IO_FIFO_DEPTH=32 STRESS_NUM_READS=512 \\
        pytest tests/python/ops/stress_concurrent.py -v
"""

# Env must be set before `import dlslime` — the C++ layer reads these
# env vars at first library init and never re-reads them.
import os

os.environ.setdefault("SLIME_MAX_IO_FIFO_DEPTH", "16")
os.environ.setdefault("SLIME_MAX_MSG_FIFO_DEPTH", "16")

import dlslime
import pytest
import torch

# Knobs — all overridable via env so the same test can run in CI-fast
# mode and in a deeper stress configuration without code changes.
DEPTH = int(os.environ["SLIME_MAX_IO_FIFO_DEPTH"])
NUM_READS = int(os.environ.get("STRESS_NUM_READS", str(DEPTH * 8)))
WINDOW_BYTES = int(os.environ.get("STRESS_WINDOW_BYTES", "16"))


def _pattern(n: int) -> torch.Tensor:
    """Deterministic byte pattern covering ``n`` bytes."""
    return (torch.arange(n, dtype=torch.int64) % 256).to(torch.uint8)


@pytest.fixture(scope="module")
def endpoints():
    nics = dlslime.available_nic()
    if not nics:
        pytest.skip("no RDMA NICs available")

    total = NUM_READS * WINDOW_BYTES

    initiator = dlslime.RDMAEndpoint(device_name=nics[0], ib_port=1, link_type="RoCE")
    target = dlslime.RDMAEndpoint(device_name=nics[-1], ib_port=1, link_type="RoCE")

    # Local (destination on initiator) and remote (source on target)
    # buffers. Remote holds a known pattern; local starts zeroed and
    # each read fills one WINDOW_BYTES-wide slice of it.
    local_buf = torch.zeros(total, dtype=torch.uint8)
    remote_buf = _pattern(total)

    h_local = initiator.register_memory_region(
        "local",
        local_buf.data_ptr(),
        int(local_buf.storage_offset()),
        total,
    )
    target.register_memory_region(
        "remote",
        remote_buf.data_ptr(),
        int(remote_buf.storage_offset()),
        total,
    )

    # OOB exchange → initiator learns the target's remote MR handle.
    info = target.endpoint_info()
    h_remote = initiator.register_remote_memory_region(
        "remote", info["mr_info"]["remote"]
    )

    target.connect(initiator.endpoint_info())
    initiator.connect(target.endpoint_info())

    yield {
        "initiator": initiator,
        "target": target,
        "local_buf": local_buf,
        "remote_buf": remote_buf,
        "h_local": h_local,
        "h_remote": h_remote,
    }

    # Explicit teardown so the background worker stops observing these
    # endpoints before Python's GC tears the pybind objects down in
    # arbitrary order.
    del initiator, target


def test_concurrent_reads_exceed_depth(endpoints):
    """More concurrent reads than SLIME_MAX_IO_FIFO_DEPTH → exercises
    the blocking free-slot acquire path. Each read targets a distinct,
    non-overlapping window so a wrong-op completion (the ABA hazard
    this refactor fixes) would be caught as a byte mismatch in the
    local buffer."""

    assert NUM_READS > DEPTH, (
        f"NUM_READS={NUM_READS} must exceed SLIME_MAX_IO_FIFO_DEPTH={DEPTH} "
        f"to actually stress the free-slot ring"
    )

    initiator = endpoints["initiator"]
    local_buf = endpoints["local_buf"]
    remote_buf = endpoints["remote_buf"]
    h_local = endpoints["h_local"]
    h_remote = endpoints["h_remote"]

    futures = []
    for i in range(NUM_READS):
        off = i * WINDOW_BYTES
        # (src_handle, dst_handle, dst_offset, src_offset, length)
        assign = [(h_local, h_remote, off, off, WINDOW_BYTES)]
        futures.append(initiator.read(assign, None))

    for fut in futures:
        fut.wait()

    # Byte-for-byte: if any callback had been delivered to the wrong
    # op_state the read at that offset would still be zero (or contain
    # some other op's data).
    if not torch.equal(local_buf, remote_buf):
        # Find first mismatch to make failures debuggable.
        diff = (local_buf != remote_buf).nonzero(as_tuple=True)[0]
        first = int(diff[0]) if diff.numel() else -1
        pytest.fail(
            f"{diff.numel()} byte(s) mismatch; first at offset {first}. "
            f"local[{first}:{first + 16}]={local_buf[first:first + 16].tolist()} "
            f"remote[{first}:{first + 16}]={remote_buf[first:first + 16].tolist()}"
        )


def test_reissue_after_drain(endpoints):
    """After a first batch fully drains, the free ring should be back
    to full capacity and a second batch should behave identically.
    Catches a slot-release-never-fires regression that would leak
    slots on each op."""

    initiator = endpoints["initiator"]
    local_buf = endpoints["local_buf"]
    remote_buf = endpoints["remote_buf"]
    h_local = endpoints["h_local"]
    h_remote = endpoints["h_remote"]

    for round_idx in range(2):
        local_buf.zero_()
        futures = [
            initiator.read(
                [(h_local, h_remote, i * WINDOW_BYTES, i * WINDOW_BYTES, WINDOW_BYTES)],
                None,
            )
            for i in range(NUM_READS)
        ]
        for fut in futures:
            fut.wait()
        assert torch.equal(local_buf, remote_buf), f"round {round_idx}: buffers differ"
