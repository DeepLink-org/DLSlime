"""Stress test for writeWithImm + immRecv matching.

The refactor's most behaviorally-changed path is ``imm_recv``. Before,
ImmRecvFuture read completion fields off a transport-owned slot whose
memory could be recycled under it. After the refactor, each user
``immRecv()`` allocates a stable ``EndpointOpState``; the endpoint
matches arriving WRITE_WITH_IMM completions against either
``pending_imm_recv_ops_`` (user already waiting) or queues them in
``completed_imm_recv_events_`` (write landed first). This test
exercises both sides of that match, plus the interleaved case, and
verifies every ``imm_data`` value lands at exactly one future.

Three modes:

    * pre_post   — every immRecv() is posted BEFORE any writeWithImm.
                   Forces the pending-ops path on arrival.
    * post_first — every writeWithImm lands BEFORE any immRecv() call.
                   Forces the completed-events queue path.
    * interleaved — alternating submissions. Both paths at once.

Run directly:

    pytest tests/python/test_stress_imm_recv.py -v
"""

import os

os.environ.setdefault("SLIME_MAX_IO_FIFO_DEPTH", "16")

import time

import dlslime
import pytest
import torch

DEPTH = int(os.environ["SLIME_MAX_IO_FIFO_DEPTH"])
NUM_OPS = int(os.environ.get("STRESS_IMM_OPS", str(DEPTH * 4)))
# imm_data is a 32-bit field on the wire. We encode the op_idx directly
# so a cross-wired completion is immediately visible as an imm/futures
# set mismatch.
IMM_BASE = int(os.environ.get("STRESS_IMM_BASE", "1000"))
# Bytes per writeWithImm. Small — the semantic payload of this test is
# the imm, not the data.
PAYLOAD_BYTES = int(os.environ.get("STRESS_IMM_PAYLOAD_BYTES", "32"))


@pytest.fixture(scope="module", params=[1, 2, 4], ids=lambda q: f"num_qp={q}")
def endpoints(request):
    num_qp = request.param

    nics = dlslime.available_nic()
    if not nics:
        pytest.skip("no RDMA NICs available")

    initiator = dlslime.RDMAEndpoint(
        device_name=nics[0], ib_port=1, link_type="RoCE", num_qp=num_qp
    )
    target = dlslime.RDMAEndpoint(
        device_name=nics[-1], ib_port=1, link_type="RoCE", num_qp=num_qp
    )

    # A single shared remote buffer on target; initiator writes to
    # slices of it and the IMM is what we actually check.
    total = NUM_OPS * PAYLOAD_BYTES
    local_buf = (torch.arange(total, dtype=torch.int64) % 256).to(torch.uint8)
    remote_buf = torch.zeros(total, dtype=torch.uint8)

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

    del initiator, target


def _issue_write(initiator, h_local, h_remote, op_idx):
    off = op_idx * PAYLOAD_BYTES
    assign = [(h_local, h_remote, off, off, PAYLOAD_BYTES)]
    return initiator.write_with_imm(assign, IMM_BASE + op_idx, None)


def _assert_no_cross_wire(imm_futures):
    """Every future must return with a unique imm in the expected
    range. No duplicates, no missing."""
    seen = set()
    for i, fut in enumerate(imm_futures):
        fut.wait()
        imm = fut.imm_data()
        assert (
            imm not in seen
        ), f"duplicate imm={imm} (future #{i}); previous futures: {sorted(seen)}"
        seen.add(imm)

    expected = set(range(IMM_BASE, IMM_BASE + NUM_OPS))
    missing = expected - seen
    extra = seen - expected
    assert not missing and not extra, (
        f"imm set mismatch; missing={sorted(missing)[:16]} "
        f"extra={sorted(extra)[:16]}"
    )


def test_imm_recv_pre_post(endpoints):
    """All immRecv() futures posted BEFORE any write. Forces every
    completion to match against pending_imm_recv_ops_."""
    initiator = endpoints["initiator"]
    target = endpoints["target"]
    h_local = endpoints["h_local"]
    h_remote = endpoints["h_remote"]

    imm_futs = [target.imm_recv(None) for _ in range(NUM_OPS)]
    write_futs = [_issue_write(initiator, h_local, h_remote, i) for i in range(NUM_OPS)]

    for fut in write_futs:
        fut.wait()

    _assert_no_cross_wire(imm_futs)


def test_imm_recv_post_first(endpoints):
    """Writes land and are queued in completed_imm_recv_events_ BEFORE
    the user ever calls immRecv(). Every user future is satisfied by
    dequeuing a queued event."""
    initiator = endpoints["initiator"]
    target = endpoints["target"]
    h_local = endpoints["h_local"]
    h_remote = endpoints["h_remote"]

    write_futs = [_issue_write(initiator, h_local, h_remote, i) for i in range(NUM_OPS)]
    for fut in write_futs:
        fut.wait()

    # Give the progress thread a moment to drain the refill stack and
    # queue every event into completed_imm_recv_events_.
    time.sleep(0.05)

    imm_futs = [target.imm_recv(None) for _ in range(NUM_OPS)]
    _assert_no_cross_wire(imm_futs)


def test_imm_recv_interleaved(endpoints):
    """Alternating submission order. On each iteration the match may
    land in pending_imm_recv_ops_ or in completed_imm_recv_events_
    depending on which side the progress thread services first. Both
    paths must handle it correctly."""
    initiator = endpoints["initiator"]
    target = endpoints["target"]
    h_local = endpoints["h_local"]
    h_remote = endpoints["h_remote"]

    imm_futs = []
    write_futs = []
    for i in range(NUM_OPS):
        # Slightly bias toward posting immRecv first on even ops and
        # writeWithImm first on odd ops; both orders must be safe.
        if i % 2 == 0:
            imm_futs.append(target.imm_recv(None))
            write_futs.append(_issue_write(initiator, h_local, h_remote, i))
        else:
            write_futs.append(_issue_write(initiator, h_local, h_remote, i))
            imm_futs.append(target.imm_recv(None))

    for fut in write_futs:
        fut.wait()

    _assert_no_cross_wire(imm_futs)
