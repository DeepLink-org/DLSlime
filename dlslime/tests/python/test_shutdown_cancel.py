"""Test that ``RDMAEndpoint::shutdown()`` / ``cancelAll()`` force-completes
outstanding user ops instead of letting their futures hang forever.

The refactor tracks every in-flight ``EndpointOpState`` through a
weak_ptr registry (``in_flight_ops_``). On shutdown, ``cancelAll()``:

  * drains ``pending_imm_recv_ops_`` (ops that posted but never matched)
    and ``completed_imm_recv_events_``, setting FAILED and waking signals;
  * iterates ``in_flight_ops_``, locks each weak_ptr, and
    force-completes exactly those ops;
  * force-completes the transport-owned imm-recv slot signals.

This test exercises those paths by posting ``immRecv()`` futures that
have NO corresponding ``writeWithImm`` on the peer. Without shutdown
they would block forever in ``wait()``. shutdown() must wake them and
they must report FAILED.
"""

import os

os.environ.setdefault("SLIME_MAX_IO_FIFO_DEPTH", "16")

import threading

import dlslime
import pytest

N_PENDING = int(os.environ.get("STRESS_CANCEL_N", "8"))

# RDMAAssign::CALLBACK_STATUS::FAILED = 403.
FAILED = 403


@pytest.fixture
def endpoints():
    nics = dlslime.available_nic()
    if not nics:
        pytest.skip("no RDMA NICs available")

    # Fresh per-test endpoints because shutdown() is destructive.
    initiator = dlslime.RDMAEndpoint(device_name=nics[0], ib_port=1, link_type="RoCE")
    target = dlslime.RDMAEndpoint(device_name=nics[-1], ib_port=1, link_type="RoCE")

    target.connect(initiator.endpoint_info())
    initiator.connect(target.endpoint_info())

    yield initiator, target

    # Best-effort cleanup; tests may have already shut things down.
    del initiator, target


def test_shutdown_wakes_pending_imm_recv(endpoints):
    """immRecv futures posted with no matching write should unblock
    on shutdown() and report FAILED rather than SUCCESS."""
    initiator, target = endpoints
    del initiator

    futs = [target.imm_recv(None) for _ in range(N_PENDING)]

    # Wait on all of them in background threads — shutdown must unblock
    # every one. If shutdown left any stuck we time out on join().
    results = [None] * N_PENDING

    def _await(idx):
        results[idx] = futs[idx].wait()

    waiters = [threading.Thread(target=_await, args=(i,)) for i in range(N_PENDING)]
    for t in waiters:
        t.start()

    # Sanity: the threads should actually be blocked on wait() right
    # now (no corresponding writeWithImm was issued). Give them a beat
    # to enter wait_comm_done_cpu.
    import time

    time.sleep(0.05)
    alive_before = [t.is_alive() for t in waiters]
    assert all(alive_before), (
        "expected every wait() to be blocked before shutdown, " f"got {alive_before}"
    )

    target.shutdown()

    for t in waiters:
        t.join(timeout=5.0)
        assert not t.is_alive(), (
            "wait() did not return within 5s of shutdown — cancelAll "
            "did not wake the future"
        )

    # Every future should report FAILED; SUCCESS would mean the
    # completion path fired a real CQE, which cannot happen because no
    # peer write was ever issued.
    for i, status in enumerate(results):
        assert status == FAILED, (
            f"future #{i}: expected FAILED ({FAILED}), got {status}. "
            f"cancelAll must set completion_status=FAILED before "
            f"force_complete()-ing the signal"
        )


def test_shutdown_is_idempotent(endpoints):
    """Calling shutdown() a second time should not deadlock or throw.
    No pending futures; just exercise the path."""
    _, target = endpoints
    target.shutdown()
    # Second call — should be a no-op.
    target.shutdown()
