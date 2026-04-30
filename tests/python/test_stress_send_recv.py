"""Stress test for send/recv slot reuse after the PR #70 refactor.

What this verifies
------------------
The pre-refactor code reused SendContext / RecvContext slots via
``counter % DEPTH``. That left two failure modes on the two-sided path:

1. A slot could be re-leased to op B while op A's data-send callback
   was still in flight, so the CQE for op A's WR would dispatch into
   op B's callback (op A's future hangs, op B's future completes
   early — "cross-wiring").
2. A slot's per-op transport state (``meta_arrived_flag_``,
   ``remote_meta_info_``, …) could be stomped by a later op while the
   previous op's WRs were still outstanding.

After the refactor a slot leaves ``send_free_ring_`` / ``recv_free_ring_``
on acquire and returns only after every per-qp data-path callback for
its current tenant has fired. So:

* slot-release-on-complete must actually fire (I2 in
  ``docs/endpoint-ownership-model.md``) — otherwise the free ring
  drains and the (DEPTH+1)-th op blocks forever on acquire;
* the callback for op A must deliver completion to op A's
  ``EndpointOpState`` — otherwise the recv buffer for op A would end
  up with the wrong bytes.

Why this test is sequential, not concurrent
-------------------------------------------
The PR explicitly leaves one pre-existing protocol hazard unfixed:
``SendContext::meta_arrived_flag_`` is set by whichever meta RECV the
hardware consumes in FIFO order, not by the slot the peer wrote meta
for. With two or more concurrent sends in flight the flag ends up on
the wrong slot and ``sendProcess`` deadlocks in ``WAIT_META``. This is
a protocol-level issue that the free-slot-ring refactor does not
address (see ``docs/endpoint-ownership-model.md``, "Message-path
specifics"). Running concurrent pairs through this stress would hang
on that protocol bug, not on the refactor. So we use sequential
churn, which exercises the refactor-owned invariants (release-on-
complete + no-cross-wire-after-reuse) and does NOT trip the
meta-handshake protocol bug.

Run directly:

    pytest tests/python/stress_send_recv.py -v

Overridable via env:

    SLIME_MAX_MSG_FIFO_DEPTH=4 STRESS_ROUNDS=32 STRESS_MSG_BYTES=1024 \\
        pytest tests/python/stress_send_recv.py -v
"""

# Env must be set before `import dlslime` — the C++ layer reads these
# env vars at first library init and never re-reads them.
import os

os.environ.setdefault("SLIME_MAX_MSG_FIFO_DEPTH", "8")
os.environ.setdefault("SLIME_MAX_IO_FIFO_DEPTH", "16")

import dlslime
import pytest
import torch

DEPTH = int(os.environ["SLIME_MAX_MSG_FIFO_DEPTH"])
# Every op takes one slot on each side. TOTAL_OPS > DEPTH guarantees
# every slot in the pool gets reused at least once; TOTAL_OPS >>
# DEPTH forces each slot through the free ring many times.
TOTAL_OPS = int(os.environ.get("STRESS_ROUNDS", "16")) * DEPTH
MSG_BYTES = int(os.environ.get("STRESS_MSG_BYTES", "256"))


def _payload(op_idx: int, nbytes: int) -> torch.Tensor:
    """Unique-per-op byte pattern. The first 4 bytes hold op_idx in
    little-endian; the rest are filled with (op_idx % 256). A
    cross-wired completion would leave either the header or the body
    bearing some other op's marks."""
    buf = torch.full((nbytes,), op_idx % 256, dtype=torch.uint8)
    tag = int(op_idx).to_bytes(4, "little", signed=False)
    for i, b in enumerate(tag):
        buf[i] = b
    return buf


@pytest.fixture(scope="module", params=[1, 2, 4], ids=lambda q: f"num_qp={q}")
def endpoints(request):
    num_qp = request.param

    nics = dlslime.available_nic()
    if not nics:
        pytest.skip("no RDMA NICs available")

    sender = dlslime.RDMAEndpoint(
        device_name=nics[0], ib_port=1, link_type="RoCE", num_qp=num_qp
    )
    receiver = dlslime.RDMAEndpoint(
        device_name=nics[-1], ib_port=1, link_type="RoCE", num_qp=num_qp
    )

    sender.connect(receiver.endpoint_info())
    receiver.connect(sender.endpoint_info())

    yield sender, receiver

    del sender, receiver


def test_sequential_slot_churn(endpoints):
    """Fire TOTAL_OPS sequential send/recv pairs through a pool of
    DEPTH slots. Each slot is reused roughly ``TOTAL_OPS / DEPTH``
    times. After the refactor every pair must complete with the
    sender's unique payload intact in the receiver's buffer.

    Two failure modes this catches:

    * If ``releaseSendSlot`` / ``releaseRecvSlot`` is not called on
      the final per-qp callback, the free ring drains and the
      (DEPTH+1)-th op hangs forever in ``acquireSendSlot`` /
      ``acquireRecvSlot``. The test would time out.

    * If a stale callback from a previous op were still bound to the
      slot's assigns (the pre-refactor ABA hazard), the reused op
      would see the previous op's completion fire into its
      ``EndpointOpState``, and either hang (if the old op wrote
      completion_status=FAILED) or observe wrong bytes (if the
      callback's ``set_comm_done`` matched an incomplete op). The
      payload check fails either way."""

    sender, receiver = endpoints

    assert TOTAL_OPS > DEPTH, (
        f"TOTAL_OPS={TOTAL_OPS} must exceed SLIME_MAX_MSG_FIFO_DEPTH={DEPTH} "
        f"to actually exercise slot reuse"
    )

    for op_idx in range(TOTAL_OPS):
        send_buf = _payload(op_idx, MSG_BYTES)
        recv_buf = torch.zeros(MSG_BYTES, dtype=torch.uint8)

        # Post the recv BEFORE the send: the meta handshake needs the
        # receiver to have an op queued so its recvProcess can publish
        # meta to the sender.
        recv_fut = receiver.recv((recv_buf.data_ptr(), 0, MSG_BYTES))
        send_fut = sender.send((send_buf.data_ptr(), 0, MSG_BYTES))

        send_fut.wait()
        recv_fut.wait()

        if not torch.equal(recv_buf, send_buf):
            got_tag = int.from_bytes(bytes(recv_buf[:4].tolist()), "little")
            pytest.fail(
                f"op_idx={op_idx}: recv buffer does not match sent payload. "
                f"Slot reuse cross-wired this op: received header says "
                f"op_idx={got_tag} (expected {op_idx}); "
                f"got[:16]={recv_buf[:16].tolist()} "
                f"want[:16]={send_buf[:16].tolist()}"
            )
