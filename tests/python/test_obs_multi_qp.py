"""Multi-QP observability correctness test.

Drives a one-sided RDMA write across `num_qp > 1` through two real
PeerAgents and asserts the obs counters behave per the v0 contract:

  * `completed_bytes_total` == bytes written (NOT num_qp * bytes)
  * `pending_ops` returns to 0
  * `pending_by_op["write"]` returns to 0
  * `pending_ops` never observed negative at any sample point

This is the guard for the P0 fix in this PR that moved semantic
completion accounting from per-CQE (over-counted by num_qp) to
per-EndpointOpState with an exchange-guarded once-only record.

Skips cleanly when NanoCtrl / Redis / RDMA NICs are not available.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest


NUM_QP = 4
BUFFER_BYTES = 64 * 1024  # 64 KiB is enough to exercise all 4 QPs


def _need_env():
    # Obs must be on for counters to update at all.
    if os.environ.get("DLSLIME_OBS", "") in ("", "0", "false"):
        pytest.skip("DLSLIME_OBS not enabled; re-run with DLSLIME_OBS=1")


def _need_torch():
    try:
        import torch  # noqa: F401

        return torch
    except ImportError:
        pytest.skip("torch not available")


def _need_slime_c():
    try:
        import dlslime._slime_c as _c  # type: ignore

        return _c
    except ImportError:
        pytest.skip("dlslime._slime_c not available")


def _need_nanoctrl(url: str) -> None:
    """Skip if NanoCtrl isn't reachable — the two-PeerAgent dance needs it."""
    try:
        import httpx

        httpx.get(url, timeout=1.0)
    except Exception as exc:
        pytest.skip(f"NanoCtrl not reachable at {url}: {exc}")


@pytest.fixture(autouse=True)
def _enable_obs(monkeypatch):
    monkeypatch.setenv("DLSLIME_OBS", "1")


def test_multi_qp_completion_is_counted_once():
    """One user-visible write() with num_qp=4 must decrement pending_ops
    exactly once and credit `completed_bytes_total` exactly once — not
    num_qp times.
    """
    _need_env()
    torch = _need_torch()
    _c = _need_slime_c()

    if not _c.obs_enabled():
        pytest.skip("obs_enabled() is False; env var was not set at import")
    if not getattr(_c, "_BUILD_RDMA", False):
        pytest.skip("build does not include RDMA")

    nanoctrl_url = os.environ.get("NANOCTRL_URL", "http://127.0.0.1:3000")
    _need_nanoctrl(nanoctrl_url)

    try:
        from dlslime import start_peer_agent
    except ImportError:
        pytest.skip("dlslime.start_peer_agent not importable")

    # Unique scope per test run so we don't collide with other live agents.
    scope = f"test-obs-multi-qp-{uuid.uuid4().hex[:8]}"

    # Baseline the counters. obs_reset_for_test() zeros peer + per-NIC atomics.
    _c.obs_reset_for_test()
    baseline = _c.obs_snapshot()["summary"]
    assert baseline["pending_ops"] == 0
    assert baseline["completed_bytes_total"] == 0

    initiator = None
    target = None
    try:
        try:
            initiator = start_peer_agent(
                nanoctrl_url=nanoctrl_url,
                alias=f"obs-init-{uuid.uuid4().hex[:6]}",
                scope=scope,
            )
            target = start_peer_agent(
                nanoctrl_url=nanoctrl_url,
                alias=f"obs-tgt-{uuid.uuid4().hex[:6]}",
                scope=scope,
            )
        except Exception as exc:
            pytest.skip(f"PeerAgent start failed (likely no RDMA NIC): {exc}")

        # num_qp=NUM_QP on BOTH sides — symmetric, otherwise connect
        # reconciliation will refuse.
        try:
            c1 = initiator.connect_to(target.alias, ib_port=1, qp_num=NUM_QP)
            c2 = target.connect_to(initiator.alias, ib_port=1, qp_num=NUM_QP)
            c1.wait()
            c2.wait()
        except Exception as exc:
            pytest.skip(f"peer connect failed (no RDMA or wrong port): {exc}")

        # Register a local MR on each side with the **same name** —
        # agent.write(peer, [(name, ...)]) looks up `name` locally (as
        # the source MR) and on the peer (as the destination MR).
        src = torch.zeros([BUFFER_BYTES], dtype=torch.uint8, device="cpu")
        dst = torch.zeros([BUFFER_BYTES], dtype=torch.uint8, device="cpu")
        src.fill_(0xAB)

        initiator.register_memory_region(
            "kv",
            src.data_ptr(),
            int(src.storage_offset()),
            src.numel() * src.itemsize,
        )
        target.register_memory_region(
            "kv",
            dst.data_ptr(),
            int(dst.storage_offset()),
            dst.numel() * dst.itemsize,
        )

        # Issue a single user-visible write. The transfer is
        # striped across NUM_QP QPs under the hood — that's the
        # condition under which the original PR over-counted.
        fut = initiator.write(
            target.alias,
            [("kv", 0, 0, BUFFER_BYTES)],
        )
        fut.wait()

        # Give the (asynchronous) free-slot release a chance to fire the
        # final qpi callback, in case wait() returned before the last
        # slot_qp_mask or after all four but before the exchange record.
        # The accounting is synchronous inside the callback, but scheduling
        # can still race the snapshot by microseconds.
        for _ in range(50):
            snap = _c.obs_snapshot()["summary"]
            if snap["pending_ops"] == 0 and snap["completed_bytes_total"] > 0:
                break
            time.sleep(0.01)

        # Sample the final state.
        summary = _c.obs_snapshot()["summary"]

        # The smoking gun: bytes must equal the single user write, not
        # num_qp * write. The pre-fix code did NUM_QP * BUFFER_BYTES here.
        assert summary["completed_bytes_total"] == BUFFER_BYTES, (
            f"completed_bytes_total should be exactly {BUFFER_BYTES}, "
            f"got {summary['completed_bytes_total']} "
            f"(num_qp={NUM_QP}, num_qp*bytes={NUM_QP * BUFFER_BYTES})"
        )

        # Semantic pending must return to zero.
        assert (
            summary["pending_ops"] == 0
        ), f"pending_ops leaked; got {summary['pending_ops']} after one write"

        # pending_by_op breakdown must agree: no write is in-flight.
        assert summary["pending_by_op"]["write"] == 0, (
            f"pending_by_op['write'] leaked; got "
            f"{summary['pending_by_op']['write']}"
        )

        # submit counter should have incremented by exactly one batch.
        # (submit increments batch_total by 1 per user op)
        assert summary["batch_total"] >= 1
        assert summary["submitted_bytes_total"] == BUFFER_BYTES, (
            f"submitted_bytes_total = {summary['submitted_bytes_total']}, "
            f"expected exactly {BUFFER_BYTES}"
        )

        # No failures.
        assert summary["failed_bytes_total"] == 0
        assert summary["error_total"] == 0

    finally:
        # Tear down explicitly. shutdown() also emits the final
        # "stopped" snapshot via the ObsReporter.
        for agent in (initiator, target):
            if agent is not None:
                try:
                    agent.shutdown()
                except Exception:
                    pass


def test_multi_qp_pending_never_negative_across_writes():
    """Over a small burst of writes with num_qp=4, pending_ops sampled
    between submit and wait must always be non-negative. This is the
    other failure mode of the original bug: multi-QP callbacks fired
    fetch_sub(1) per CQE, driving pending_ops below zero.
    """
    _need_env()
    torch = _need_torch()
    _c = _need_slime_c()

    if not _c.obs_enabled():
        pytest.skip("obs_enabled() is False")
    if not getattr(_c, "_BUILD_RDMA", False):
        pytest.skip("build does not include RDMA")

    nanoctrl_url = os.environ.get("NANOCTRL_URL", "http://127.0.0.1:3000")
    _need_nanoctrl(nanoctrl_url)

    try:
        from dlslime import start_peer_agent
    except ImportError:
        pytest.skip("dlslime.start_peer_agent not importable")

    scope = f"test-obs-multi-qp-burst-{uuid.uuid4().hex[:8]}"
    _c.obs_reset_for_test()

    initiator = None
    target = None
    try:
        try:
            initiator = start_peer_agent(
                nanoctrl_url=nanoctrl_url,
                alias=f"obs-burst-init-{uuid.uuid4().hex[:6]}",
                scope=scope,
            )
            target = start_peer_agent(
                nanoctrl_url=nanoctrl_url,
                alias=f"obs-burst-tgt-{uuid.uuid4().hex[:6]}",
                scope=scope,
            )
            c1 = initiator.connect_to(target.alias, ib_port=1, qp_num=NUM_QP)
            c2 = target.connect_to(initiator.alias, ib_port=1, qp_num=NUM_QP)
            c1.wait()
            c2.wait()
        except Exception as exc:
            pytest.skip(f"RDMA setup failed: {exc}")

        src = torch.zeros([BUFFER_BYTES], dtype=torch.uint8, device="cpu")
        dst = torch.zeros([BUFFER_BYTES], dtype=torch.uint8, device="cpu")
        src.fill_(0xCD)

        initiator.register_memory_region(
            "kv",
            src.data_ptr(),
            int(src.storage_offset()),
            src.numel() * src.itemsize,
        )
        target.register_memory_region(
            "kv",
            dst.data_ptr(),
            int(dst.storage_offset()),
            dst.numel() * dst.itemsize,
        )

        NUM_WRITES = 8
        futures = []
        pending_samples = []

        for _ in range(NUM_WRITES):
            fut = initiator.write(target.alias, [("kv", 0, 0, BUFFER_BYTES)])
            futures.append(fut)
            # Sample mid-burst so we catch pending_ops while in-flight.
            pending_samples.append(_c.obs_snapshot()["summary"]["pending_ops"])

        for fut in futures:
            fut.wait()

        for _ in range(100):
            snap = _c.obs_snapshot()["summary"]
            if snap["pending_ops"] == 0:
                break
            time.sleep(0.01)

        summary = _c.obs_snapshot()["summary"]

        # The floor: no QP callback may drive pending below zero.
        assert (
            min(pending_samples) >= 0
        ), f"pending_ops went negative during burst: samples={pending_samples}"

        # After the whole burst settles, everything returns to zero.
        assert summary["pending_ops"] == 0, (
            f"pending_ops leaked after {NUM_WRITES} writes: "
            f"got {summary['pending_ops']}"
        )
        assert summary["pending_by_op"]["write"] == 0

        # Bytes: exactly NUM_WRITES * BUFFER_BYTES, not
        # NUM_QP * NUM_WRITES * BUFFER_BYTES.
        assert summary["completed_bytes_total"] == NUM_WRITES * BUFFER_BYTES, (
            f"completed_bytes_total = {summary['completed_bytes_total']}, "
            f"expected {NUM_WRITES * BUFFER_BYTES} "
            f"(NOT {NUM_QP * NUM_WRITES * BUFFER_BYTES})"
        )

    finally:
        for agent in (initiator, target):
            if agent is not None:
                try:
                    agent.shutdown()
                except Exception:
                    pass
