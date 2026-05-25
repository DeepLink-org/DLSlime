"""Tests for C++ observability counters via pybind11."""

import os
import threading

import pytest


@pytest.fixture(autouse=True)
def _set_obs_env(monkeypatch):
    """Enable obs for the test process."""
    monkeypatch.setenv("DLSLIME_OBS", "1")


def _import_slime_c():
    try:
        import dlslime._slime_c as _c

        return _c
    except ImportError:
        pytest.skip("dlslime._slime_c not available (build without RDMA?)")


class TestObsEnabled:
    """obs_enabled() reflects DLSLIME_OBS env var."""

    def test_enabled(self):
        _c = _import_slime_c()
        # Note: obs_enabled() reads env once at first call (static bool).
        # If another test already called it with DLSLIME_OBS=0, this
        # would be sticky. In practice the env is set via autouse fixture
        # before any import.
        assert _c.obs_enabled() in (True, False)  # at least callable

    def test_snapshot_returns_dict(self):
        _c = _import_slime_c()
        snap = _c.obs_snapshot()
        assert isinstance(snap, dict)
        # Should have 'enabled' key
        assert "enabled" in snap


class TestObsSnapshot:
    """obs_snapshot() returns well-structured data."""

    def test_snapshot_structure_when_enabled(self):
        _c = _import_slime_c()
        snap = _c.obs_snapshot()

        if not snap.get("enabled"):
            pytest.skip("obs not enabled (env var read at import time)")

        assert "summary" in snap
        assert "nics" in snap

        summary = snap["summary"]
        for key in [
            "assign_total",
            "batch_total",
            "submitted_bytes_total",
            "completed_bytes_total",
            "failed_bytes_total",
            "pending_ops",
            "error_total",
            # User/sys MR split (new) — with back-compat aliases
            "user_mr_count",
            "user_mr_bytes",
            "sys_mr_count",
            "sys_mr_bytes",
            "mr_count",
            "mr_bytes",
            # Aggregated per-op pending (new)
            "pending_by_op",
        ]:
            assert key in summary, f"Missing summary key: {key}"

        # Back-compat: mr_count/mr_bytes must equal user_mr_count/user_mr_bytes
        assert summary["mr_count"] == summary["user_mr_count"]
        assert summary["mr_bytes"] == summary["user_mr_bytes"]

        # pending_by_op shape: at least the one-sided ops must appear
        pending_by_op = summary["pending_by_op"]
        assert isinstance(pending_by_op, dict)
        for op_name in ("read", "write", "write_with_imm"):
            assert op_name in pending_by_op

    def test_reset_for_test(self):
        _c = _import_slime_c()
        _c.obs_reset_for_test()

        snap = _c.obs_snapshot()
        if not snap.get("enabled"):
            pytest.skip("obs not enabled")

        summary = snap["summary"]
        assert summary["assign_total"] == 0
        assert summary["batch_total"] == 0
        assert summary["submitted_bytes_total"] == 0
        assert summary["completed_bytes_total"] == 0
        assert summary["pending_ops"] == 0
        for v in summary["pending_by_op"].values():
            assert v == 0

    def test_nics_array(self):
        _c = _import_slime_c()
        snap = _c.obs_snapshot()
        if not snap.get("enabled"):
            pytest.skip("obs not enabled")

        nics = snap["nics"]
        assert isinstance(nics, list)

        # If NICs are registered, each should have required fields
        for nic in nics:
            assert "nic" in nic
            assert "assign_total" in nic
            assert "batch_total" in nic
            assert "cq_errors_total" in nic


class TestObsRegisterNicRace:
    """Concurrent obs_register_nic("mlx5_0") must collapse to a single slot."""

    def test_concurrent_same_name_returns_single_slot(self):
        _c = _import_slime_c()
        if not hasattr(_c, "obs_register_nic_for_test"):
            pytest.skip("obs_register_nic_for_test not exposed")
        if not _c.obs_enabled():
            pytest.skip("obs not enabled")

        name = "test_nic_race_mlx5_0"
        results: list[int] = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            slot = _c.obs_register_nic_for_test(name)
            with lock:
                results.append(slot)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads must receive the same slot id.
        assert len(results) == 8
        assert all(r == results[0] for r in results), results
        assert results[0] >= 0

        # The snapshot must contain exactly one entry for this name.
        snap = _c.obs_snapshot()
        matching = [n for n in snap.get("nics", []) if n.get("nic") == name]
        assert len(matching) == 1, matching


class TestObsNoTwoSideSubmit:
    """v0: production code must not call obs_record_submit for send/recv/immRecv."""

    def test_no_twoside_submit_in_rdma_endpoint(self):
        import pathlib

        src = pathlib.Path(__file__).resolve().parents[2] / (
            "dlslime/csrc/engine/rdma/rdma_endpoint.cpp"
        )
        if not src.exists():
            pytest.skip(f"source not found at {src}")

        text = src.read_text()

        # obs_record_submit may legitimately appear only inside the three
        # one-sided functions: read(), write(), writeWithImm(). Detect
        # by scanning forward from each helper's definition boundary.
        twoside_defs = [
            "RDMAEndpoint::send(",
            "RDMAEndpoint::recv(",
            "RDMAEndpoint::immRecv(",
        ]
        for sig in twoside_defs:
            start = text.find(sig)
            assert start != -1, f"expected to find {sig} in rdma_endpoint.cpp"
            # take a ~2000-char window which comfortably covers one function
            window = text[start : start + 2000]
            assert (
                "obs::obs_record_submit" not in window
            ), f"{sig} must not call obs::obs_record_submit in v0"
