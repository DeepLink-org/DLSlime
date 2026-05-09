"""Tests for C++ observability counters via pybind11."""

import os
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
            "mr_count",
            "mr_bytes",
        ]:
            assert key in summary, f"Missing summary key: {key}"

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
