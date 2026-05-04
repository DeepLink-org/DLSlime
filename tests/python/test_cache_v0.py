"""V0 smoke test for dlslime.cache (in-process, shallow-only)."""

import pytest
from dlslime._slime_c import cache


def make_server():
    return cache.CacheServer()


def test_store_load_roundtrip():
    srv = make_server()
    extents = [
        cache.Extent(peer="peer-a", mr_handle=1, offset=0, length=4096),
        cache.Extent(peer="peer-a", mr_handle=1, offset=4096, length=4096),
    ]
    m = srv.store("k", extents, mode="shallow")
    assert m.mode == "shallow"
    assert m.version == 1
    assert m.total_bytes() == 8192
    assert len(m.extents) == 2

    got = srv.load("k")
    assert got is not None
    assert got.version == m.version
    assert got.mode == "shallow"
    assert got.total_bytes() == 8192
    assert [(e.peer, e.mr_handle, e.offset, e.length) for e in got.extents] == [
        ("peer-a", 1, 0, 4096),
        ("peer-a", 1, 4096, 4096),
    ]


def test_load_miss():
    srv = make_server()
    assert srv.load("nope") is None


def test_restore_bumps_version():
    srv = make_server()
    e = cache.Extent(peer="p", mr_handle=0, offset=0, length=64)
    v1 = srv.store("k", [e], mode="shallow").version
    v2 = srv.store("k", [e], mode="shallow").version
    assert v2 > v1


def test_delete_returns_existed_flag():
    srv = make_server()
    e = cache.Extent(peer="p", mr_handle=0, offset=0, length=64)
    srv.store("k", [e], mode="shallow")
    assert srv.delete("k") is True
    assert srv.delete("k") is False  # second delete: key already gone
    assert srv.load("k") is None


def test_stats_aggregate_across_keys():
    srv = make_server()
    e_a = cache.Extent(peer="a", mr_handle=0, offset=0, length=128)
    e_b = cache.Extent(peer="b", mr_handle=0, offset=0, length=256)
    srv.store("k0", [e_a, e_a], mode="shallow")
    srv.store("k1", [e_b], mode="shallow")
    s = srv.stats()
    assert s.num_keys == 2
    assert s.num_shallow_keys == 2
    assert s.num_deep_keys == 0
    assert s.num_extents == 3
    assert s.bytes_addressed == 128 * 2 + 256


def test_deep_mode_rejected_in_v0():
    srv = make_server()
    e = cache.Extent(peer="p", mr_handle=0, offset=0, length=64)
    with pytest.raises(RuntimeError, match="deep not implemented"):
        srv.store("k", [e], mode="deep")


def test_unknown_mode_rejected():
    srv = make_server()
    e = cache.Extent(peer="p", mr_handle=0, offset=0, length=64)
    with pytest.raises(ValueError, match="must be 'deep' or 'shallow'"):
        srv.store("k", [e], mode="other")


def test_clear_drops_everything():
    srv = make_server()
    e = cache.Extent(peer="p", mr_handle=0, offset=0, length=64)
    srv.store("k0", [e], mode="shallow")
    srv.store("k1", [e], mode="shallow")
    assert srv.stats().num_keys == 2
    srv.clear()
    assert srv.stats().num_keys == 0
    # Version counter resets too — V0 contract; revisit in V1 if useful
    # to expose it but never rely on monotonicity across clear() calls.
    v = srv.store("k0", [e], mode="shallow").version
    assert v == 1
