"""Smoke tests for dlslime.cache metadata and assignment directory."""

import pytest
from dlslime._slime_c import Assignment, cache


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


def test_deep_mode_requires_page_pool():
    srv = make_server()
    e = cache.Extent(peer="p", mr_handle=0, offset=0, length=64)
    with pytest.raises(RuntimeError, match="store assignment batches"):
        srv.store("k", [e], mode="deep")


def test_default_slab_size_is_256k():
    srv = make_server()
    assert srv.slab_size() == 256 * 1024
    assert srv.memory_size() == 0
    assert srv.num_slabs() == 0
    assert srv.stats().slab_size == 256 * 1024
    assert srv.stats().memory_size == 0


def test_memory_size_configures_fixed_slab_pool():
    srv = cache.CacheServer(slab_size=128, memory_size=512)

    assert srv.memory_size() == 512
    assert srv.num_slabs() == 4
    s = srv.stats()
    assert s.memory_size == 512
    assert s.num_slabs == 4
    assert s.used_slabs == 0
    assert s.free_slabs == 4


def test_memory_size_must_align_to_slab_size():
    with pytest.raises(ValueError, match="multiple of slab_size"):
        cache.CacheServer(slab_size=128, memory_size=300)


def test_assignment_store_rejects_when_preallocated_slabs_are_full():
    srv = cache.CacheServer(slab_size=128, memory_size=256)
    srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 256)])

    s = srv.stats()
    assert s.used_slabs == 2
    assert s.free_slabs == 0

    with pytest.raises(RuntimeError, match="not enough preallocated cache slabs"):
        srv.store_assignments("engine-b", [Assignment(1, 2, 0, 0, 1)])


def test_assignment_store_query_roundtrip():
    srv = make_server()
    batch = [
        Assignment(11, 22, 0, 1024, 4096),
        Assignment(11, 22, 4096, 8192, 2048),
    ]

    m = srv.store_assignments("engine-a", batch)
    assert m.peer_agent_id == "engine-a"
    assert m.version == 1
    assert m.total_bytes() == 6144

    got = srv.query_assignments("engine-a", m.version)
    assert got is not None
    assert got.peer_agent_id == "engine-a"
    assert got.version == m.version
    assert [
        (a.mr_key, a.remote_mr_key, a.target_offset, a.source_offset, a.length)
        for a in got.assignments
    ] == [
        (11, 22, 0, 1024, 4096),
        (11, 22, 4096, 8192, 2048),
    ]


def test_assignment_query_miss():
    srv = make_server()
    batch = [Assignment(1, 2, 0, 0, 64)]
    m = srv.store_assignments("engine-a", batch)
    assert srv.query_assignments("engine-a", m.version + 1) is None
    assert srv.query_assignments("engine-b", m.version) is None


def test_assignment_store_splits_into_configured_slabs():
    slab = 256 * 1024
    srv = cache.CacheServer(slab_size=slab)
    batch = [Assignment(1, 2, 10, 20, slab * 2 + 123)]

    m = srv.store_assignments("engine-a", batch)
    got = srv.query_assignments("engine-a", m.version)

    assert got is not None
    assert [(a.target_offset, a.source_offset, a.length) for a in got.assignments] == [
        (10, 20, slab),
        (10 + slab, 20 + slab, slab),
        (10 + 2 * slab, 20 + 2 * slab, 123),
    ]


def test_assignment_store_uses_custom_slab_size():
    srv = cache.CacheServer(slab_size=128)
    m = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 300)])
    got = srv.query_assignments("engine-a", m.version)
    assert got is not None
    assert [a.length for a in got.assignments] == [128, 128, 44]


def test_assignment_stats_and_delete():
    srv = cache.CacheServer(slab_size=128)
    m0 = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 300)])
    m1 = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 64)])
    srv.store_assignments("engine-b", [Assignment(3, 4, 0, 0, 32)])

    s = srv.stats()
    assert s.num_assignment_peers == 2
    assert s.num_assignment_entries == 3
    assert s.num_assignments == 5  # 300 -> 3 slabs, then 64 and 32
    assert s.assignment_bytes == 300 + 64 + 32

    assert srv.delete_assignments("engine-a", m0.version) is True
    assert srv.delete_assignments("engine-a", m0.version) is False
    assert srv.query_assignments("engine-a", m0.version) is None
    assert srv.query_assignments("engine-a", m1.version) is not None


def test_assignment_store_rejects_empty_peer():
    srv = make_server()
    with pytest.raises(ValueError, match="peer_agent_id"):
        srv.store_assignments("", [Assignment(1, 2, 0, 0, 64)])


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
    m = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 64)])
    assert srv.stats().num_keys == 2
    assert srv.stats().num_assignment_entries == 1
    srv.clear()
    assert srv.stats().num_keys == 0
    assert srv.stats().num_assignment_entries == 0
    assert srv.query_assignments("engine-a", m.version) is None
    # Version counter resets too — V0 contract; revisit in V1 if useful
    # to expose it but never rely on monotonicity across clear() calls.
    v = srv.store("k0", [e], mode="shallow").version
    assert v == 1
    assert srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 64)]).version == 1
