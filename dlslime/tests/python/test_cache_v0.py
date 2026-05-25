"""Smoke tests for the DLSlime cache assignment directory."""

import pytest
from dlslime._slime_c import Assignment, cache


def make_server():
    return cache.CacheServer()


def test_default_slab_size_is_256k():
    srv = make_server()
    assert srv.slab_size() == 256 * 1024
    assert srv.memory_size() == 0
    assert srv.num_slabs() == 0
    assert srv.stats().slab_size == 256 * 1024
    assert srv.stats().memory_size == 0


def test_memory_size_configures_fixed_slab_pool():
    srv = cache.CacheServer(slab_size=128 * 1024, memory_size=512 * 1024)

    assert srv.memory_size() == 512 * 1024
    assert srv.num_slabs() == 4
    s = srv.stats()
    assert s.memory_size == 512 * 1024
    assert s.num_slabs == 4
    assert s.used_slabs == 0
    assert s.free_slabs == 4


def test_memory_size_must_align_to_slab_size():
    with pytest.raises(ValueError, match="multiple of slab_size"):
        cache.CacheServer(slab_size=128 * 1024, memory_size=300 * 1024)


def test_slab_size_must_be_in_supported_range():
    with pytest.raises(ValueError, match=r"\[128K, 1G\]"):
        cache.CacheServer(slab_size=128 * 1024 - 1)
    with pytest.raises(ValueError, match=r"\[128K, 1G\]"):
        cache.CacheServer(slab_size=1024**3 + 1)


def test_assignment_store_rejects_when_preallocated_slabs_are_full():
    srv = cache.CacheServer(slab_size=128 * 1024, memory_size=256 * 1024)
    m = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 256 * 1024)])
    assert m.slab_ids == [0, 1]
    assert [a.source_offset for a in m.assignments] == [0, 128 * 1024]

    s = srv.stats()
    assert s.used_slabs == 2
    assert s.free_slabs == 0

    with pytest.raises(RuntimeError, match="not enough preallocated cache slabs"):
        srv.store_assignments("engine-b", [Assignment(1, 2, 0, 0, 1)])

    assert srv.delete_assignments("engine-a", m.version) is True
    assert srv.stats().free_slabs == 2
    reused = srv.store_assignments("engine-b", [Assignment(1, 2, 0, 0, 1)])
    assert reused.slab_ids == [1]


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
    srv = cache.CacheServer(slab_size=slab, memory_size=slab * 4)
    batch = [Assignment(1, 2, 10, 20, slab * 2 + 123)]

    m = srv.store_assignments("engine-a", batch)
    got = srv.query_assignments("engine-a", m.version)

    assert got is not None
    assert got.slab_ids == [0, 1, 2]
    assert [(a.target_offset, a.source_offset, a.length) for a in got.assignments] == [
        (10, 0, slab),
        (10 + slab, slab, slab),
        (10 + 2 * slab, 2 * slab, 123),
    ]


def test_assignment_store_uses_custom_slab_size():
    srv = cache.CacheServer(slab_size=128 * 1024, memory_size=512 * 1024)
    m = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 300 * 1024)])
    got = srv.query_assignments("engine-a", m.version)
    assert got is not None
    assert got.slab_ids == [0, 1, 2]
    assert [a.length for a in got.assignments] == [128 * 1024, 128 * 1024, 44 * 1024]
    assert [a.source_offset for a in got.assignments] == [0, 128 * 1024, 256 * 1024]


def test_assignment_stats_and_delete():
    srv = cache.CacheServer(slab_size=128 * 1024, memory_size=1024 * 1024)
    m0 = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 300 * 1024)])
    m1 = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 64 * 1024)])
    srv.store_assignments("engine-b", [Assignment(3, 4, 0, 0, 32 * 1024)])

    s = srv.stats()
    assert s.num_assignment_peers == 2
    assert s.num_assignment_entries == 3
    assert s.num_assignments == 5  # 300 -> 3 slabs, then 64 and 32
    assert s.assignment_bytes == (300 + 64 + 32) * 1024
    assert s.used_slabs == 5
    assert s.free_slabs == 3

    assert srv.delete_assignments("engine-a", m0.version) is True
    assert srv.delete_assignments("engine-a", m0.version) is False
    assert srv.query_assignments("engine-a", m0.version) is None
    assert srv.query_assignments("engine-a", m1.version) is not None
    assert srv.stats().used_slabs == 2
    assert srv.stats().free_slabs == 6


def test_assignment_store_rejects_empty_peer():
    srv = make_server()
    with pytest.raises(ValueError, match="peer_agent_id"):
        srv.store_assignments("", [Assignment(1, 2, 0, 0, 64)])


def test_clear_drops_everything():
    srv = make_server()
    m = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 64)])
    assert srv.stats().num_assignment_entries == 1
    srv.clear()
    assert srv.stats().num_assignment_entries == 0
    assert srv.query_assignments("engine-a", m.version) is None
    assert srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 64)]).version == 1
