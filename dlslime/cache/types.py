"""JSON helpers for DLSlimeCache assignment manifests."""

from __future__ import annotations

from typing import Any

from dlslime._slime_c import Assignment, cache as _cache


def assignment_from_json(item: dict[str, Any]) -> Assignment:
    return Assignment(
        int(item["mr_key"]),
        int(item["remote_mr_key"]),
        int(item["target_offset"]),
        int(item["source_offset"]),
        int(item["length"]),
    )


def assignment_to_json(a: Assignment) -> dict[str, int]:
    return {
        "mr_key": int(a.mr_key),
        "remote_mr_key": int(a.remote_mr_key),
        "target_offset": int(a.target_offset),
        "source_offset": int(a.source_offset),
        "length": int(a.length),
    }


def assignment_to_tuple(a: Assignment) -> tuple[int, int, int, int, int]:
    return (
        int(a.mr_key),
        int(a.remote_mr_key),
        int(a.target_offset),
        int(a.source_offset),
        int(a.length),
    )


def manifest_to_json(m: _cache.AssignmentManifest) -> dict[str, Any]:
    return {
        "peer_agent_id": m.peer_agent_id,
        "version": int(m.version),
        "total_bytes": int(m.total_bytes()),
        "assignments": [assignment_to_json(a) for a in m.assignments],
    }


def stats_to_json(s: _cache.CacheStats) -> dict[str, int]:
    return {
        "slab_size": int(s.slab_size),
        "memory_size": int(s.memory_size),
        "num_slabs": int(s.num_slabs),
        "used_slabs": int(s.used_slabs),
        "free_slabs": int(s.free_slabs),
        "num_assignment_peers": int(s.num_assignment_peers),
        "num_assignment_entries": int(s.num_assignment_entries),
        "num_assignments": int(s.num_assignments),
        "assignment_bytes": int(s.assignment_bytes),
    }
