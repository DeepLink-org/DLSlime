import ctypes

import pytest

from dlslime import discover_topology, RDMAContext, RDMAMemoryPool


def _first_rdma_resource():
    try:
        resource = discover_topology(None, 1, None)
    except RuntimeError as exc:
        pytest.skip(f"RDMA topology is unavailable: {exc}")

    for nic in resource.get("nics") or []:
        if nic.get("health", "AVAILABLE") == "UNAVAILABLE":
            continue
        for port in nic.get("ports") or []:
            state = str(port.get("state", "ACTIVE")).upper()
            if state not in {"ACTIVE", "UNKNOWN"}:
                continue
            link_type = str(port.get("link_type") or "RoCE")
            if link_type.upper() == "UNKNOWN":
                continue
            return str(nic["name"]), int(port.get("port", 1)), link_type

    pytest.skip("No usable RDMA resource found")


def _memory_pool_or_skip():
    device, ib_port, link_type = _first_rdma_resource()
    ctx = RDMAContext()
    rc = ctx.init(device, ib_port, link_type)
    if rc != 0:
        pytest.skip(
            f"RDMAContext.init failed for {device}:{ib_port}:{link_type} with rc={rc}"
        )
    return RDMAMemoryPool(ctx)


def test_rdma_memory_pool_unregister_by_handle_and_name():
    pool = _memory_pool_or_skip()
    buffer = (ctypes.c_char * 4096)()
    ptr = ctypes.addressof(buffer)

    handle = pool.register_memory_region(ptr, len(buffer), "kv")
    assert handle >= 0
    assert pool.get_handle("kv") == handle
    assert "kv" in pool.mr_info()

    assert pool.unregister_memory_region(handle) == 0
    assert pool.get_handle("kv") == -1
    assert "kv" not in (pool.mr_info() or {})
    assert pool.unregister_memory_region(handle) != 0

    name_handle = pool.register_memory_region(ptr, len(buffer), "kv-by-name")
    assert name_handle >= 0
    assert pool.get_handle("kv-by-name") == name_handle

    assert pool.unregister_memory_region("kv-by-name") == 0
    assert pool.get_handle("kv-by-name") == -1
    assert "kv-by-name" not in (pool.mr_info() or {})
    assert pool.unregister_memory_region("kv-by-name") != 0
