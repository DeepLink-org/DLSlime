"""Integration tests for PeerAgent over the TCP transport.

Skipped when NanoCtrl is unreachable so the suite stays green in
RDMA-only or no-control-plane CI environments.
"""

from __future__ import annotations

import ctypes
import os
import threading
import time
from typing import Tuple

import dlslime

import httpx
import pytest
from dlslime import PeerAgent

CTRL_URL = os.environ.get("DLSLIME_TEST_CTRL_URL", "http://127.0.0.1:4479")


def _ctrl_alive() -> bool:
    try:
        with httpx.Client(timeout=1.0) as cli:
            cli.get(CTRL_URL)
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(
        not hasattr(dlslime, "TcpEndpoint"),
        reason="dlslime built without BUILD_TCP",
    ),
    pytest.mark.skipif(
        not _ctrl_alive(),
        reason=f"NanoCtrl not reachable at {CTRL_URL}",
    ),
]


# ── two-thread barrier harness (adapted from tests/python/test_tcp.py) ──


def _sync_run(name: str, fn_a, fn_b, timeout: float = 60.0) -> None:
    err = []
    barrier = threading.Barrier(2)

    def wrap(fn):
        try:
            barrier.wait(10)
            fn()
        except Exception as e:  # noqa: BLE001
            err.append(e)

    ta = threading.Thread(target=wrap, args=(fn_a,), daemon=False)
    tb = threading.Thread(target=wrap, args=(fn_b,), daemon=False)
    ta.start()
    tb.start()
    ta.join(timeout)
    tb.join(timeout)
    if ta.is_alive() or tb.is_alive():
        raise RuntimeError(f"{name}: {timeout}s timeout")
    if err:
        raise err[0]


# ── fixture: two PeerAgents connected over TCP ──


@pytest.fixture()
def tcp_pair(request) -> Tuple[PeerAgent, PeerAgent]:
    # Unique aliases per test so a stale agent record from an earlier flaky
    # run doesn't collide.
    suffix = request.node.name.replace("[", "_").replace("]", "")
    alias_a = f"tcp_a_{suffix}"
    alias_b = f"tcp_b_{suffix}"
    agent_a = PeerAgent(ctrl_url=CTRL_URL, alias=alias_a)
    agent_b = PeerAgent(ctrl_url=CTRL_URL, alias=alias_b)

    yield agent_a, agent_b

    try:
        agent_a.shutdown()
    except Exception:
        pass
    try:
        agent_b.shutdown()
    except Exception:
        pass


def _connect_pair(agent_a: PeerAgent, agent_b: PeerAgent) -> Tuple[object, object]:
    holder = {}

    def run_a():
        conn = agent_a.connect_to(agent_b.alias, transport="tcp")
        conn.wait(timeout=30)
        holder["a"] = conn

    def run_b():
        conn = agent_b.connect_to(agent_a.alias, transport="tcp")
        conn.wait(timeout=30)
        holder["b"] = conn

    _sync_run("connect_pair", run_a, run_b, timeout=60)
    return holder["a"], holder["b"]


# ── tests ──


def test_tcp_send_recv_roundtrip(tcp_pair):
    agent_a, agent_b = tcp_pair
    conn_a, conn_b = _connect_pair(agent_a, agent_b)

    buf_a = ctypes.create_string_buffer(32)
    buf_b = ctypes.create_string_buffer(32)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    ctypes.memmove(addr_a, b"hello", 5)

    def run_a():
        st = agent_a.send(agent_b.alias, (addr_a, 0, 5)).wait()
        assert st == 0
        st = agent_a.recv(agent_b.alias, (addr_a, 5, 5)).wait()
        assert st == 0
        assert bytes(buf_a[5:10]) == b"world"

    def run_b():
        st = agent_b.recv(agent_a.alias, (addr_b, 0, 5)).wait()
        assert st == 0
        assert bytes(buf_b[:5]) == b"hello"
        ctypes.memmove(addr_b, b"world", 5)
        st = agent_b.send(agent_a.alias, (addr_b, 0, 5)).wait()
        assert st == 0

    _sync_run("send_recv_roundtrip", run_a, run_b, timeout=60)


def test_tcp_one_sided_write(tcp_pair):
    agent_a, agent_b = tcp_pair

    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    # Register before connect so endpoint_info()'s mr_info carries the MRs.
    agent_a.register_memory_region("buf_a", addr_a, 0, 64)
    agent_b.register_memory_region("buf_b", addr_b, 0, 64)

    conn_a, _conn_b = _connect_pair(agent_a, agent_b)

    payload = b"one-sided-write!"
    ctypes.memmove(addr_a, payload, len(payload))

    ep_a = conn_a.endpoint
    peer_info = conn_a.peer_endpoint_info
    assert peer_info is not None
    assert "buf_b" in peer_info.get("mr_info", {})

    h_loc = ep_a.register_memory_region("buf_a_loc", addr_a, 0, 64)
    h_rem = ep_a.register_remote_memory_region(
        "buf_b_rem", peer_info["mr_info"]["buf_b"]
    )

    st = ep_a.write([(h_loc, h_rem, 0, 0, len(payload))]).wait()
    assert st == 0

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if bytes(buf_b[: len(payload)]) == payload:
            break
        time.sleep(0.05)
    assert bytes(buf_b[: len(payload)]) == payload


def test_tcp_one_sided_read(tcp_pair):
    agent_a, agent_b = tcp_pair

    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    payload = b"one-sided-read"
    ctypes.memmove(addr_b, payload, len(payload))

    agent_a.register_memory_region("buf_a", addr_a, 0, 64)
    agent_b.register_memory_region("buf_b", addr_b, 0, 64)

    conn_a, _conn_b = _connect_pair(agent_a, agent_b)

    ep_a = conn_a.endpoint
    peer_info = conn_a.peer_endpoint_info
    assert peer_info is not None
    h_loc = ep_a.register_memory_region("buf_a_loc", addr_a, 0, 64)
    h_rem = ep_a.register_remote_memory_region(
        "buf_b_rem", peer_info["mr_info"]["buf_b"]
    )

    st = ep_a.read([(h_loc, h_rem, 0, 0, len(payload))]).wait()
    assert st == 0
    assert bytes(buf_a[: len(payload)]) == payload


def test_tcp_imm_ops_raise(tcp_pair):
    agent_a, agent_b = tcp_pair
    conn_a, _conn_b = _connect_pair(agent_a, agent_b)

    ep_a = conn_a.endpoint
    with pytest.raises(NotImplementedError):
        ep_a.write_with_imm([(0, 0, 0, 0, 0)], imm_data=42)
    with pytest.raises(NotImplementedError):
        ep_a.imm_recv()
