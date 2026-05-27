#!/usr/bin/env python3
"""TCP one-sided write through PeerAgent — mirror of p2p_rdma_rc_write.py.

Two PeerAgents in one process. Agent A writes a payload directly into a
memory region published by agent B, no recv() on the receiver side.

Prerequisites:
    1. Start NanoCtrl:   cd NanoCtrl && cargo run --release
    2. Redis must be reachable.
    3. dlslime built with BUILD_TCP=ON (default).

Usage:
    python p2p_tcp_rc_write_peer_agent.py
    python p2p_tcp_rc_write_peer_agent.py --ctrl http://host:4479
"""

import argparse
import ctypes
import time

from dlslime import PeerAgent


def main(ctrl_url: str) -> None:
    agent_a = PeerAgent(ctrl_url=ctrl_url, alias="tcp_write_a")
    agent_b = PeerAgent(ctrl_url=ctrl_url, alias="tcp_write_b")

    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    # Register MRs before connect_to so endpoint_info() carries them when
    # the rendezvous fires — required for one-sided ops on TCP.
    agent_a.register_memory_region("buf_a", addr_a, 0, 64)
    agent_b.register_memory_region("buf_b", addr_b, 0, 64)

    conn_a = agent_a.connect_to("tcp_write_b", transport="tcp")
    agent_b.connect_to("tcp_write_a", transport="tcp")
    conn_a.wait()
    print("Connected over TCP.")

    try:
        ep_a = conn_a.endpoint
        peer_info = conn_a.peer_endpoint_info
        assert peer_info is not None

        h_local = ep_a.register_memory_region("buf_a_loc", addr_a, 0, 64)
        h_remote = ep_a.register_remote_memory_region(
            "buf_b_rem", peer_info["mr_info"]["buf_b"]
        )

        payload = b"one-sided-write-via-peer-agent"
        ctypes.memmove(addr_a, payload, len(payload))

        st = ep_a.write([(h_local, h_remote, 0, 0, len(payload))]).wait()
        assert st == 0, f"write failed: {st}"

        # TCP write completes locally before the bytes land remotely; spin
        # briefly until B observes them.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if bytes(buf_b[: len(payload)]) == payload:
                break
            time.sleep(0.05)
        assert bytes(buf_b[: len(payload)]) == payload, "B did not see the write"
        print(f"A->B one-sided write = {payload!r}  ok")
    finally:
        agent_a.shutdown()
        agent_b.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TCP one-sided write through PeerAgent"
    )
    parser.add_argument("--ctrl", default="http://127.0.0.1:4479", help="NanoCtrl URL")
    main(parser.parse_args().ctrl)
