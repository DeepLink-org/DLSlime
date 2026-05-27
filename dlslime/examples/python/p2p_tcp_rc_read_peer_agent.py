#!/usr/bin/env python3
"""TCP one-sided read through PeerAgent — mirror of p2p_rdma_rc_read.py.

Two PeerAgents in one process. Agent A reads bytes directly out of a
memory region published by agent B, no send() on the source side.

Prerequisites:
    1. Start NanoCtrl:   cd NanoCtrl && cargo run --release
    2. Redis must be reachable.
    3. dlslime built with BUILD_TCP=ON (default).

Usage:
    python p2p_tcp_rc_read_peer_agent.py
    python p2p_tcp_rc_read_peer_agent.py --ctrl http://host:4479
"""

import argparse
import ctypes

from dlslime import PeerAgent


def main(ctrl_url: str) -> None:
    agent_a = PeerAgent(ctrl_url=ctrl_url, alias="tcp_read_a")
    agent_b = PeerAgent(ctrl_url=ctrl_url, alias="tcp_read_b")

    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    # Pre-fill B with the payload A is about to read out.
    payload = b"one-sided-read-via-peer-agent"
    ctypes.memmove(addr_b, payload, len(payload))

    # Register MRs before connect_to so endpoint_info() carries them when
    # the rendezvous fires — required for one-sided ops on TCP.
    agent_a.register_memory_region("buf_a", addr_a, 0, 64)
    agent_b.register_memory_region("buf_b", addr_b, 0, 64)

    conn_a = agent_a.connect_to("tcp_read_b", transport="tcp")
    agent_b.connect_to("tcp_read_a", transport="tcp")
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

        st = ep_a.read([(h_local, h_remote, 0, 0, len(payload))]).wait()
        assert st == 0, f"read failed: {st}"
        assert bytes(buf_a[: len(payload)]) == payload, "A did not receive the bytes"
        print(f"A<-B one-sided read = {bytes(buf_a[: len(payload)])!r}  ok")
    finally:
        agent_a.shutdown()
        agent_b.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TCP one-sided read through PeerAgent")
    parser.add_argument("--ctrl", default="http://127.0.0.1:4479", help="NanoCtrl URL")
    main(parser.parse_args().ctrl)
