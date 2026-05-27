#!/usr/bin/env python3
"""TCP peer-agent loopback — two PeerAgents in one process over TCP.

Prerequisites:
    1. Start NanoCtrl:   cd NanoCtrl && cargo run --release
    2. Redis must be reachable (NanoCtrl returns its address automatically).
    3. dlslime built with BUILD_TCP=ON (the default).

Usage:
    python p2p_tcp_send_recv_peer_agent.py                       # default NanoCtrl
    python p2p_tcp_send_recv_peer_agent.py --ctrl http://host:4479
"""

import argparse
import ctypes
import threading
import time

from dlslime import PeerAgent


PAYLOAD_AB = b"hello-from-a"
PAYLOAD_BA = b"hello-from-b"


def main(ctrl_url: str) -> None:
    agent_a = PeerAgent(ctrl_url=ctrl_url, alias="tcp_a")
    agent_b = PeerAgent(ctrl_url=ctrl_url, alias="tcp_b")

    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    # Register MRs before connect_to so endpoint_info() carries them when the
    # rendezvous fires. Required for one-sided write/read below.
    agent_a.register_memory_region("buf_a", addr_a, 0, 64)
    agent_b.register_memory_region("buf_b", addr_b, 0, 64)

    conn_a = agent_a.connect_to("tcp_b", transport="tcp")
    conn_b = agent_b.connect_to("tcp_a", transport="tcp")
    conn_a.wait()
    conn_b.wait()
    print("Connected over TCP.")

    try:
        # ── Two-sided send/recv: A sends, B receives. ─────────────────
        ctypes.memmove(addr_a, PAYLOAD_AB, len(PAYLOAD_AB))

        def post_recv() -> None:
            fut = agent_b.recv("tcp_a", (addr_b, 0, len(PAYLOAD_AB)))
            assert fut.wait() == 0
            assert bytes(buf_b[: len(PAYLOAD_AB)]) == PAYLOAD_AB

        t = threading.Thread(target=post_recv, daemon=True)
        t.start()
        send_fut = agent_a.send("tcp_b", (addr_a, 0, len(PAYLOAD_AB)))
        assert send_fut.wait() == 0
        t.join(timeout=10)
        print(f"A->B send/recv = {PAYLOAD_AB!r}  ok")

        # ── One-sided write: A writes into B's MR via the TCP endpoint.
        # TCP MR handles are per-endpoint, so we go through conn_a.endpoint
        # (a TcpEndpoint) directly.
        ep_a = conn_a.endpoint
        peer_info = conn_a.peer_endpoint_info  # set by mailbox post-handshake
        assert peer_info is not None, "peer_endpoint_info missing after handshake"
        h_local_a = ep_a.register_memory_region("buf_a_loc", addr_a, 0, 64)
        h_remote_b = ep_a.register_remote_memory_region(
            "buf_b_remote", peer_info["mr_info"]["buf_b"]
        )

        ctypes.memmove(buf_b, b"\x00" * 64, 64)  # clear B
        ctypes.memmove(addr_a, PAYLOAD_BA, len(PAYLOAD_BA))
        write_fut = ep_a.write([(h_local_a, h_remote_b, 0, 0, len(PAYLOAD_BA))])
        assert write_fut.wait() == 0
        for _ in range(40):
            if bytes(buf_b[: len(PAYLOAD_BA)]) == PAYLOAD_BA:
                break
            time.sleep(0.05)
        assert bytes(buf_b[: len(PAYLOAD_BA)]) == PAYLOAD_BA
        print(f"A->B one-sided write = {PAYLOAD_BA!r}  ok")

        print("\nAll TCP peer-agent ops passed.")
    finally:
        agent_a.shutdown()
        agent_b.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TCP peer-agent send/recv + one-sided write loopback"
    )
    parser.add_argument("--ctrl", default="http://127.0.0.1:4479", help="NanoCtrl URL")
    main(parser.parse_args().ctrl)
