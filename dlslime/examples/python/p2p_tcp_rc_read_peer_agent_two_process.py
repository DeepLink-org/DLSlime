#!/usr/bin/env python3
"""TCP one-sided read through two PeerAgent processes.

The initiator reads bytes directly out of a memory region published by the
target, then notifies the target so both processes can shut down cleanly.

Prerequisites:
    1. Start NanoCtrl:   cd NanoCtrl && cargo run --release
    2. Redis must be reachable.
    3. dlslime built with BUILD_TCP=ON (default).

Usage:
    python p2p_tcp_rc_read_peer_agent_two_process.py --role target
    python p2p_tcp_rc_read_peer_agent_two_process.py --role initiator
    python p2p_tcp_rc_read_peer_agent_two_process.py --role target --ctrl_address http://host:4479
    python p2p_tcp_rc_read_peer_agent_two_process.py --role initiator --ctrl_address http://host:4479
    python p2p_tcp_rc_read_peer_agent_two_process.py --role target --local_host 10.0.0.2
    python p2p_tcp_rc_read_peer_agent_two_process.py --role initiator --local_host 10.0.0.3
"""

import argparse
import ctypes
from typing import Optional

from dlslime import PeerAgent


INITIATOR_ALIAS = "tcp_read_initiator"
TARGET_ALIAS = "tcp_read_target"
PAYLOAD = b"one-sided-read-via-peer-agent"
DONE = b"done"


def _buffer_from_bytes(data: bytes) -> ctypes.Array:
    buf = ctypes.create_string_buffer(len(data))
    ctypes.memmove(ctypes.addressof(buf), data, len(data))
    return buf


def run_initiator(
    ctrl_url: str, local_host: Optional[str], local_port: int, connect_timeout: float
) -> None:
    agent = PeerAgent(ctrl_url=ctrl_url, alias=INITIATOR_ALIAS)
    try:
        buf_a = ctypes.create_string_buffer(64)
        addr_a = ctypes.addressof(buf_a)

        # Register the local MR before connect_to so endpoint_info() carries it
        # when the rendezvous fires. Required for one-sided ops on TCP.
        agent.register_memory_region("buf_a", addr_a, 0, 64)

        if TARGET_ALIAS not in agent.list_agents():
            raise RuntimeError(f"target agent {TARGET_ALIAS!r} is not running")

        print(f"[initiator] TCP local host: {local_host or 'default'}:{local_port}")
        conn = agent.connect_to(
            TARGET_ALIAS,
            transport="tcp",
            local_host=local_host,
            local_port=local_port,
        )
        conn.wait(timeout=connect_timeout)
        local_info = conn.endpoint.endpoint_info()
        print(
            f"[initiator] Connected over TCP: {INITIATOR_ALIAS} -> {TARGET_ALIAS} "
            f"(local={local_info.get('host')}:{local_info.get('port')})"
        )

        done_buf = _buffer_from_bytes(DONE)

        ep = conn.endpoint
        if not ep.is_connected():
            raise RuntimeError(
                "TCP endpoint is not connected; make sure both processes publish "
                "a peer-reachable --local_host"
            )
        peer_info = conn.peer_endpoint_info
        assert peer_info is not None
        remote_mr_info = peer_info.get("mr_info", {}).get("buf_b")
        if remote_mr_info is None:
            raise RuntimeError(
                f"{TARGET_ALIAS!r} is connected but did not publish buf_b; "
                "start the target process first"
            )

        h_local = ep.register_memory_region("buf_a_loc", addr_a, 0, 64)
        h_remote = ep.register_remote_memory_region("buf_b_rem", remote_mr_info)

        st = ep.read([(h_local, h_remote, 0, 0, len(PAYLOAD))]).wait()
        assert st == 0, f"read failed: {st}"
        assert bytes(buf_a[: len(PAYLOAD)]) == PAYLOAD, "initiator did not read bytes"
        print(
            "[initiator] target->initiator one-sided read = "
            f"{bytes(buf_a[: len(PAYLOAD)])!r}  ok"
        )

        st = agent.send(TARGET_ALIAS, (ctypes.addressof(done_buf), 0, len(DONE))).wait()
        assert st == 0, f"done send failed: {st}"
        print("[initiator] Sent completion notice.")
    finally:
        agent.shutdown()


def run_target(
    ctrl_url: str, local_host: Optional[str], local_port: int, connect_timeout: float
) -> None:
    agent = PeerAgent(ctrl_url=ctrl_url, alias=TARGET_ALIAS)
    try:
        buf_b = ctypes.create_string_buffer(64)
        addr_b = ctypes.addressof(buf_b)

        # Pre-fill the target buffer with the payload the initiator reads out.
        ctypes.memmove(addr_b, PAYLOAD, len(PAYLOAD))

        # Register MRs before connect_to so endpoint_info() carries them when
        # the rendezvous fires. Required for one-sided ops on TCP.
        agent.register_memory_region("buf_b", addr_b, 0, 64)

        print(f"[target] TCP local host: {local_host or 'default'}:{local_port}")
        conn = agent.connect_to(
            INITIATOR_ALIAS,
            transport="tcp",
            local_host=local_host,
            local_port=local_port,
        )
        conn.wait(timeout=connect_timeout)
        local_info = conn.endpoint.endpoint_info()
        print(
            f"[target] Connected over TCP: {TARGET_ALIAS} -> {INITIATOR_ALIAS} "
            f"(local={local_info.get('host')}:{local_info.get('port')})"
        )

        done_buf = ctypes.create_string_buffer(len(DONE))

        if not conn.endpoint.is_connected():
            raise RuntimeError(
                "TCP endpoint is not connected; make sure both processes publish "
                "a peer-reachable --local_host"
            )
        print("[target] Published buffer; waiting for initiator completion.")

        st = agent.recv(
            INITIATOR_ALIAS, (ctypes.addressof(done_buf), 0, len(DONE))
        ).wait()
        assert st == 0, f"done recv failed: {st}"
        assert bytes(done_buf[: len(DONE)]) == DONE, "initiator did not send done"
        print("[target] Initiator completed read; shutting down.")
    finally:
        agent.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TCP one-sided read through two PeerAgent processes"
    )
    parser.add_argument(
        "--role",
        choices=("initiator", "target"),
        required=True,
        help="Process role to run",
    )
    parser.add_argument(
        "--ctrl_address",
        "--ctrl",
        default="http://127.0.0.1:4479",
        help="NanoCtrl URL",
    )
    parser.add_argument(
        "--local_host",
        default=None,
        help="Local TCP host/IP to publish to the peer; defaults to discovered host",
    )
    parser.add_argument(
        "--local_port",
        type=int,
        default=0,
        help="Local TCP port to bind; 0 lets the OS choose",
    )
    parser.add_argument(
        "--connect_timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for the peer-agent TCP connection",
    )
    args = parser.parse_args()
    if args.role == "initiator":
        run_initiator(
            args.ctrl_address, args.local_host, args.local_port, args.connect_timeout
        )
    else:
        run_target(
            args.ctrl_address, args.local_host, args.local_port, args.connect_timeout
        )
