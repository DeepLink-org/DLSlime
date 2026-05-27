#!/usr/bin/env python3
"""Raw TcpEndpoint send/recv — no PeerAgent, no NanoCtrl, no Redis.

The TCP transport is symmetric: both sides bind a port, exchange the
endpoint_info JSON out-of-band (here: just a Python dict in the same
process), and call connect() in their own thread before posting I/O.

Usage:
    python p2p_tcp_rc_send_recv.py
"""

import ctypes
import threading
import time

from dlslime import TcpEndpoint


PAYLOAD_AB = b"hello-from-a"
PAYLOAD_BA = b"hello-from-b"


def main() -> None:
    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)
    addr_a = ctypes.addressof(buf_a)
    addr_b = ctypes.addressof(buf_b)

    ep_a = TcpEndpoint(ip="0.0.0.0", port=0)
    ep_b = TcpEndpoint(ip="0.0.0.0", port=0)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    print(f"endpoint A bound at {info_a['host']}:{info_a['port']}")
    print(f"endpoint B bound at {info_b['host']}:{info_b['port']}")

    err = []
    barrier = threading.Barrier(2)

    def run_a() -> None:
        try:
            barrier.wait(5)
            ep_a.connect(info_b)
            ctypes.memmove(addr_a, PAYLOAD_AB, len(PAYLOAD_AB))
            # Small stagger so B has its async_recv posted before our send.
            time.sleep(0.5)
            assert ep_a.send((addr_a, 0, len(PAYLOAD_AB))).wait() == 0
            assert ep_a.recv((addr_a, len(PAYLOAD_AB), len(PAYLOAD_BA))).wait() == 0
            received = bytes(buf_a[len(PAYLOAD_AB) : len(PAYLOAD_AB) + len(PAYLOAD_BA)])
            assert received == PAYLOAD_BA, received
        except Exception as e:  # noqa: BLE001
            err.append(("a", e))

    def run_b() -> None:
        try:
            barrier.wait(5)
            ep_b.connect(info_a)
            assert ep_b.recv((addr_b, 0, len(PAYLOAD_AB))).wait() == 0
            received = bytes(buf_b[: len(PAYLOAD_AB)])
            assert received == PAYLOAD_AB, received
            ctypes.memmove(addr_b, PAYLOAD_BA, len(PAYLOAD_BA))
            time.sleep(0.5)
            assert ep_b.send((addr_b, 0, len(PAYLOAD_BA))).wait() == 0
        except Exception as e:  # noqa: BLE001
            err.append(("b", e))

    ta = threading.Thread(target=run_a, daemon=False)
    tb = threading.Thread(target=run_b, daemon=False)
    ta.start()
    tb.start()
    ta.join(timeout=30)
    tb.join(timeout=30)

    try:
        ep_a.shutdown()
    except Exception:
        pass
    try:
        ep_b.shutdown()
    except Exception:
        pass

    if err:
        raise RuntimeError(f"raw TCP example failed: {err}")
    print(f"A->B = {PAYLOAD_AB!r}  ok")
    print(f"B->A = {PAYLOAD_BA!r}  ok")


if __name__ == "__main__":
    main()
