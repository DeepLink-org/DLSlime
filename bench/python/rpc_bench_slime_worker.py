#!/usr/bin/env python3
"""SlimeRPC benchmark — worker (server) side.

Start this first, then run rpc_bench_slime_driver.py in another terminal.

Usage:
    python rpc_bench_slime_worker.py [--ctrl http://127.0.0.1:3000] [--buf-mb 256]
"""

import argparse
import ctypes
import traceback

from dlslime import PeerAgent
from dlslime.rpc import method, serve


class EchoService:
    """Minimal echo service: receives raw bytes and sends them back.

    Uses ``raw=True, inplace=True`` on the server because that's where
    zero-copy actually matters: the inplace handler memmoves recv slot
    → send buffer in one CPU pass, vs the bytes path which costs a
    16 MB ``bytes(...)`` alloc + a second memcpy in ``post_reply``
    (~+1.2 ms at 16 MB). The driver side stays on plain bytes —
    client-side inplace is a wash for this echo workload.

    The two sides can disagree on ``inplace``: the flag is a local API
    choice (writer callback vs bytes argument), not a wire-level field.
    """

    @method(raw=True, inplace=True)
    def echo(self, req_ptr, req_nbytes, resp_ptr, resp_cap):
        if req_nbytes > resp_cap:
            raise RuntimeError(
                f"Reply {req_nbytes} B exceeds slot capacity {resp_cap} B"
            )
        ctypes.memmove(resp_ptr, req_ptr, req_nbytes)
        return req_nbytes


def main():
    parser = argparse.ArgumentParser(description="SlimeRPC benchmark worker")
    parser.add_argument("--ctrl", default="http://127.0.0.1:3000")
    parser.add_argument("--scope", default="rpc-bench")
    parser.add_argument(
        "--buf-mb",
        type=int,
        default=256,
        help="RPC channel buffer size in MB (must match driver)",
    )
    parser.add_argument(
        "--max-inflight",
        type=int,
        default=1,
        help=(
            "Number of mailbox slots. Default 1 matches the driver's "
            "no-pump single-flight setting; raise on both sides to "
            "enable the pump-backed runtime."
        ),
    )
    args = parser.parse_args()

    worker = PeerAgent(alias="bench-worker", server_url=args.ctrl, scope=args.scope)
    worker._rpc_buffer_size = args.buf_mb * 1024 * 1024
    # Bench uses single-flight RPCs; cap max_inflight so we don't pin
    # buf_mb * SLIME_RPC_MAX_INFLIGHT bytes of CPU memory needlessly.
    worker._rpc_max_inflight = max(1, int(getattr(args, "max_inflight", 4)))

    worker.set_desired_topology(["bench-driver"], symmetric=True)
    print("Worker ready, waiting for driver to connect…")
    worker.wait_for_peers(["bench-driver"], timeout_sec=120)
    print("Connected. Serving (Ctrl-C to stop).")

    try:
        serve(worker, EchoService(), "bench-driver")
        print(
            "Worker serve loop exited normally (peer disconnected or channel closed)."
        )
    except KeyboardInterrupt:
        pass
    except Exception:
        print("Worker serve loop crashed:")
        traceback.print_exc()
        raise
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
