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
    """Minimal echo service: receives raw bytes and sends them back."""

    @method(raw=True)
    def echo(self, channel, ptr, nbytes):
        return bytes((ctypes.c_char * nbytes).from_address(ptr))


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
    args = parser.parse_args()

    worker = PeerAgent(alias="bench-worker", server_url=args.ctrl, scope=args.scope)
    worker._rpc_buffer_size = args.buf_mb * 1024 * 1024

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
