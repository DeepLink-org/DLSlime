#!/usr/bin/env python3
"""SlimeRPC loopback test — two PeerAgents in one process.

Prerequisites:
    1. Start NanoCtrl:   cd NanoCtrl && cargo run --release
    2. Redis must be reachable (NanoCtrl returns its address automatically).

Usage:
    python rpc_example.py                       # default NanoCtrl at localhost:3000
    python rpc_example.py --ctrl http://host:3000
"""

import argparse
import threading

from dlslime import PeerAgent
from dlslime.rpc import method, proxy, serve

# ═══ Service definition ═════════════════════════════


class CalcService:
    @method
    def add(self, a: int, b: int) -> int:
        return a + b

    @method
    def mul(self, a: float, b: float) -> float:
        return a * b

    @method
    def echo(self, msg: str) -> str:
        return f"echo: {msg}"


# ═══ Loopback test ══════════════════════════════════


def main(ctrl_url: str):
    worker = PeerAgent(alias="worker:0", server_url=ctrl_url)
    worker.set_desired_topology(["driver:0"], symmetric=True)

    # --- driver agent ---
    driver = PeerAgent(alias="driver:0", server_url=ctrl_url)
    driver.set_desired_topology(["worker:0"], symmetric=True)

    # wait for both sides to connect
    worker.wait_for_peers(["driver:0"])
    driver.wait_for_peers(["worker:0"])
    print("Connected.")

    try:
        # serve() blocks — run it in a daemon thread
        t = threading.Thread(
            target=serve,
            args=(worker, CalcService(), "driver:0"),
            daemon=True,
        )
        t.start()

        w = proxy(driver, "worker:0", CalcService)

        # ── synchronous ──────────────────────────────────
        assert w.add(1, 2).wait() == 3
        print("add(1, 2) = 3  ✓")

        assert abs(w.mul(3.14, 2.0).wait() - 6.28) < 1e-6
        print("mul(3.14, 2.0) = 6.28  ✓")

        assert w.echo("hello").wait() == "echo: hello"
        print("echo('hello')  ✓")

        # ── batch (sequential request-reply) ─────────────
        results = [w.add(i, i * 10).wait() for i in range(5)]
        assert results == [0, 11, 22, 33, 44]
        print(f"batch add = {results}  ✓")

        print("\nAll tests passed!")
    finally:
        worker.shutdown()
        driver.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SlimeRPC loopback test")
    parser.add_argument("--ctrl", default="http://127.0.0.1:3000", help="NanoCtrl URL")
    main(parser.parse_args().ctrl)
