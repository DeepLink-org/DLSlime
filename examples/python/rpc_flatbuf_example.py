#!/usr/bin/env python3
"""SlimeRPC FlatBuffers (raw mode) loopback test.

This example demonstrates using ``@method(raw=True)`` with FlatBuffers
for zero-copy serialization over RDMA, bypassing pickle entirely.

Prerequisites:
    1. pip install flatbuffers
    2. Start NanoCtrl:   cd NanoCtrl && cargo run --release
    3. Redis must be reachable.

Usage:
    python rpc_flatbuf_example.py
    python rpc_flatbuf_example.py --ctrl http://host:3000
"""

import argparse
import ctypes
import threading

import flatbuffers
from dlslime import PeerAgent
from dlslime.rpc import method, proxy, serve

# ═══ FlatBuffers schema (inline, no .fbs codegen needed) ═════
#
# We use the FlatBuffers raw API to build/parse buffers manually.
# This avoids requiring a code-generation step.
#
# Schema equivalent:
#   table CalcRequest  { a: int; b: int; }
#   table CalcResponse { result: int; }
#   table EchoRequest  { msg: string; }
#   table EchoResponse { msg: string; }


def build_calc_request(a: int, b: int) -> bytes:
    """Build a FlatBuffer with two int32 fields: a, b."""
    builder = flatbuffers.Builder(64)
    # Table: CalcRequest { a:int, b:int }
    builder.StartObject(2)
    builder.PrependInt32Slot(0, a, 0)  # field 0 = a
    builder.PrependInt32Slot(1, b, 0)  # field 1 = b
    req = builder.EndObject()
    builder.Finish(req)
    return bytes(builder.Output())


def parse_calc_request(buf: bytes) -> tuple[int, int]:
    """Parse CalcRequest FlatBuffer → (a, b)."""
    tab = flatbuffers.table.Table(
        buf, flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    )
    # Offset() returns 0 when a field is absent (value equals the default).
    # For int32 the default is 0, so return 0 in that case.
    off_a = tab.Offset(4)
    off_b = tab.Offset(6)
    a = tab.Get(flatbuffers.number_types.Int32Flags, tab.Pos + off_a) if off_a else 0
    b = tab.Get(flatbuffers.number_types.Int32Flags, tab.Pos + off_b) if off_b else 0
    return a, b


def build_int_response(value: int) -> bytes:
    """Build a FlatBuffer with one int32 field: result."""
    builder = flatbuffers.Builder(32)
    builder.StartObject(1)
    builder.PrependInt32Slot(0, value, 0)
    resp = builder.EndObject()
    builder.Finish(resp)
    return bytes(builder.Output())


def parse_int_response(buf: bytes) -> int:
    """Parse single-int response FlatBuffer."""
    tab = flatbuffers.table.Table(
        buf, flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    )
    off = tab.Offset(4)
    return tab.Get(flatbuffers.number_types.Int32Flags, tab.Pos + off) if off else 0


def build_echo_request(msg: str) -> bytes:
    """Build a FlatBuffer with one string field: msg."""
    builder = flatbuffers.Builder(128)
    msg_off = builder.CreateString(msg)
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(0, msg_off, 0)
    req = builder.EndObject()
    builder.Finish(req)
    return bytes(builder.Output())


def parse_echo_request(buf: bytes) -> str:
    """Parse EchoRequest FlatBuffer → msg."""
    tab = flatbuffers.table.Table(
        buf, flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    )
    off = tab.Offset(4)
    if off == 0:
        return ""
    return tab.String(tab.Pos + off).decode("utf-8")


def build_echo_response(msg: str) -> bytes:
    return build_echo_request(msg)  # same layout


def parse_echo_response(buf: bytes) -> str:
    return parse_echo_request(buf)


# ═══ Service definition (raw mode) ══════════════════


class CalcServiceFB:
    """Same CalcService but using FlatBuffers with raw=True."""

    @method(raw=True)
    def add(self, channel, ptr, nbytes):
        buf = bytes((ctypes.c_char * nbytes).from_address(ptr))
        a, b = parse_calc_request(buf)
        return build_int_response(a + b)

    @method(raw=True)
    def echo(self, channel, ptr, nbytes):
        buf = bytes((ctypes.c_char * nbytes).from_address(ptr))
        msg = parse_echo_request(buf)
        return build_echo_response(f"echo: {msg}")

    @method(raw=True)
    def mul(self, channel, ptr, nbytes):
        buf = bytes((ctypes.c_char * nbytes).from_address(ptr))
        a, b = parse_calc_request(buf)
        return build_int_response(a * b)


# ═══ Loopback test ══════════════════════════════════


def main(ctrl_url: str):
    # --- worker agent ---
    worker = PeerAgent(nanoctrl_url=ctrl_url, alias="worker:0")

    # --- driver agent ---
    driver = PeerAgent(nanoctrl_url=ctrl_url, alias="driver:0")
    driver_conn = driver.connect_to("worker:0", ib_port=1, qp_num=1)
    worker_conn = worker.connect_to("driver:0", ib_port=1, qp_num=1)

    # wait for both sides to connect
    driver_conn.wait()
    worker_conn.wait()
    print("Connected.")

    try:
        # serve() blocks — run it in a daemon thread
        t = threading.Thread(
            target=serve,
            args=(worker, CalcServiceFB(), "driver:0"),
            daemon=True,
        )
        t.start()

        w = proxy(driver, "worker:0", CalcServiceFB)

        # ── raw/flatbuf: add ─────────────────────────────
        req = build_calc_request(1, 2)
        resp_bytes = w.add(req).wait()
        assert parse_int_response(resp_bytes) == 3
        print("flatbuf add(1, 2) = 3  ✓")

        # ── raw/flatbuf: mul ─────────────────────────────
        req = build_calc_request(7, 6)
        resp_bytes = w.mul(req).wait()
        assert parse_int_response(resp_bytes) == 42
        print("flatbuf mul(7, 6) = 42  ✓")

        # ── raw/flatbuf: echo ────────────────────────────
        req = build_echo_request("hello flatbuf")
        resp_bytes = w.echo(req).wait()
        assert parse_echo_response(resp_bytes) == "echo: hello flatbuf"
        print("flatbuf echo('hello flatbuf')  ✓")

        # ── batch ────────────────────────────────────────
        results = []
        for i in range(5):
            req = build_calc_request(i, i * 10)
            resp_bytes = w.add(req).wait()
            results.append(parse_int_response(resp_bytes))
        assert results == [0, 11, 22, 33, 44]
        print(f"flatbuf batch add = {results}  ✓")

        print("\nAll FlatBuffers tests passed!")
    finally:
        worker.shutdown()
        driver.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SlimeRPC FlatBuffers test")
    parser.add_argument("--ctrl", default="http://127.0.0.1:3000", help="NanoCtrl URL")
    main(parser.parse_args().ctrl)
