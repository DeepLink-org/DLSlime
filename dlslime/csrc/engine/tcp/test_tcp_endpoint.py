"""End-to-end test for TcpEndpoint v3 async primitives with timeout.

Usage:
    LD_LIBRARY_PATH=dlslime PYTHONPATH=. DLSLIME_LOG_LEVEL=0 python3 \
        dlslime/csrc/engine/tcp/test_tcp_endpoint.py
"""

import ctypes
import threading
import time

from dlslime import TcpEndpoint, TcpMemoryPool


def _sync_run(fn_a, fn_b):
    b = threading.Barrier(2)
    ta = threading.Thread(target=lambda: (b.wait(), fn_a()), daemon=True)
    tb = threading.Thread(target=lambda: (b.wait(), fn_b()), daemon=True)
    ta.start(); tb.start()
    ta.join(); tb.join()


def test_async_send_recv():
    """Two endpoints async_send/async_recv each other."""
    print("=== test_async_send_recv ===")

    buf_a = ctypes.create_string_buffer(4096)
    buf_b = ctypes.create_string_buffer(4096)

    ep_a = TcpEndpoint(10001)
    ep_b = TcpEndpoint(10002)
    h_a = ep_a.register_memory_region("a", ctypes.addressof(buf_a), 0, 4096)
    h_b = ep_b.register_memory_region("b", ctypes.addressof(buf_b), 0, 4096)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()

    def run_a():
        ep_a.connect(info_b)
        print("  A connected")
        ctypes.memmove(ctypes.addressof(buf_a), b"hello", 5)
        st = ep_a.async_send((h_a, 0, 5)).wait()
        assert st == 0, f"send failed: {st}"
        print("  A sent 5 bytes")
        st = ep_a.async_recv((h_a, 0, 5)).wait()
        assert st == 0, f"recv failed: {st}"
        assert bytes(buf_a[:5]) == b"world"
        print("  A recv'd: world")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        print("  B connected")
        st = ep_b.async_recv((h_b, 0, 5)).wait()
        assert st == 0 and bytes(buf_b[:5]) == b"hello"
        print("  B recv'd: hello")
        ctypes.memmove(ctypes.addressof(buf_b), b"world", 5)
        st = ep_b.async_send((h_b, 0, 5)).wait()
        assert st == 0
        print("  B sent 5 bytes")
        ep_b.shutdown()

    _sync_run(run_a, run_b)
    print("  PASSED\n")


def test_async_write_read():
    """A writes to B's buffer, then reads from B's buffer."""
    print("=== test_async_write_read ===")

    buf_a = ctypes.create_string_buffer(4096)
    buf_b = ctypes.create_string_buffer(4096)
    addr_a = ctypes.addressof(buf_a)

    ep_a = TcpEndpoint(0)
    ep_b = TcpEndpoint(0)

    h_a = ep_a.register_memory_region("a", addr_a, 0, 4096)
    h_b = ep_b.register_memory_region("b", ctypes.addressof(buf_b), 0, 4096)

    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()

    h_br = ep_a.register_remote_memory_region("rb", info_b["mr_info"]["b"])

    test_data = b"hello_from_a"

    def run_a():
        ep_a.connect(info_b)
        print("  A connected")
        ctypes.memmove(addr_a, test_data, len(test_data))
        st = ep_a.async_write([(h_a, h_br, 0, 0, len(test_data))]).wait()
        assert st == 0, f"write failed: {st}"
        print(f"  A wrote {len(test_data)} bytes to B")
        time.sleep(0.1)
        st = ep_a.async_read([(h_a, h_br, 0, 0, len(test_data))]).wait()
        assert st == 0 and bytes(buf_a[:len(test_data)]) == test_data
        print(f"  A read from B: {bytes(buf_a[:len(test_data)])}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        print("  B connected")
        time.sleep(0.2)
        for _ in range(50):
            if bytes(buf_b[:len(test_data)]) == test_data:
                break
            time.sleep(0.01)
        assert bytes(buf_b[:len(test_data)]) == test_data
        print(f"  B buffer verified")
        ep_b.shutdown()

    _sync_run(run_a, run_b)
    print("  PASSED\n")


def test_recv_timeout():
    """recv times out when peer never sends."""
    print("=== test_recv_timeout ===")

    buf_a = ctypes.create_string_buffer(64)

    ep_a = TcpEndpoint(10003)
    h_a = ep_a.register_memory_region("a", ctypes.addressof(buf_a), 0, 64)
    ep_b = TcpEndpoint(10004)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        time.sleep(1.5)
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        fut = ep_a.async_recv((h_a, 0, 5))
        result = fut.wait_for(0.3)
        print(f"  recv wait_for(0.3s): {result} (expected None)")
        assert result is None, f"Expected None (timeout), got {result}"
        ep_a.shutdown()

    _sync_run(run_a, run_b)
    print("  PASSED\n")


def test_send_timeout_ms():
    """async_send accepts timeout_ms parameter."""
    print("=== test_send_timeout_ms ===")

    buf_a = ctypes.create_string_buffer(256)
    buf_b = ctypes.create_string_buffer(256)

    ep_a = TcpEndpoint(10005)
    ep_b = TcpEndpoint(10006)
    h_a = ep_a.register_memory_region("a", ctypes.addressof(buf_a), 0, 256)
    h_b = ep_b.register_memory_region("b", ctypes.addressof(buf_b), 0, 256)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        st = ep_b.async_recv((h_b, 0, 5)).wait()
        assert st == 0
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        ctypes.memmove(ctypes.addressof(buf_a), b"world", 5)
        st = ep_a.async_send((h_a, 0, 5), timeout_ms=10000).wait()
        assert st == 0, f"send timeout_ms=10000 failed: {st}"
        print(f"  async_send with timeout_ms=10000: status={st}")
        ep_a.shutdown()

    _sync_run(run_a, run_b)
    print("  PASSED\n")


def test_default_timeout():
    """async_send uses kDefaultTimeoutMs=30000 when timeout_ms not given."""
    print("=== test_default_timeout ===")

    buf_a = ctypes.create_string_buffer(128)
    buf_b = ctypes.create_string_buffer(128)

    ep_a = TcpEndpoint(10007)
    ep_b = TcpEndpoint(10008)
    h_a = ep_a.register_memory_region("a", ctypes.addressof(buf_a), 0, 128)
    h_b = ep_b.register_memory_region("b", ctypes.addressof(buf_b), 0, 128)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        st = ep_b.async_recv((h_b, 0, 5)).wait()
        assert st == 0
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        ctypes.memmove(ctypes.addressof(buf_a), b"test!", 5)
        # No timeout_ms arg — uses default 30000ms
        st = ep_a.async_send((h_a, 0, 5)).wait()
        assert st == 0, f"default timeout send failed: {st}"
        print(f"  async_send with default timeout: status={st}")
        ep_a.shutdown()

    _sync_run(run_a, run_b)
    print("  PASSED\n")


if __name__ == "__main__":
    test_async_send_recv()
    test_async_write_read()
    test_recv_timeout()
    test_send_timeout_ms()
    test_default_timeout()
    print("All TcpEndpoint v3 tests passed!")
