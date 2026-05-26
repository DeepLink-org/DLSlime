import ctypes
import os
import threading
import time

from dlslime import TcpEndpoint, TcpMemoryPool

# ── optional torch / CUDA support ────────────────────────

_HAS_TORCH = False
_HAS_CUDA = False

try:
    import torch

    _HAS_TORCH = True
    _HAS_CUDA = torch.cuda.is_available()
except Exception:
    pass

_CUDA_FORCE_OFF = os.environ.get("DLSLIME_TCP_TEST_CUDA", "") in ("0", "false", "no")


def _torch_skip():
    return not _HAS_TORCH


def _cuda_skip():
    if _CUDA_FORCE_OFF:
        return True
    return not _HAS_CUDA


# ── test harness ─────────────────────────────────────────


def _sync_run(name, fn_a, fn_b, timeout=120):
    err = []
    b = threading.Barrier(2)

    def wrap(fn):
        try:
            b.wait(10)
            fn()
        except Exception as e:
            err.append(e)

    ta = threading.Thread(target=wrap, args=(fn_a,), daemon=False)
    tb = threading.Thread(target=wrap, args=(fn_b,), daemon=False)
    ta.start()
    tb.start()
    ta.join(timeout)
    tb.join(timeout)
    if ta.is_alive() or tb.is_alive():
        raise RuntimeError(f"{name} FAIL!{timeout}s timeout!")
    if len(err) > 0:
        print(f"{name} FAIL {err}", flush=True)
        return False
    else:
        print(f"{name} SUCC ", flush=True)
        return True


# ── ctypes-based tests ───────────────────────────────────


def test_async_send_recv(
    ip_a: str = "0.0.0.0",
    port_a: int = 10001,
    ip_b: str = "0.0.0.0",
    port_b: int = 10002,
):
    buf_a = ctypes.create_string_buffer(128)
    buf_b = ctypes.create_string_buffer(128)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()

    def run_a():
        ep_a.connect(info_b)
        ctypes.memmove(ctypes.addressof(buf_a), b"hello", 5)
        time.sleep(5)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        st = ep_a.async_recv((ctypes.addressof(buf_a), 5, 5)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        if bytes(buf_a[5:10]) != b"world":
            raise RuntimeError(f"data: {bytes(buf_a[5:10])}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        st = ep_b.async_recv((ctypes.addressof(buf_b), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        if bytes(buf_b[:5]) != b"hello":
            raise RuntimeError(f"data: {bytes(buf_b[:5])}")
        ctypes.memmove(ctypes.addressof(buf_b), b"world", 5)
        time.sleep(5)
        st = ep_b.async_send((ctypes.addressof(buf_b), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        ep_b.shutdown()

    _sync_run("test_async_send_recv", run_a, run_b)


def test_async_send2recv(
    ip_a: str = "0.0.0.0",
    port_a: int = 10401,
    ip_b: str = "0.0.0.0",
    port_b: int = 10402,
):
    buf_a = ctypes.create_string_buffer(32)
    buf_b = ctypes.create_string_buffer(32)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()

    def run_a():
        ep_a.connect(info_b)
        ctypes.memmove(ctypes.addressof(buf_a), b"one", 3)
        time.sleep(5)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 3)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        st = ep_b.async_recv((ctypes.addressof(buf_b), 0, 3)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        if bytes(buf_b[:3]) != b"one":
            raise RuntimeError(f"data: {bytes(buf_b[:3])}")
        ep_b.shutdown()

    _sync_run("test_async_send_recv_one", run_a, run_b)


def test_async_write(
    ip_a: str = "0.0.0.0",
    port_a: int = 10003,
    ip_b: str = "0.0.0.0",
    port_b: int = 10004,
):
    buf_a = ctypes.create_string_buffer(256)
    buf_b = ctypes.create_string_buffer(256)
    addr_a = ctypes.addressof(buf_a)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    h_a = ep_a.register_memory_region("a", addr_a, 0, 256)
    h_b = ep_b.register_memory_region("b", ctypes.addressof(buf_b), 0, 256)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    h_br = ep_a.register_remote_memory_region("rb", info_b["mr_info"]["b"])

    test_data = b"hello_from_a"

    def run_a():
        ep_a.connect(info_b)
        ctypes.memmove(addr_a, test_data, len(test_data))
        st = ep_a.async_write([(h_a, h_br, 0, 0, len(test_data))]).wait()
        if st != 0:
            raise RuntimeError(f"write: {st}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        for _ in range(50):
            if bytes(buf_b[: len(test_data)]) == test_data:
                break
            time.sleep(0.5)
        if bytes(buf_b[: len(test_data)]) != test_data:
            raise RuntimeError(f"B write not received in {50 * 0.5}s")
        ep_b.shutdown()

    _sync_run("test_async_write", run_a, run_b)


def test_async_read(
    ip_a: str = "0.0.0.0",
    port_a: int = 10005,
    ip_b: str = "0.0.0.0",
    port_b: int = 10006,
):
    buf_a = ctypes.create_string_buffer(256)
    buf_b = ctypes.create_string_buffer(256)
    addr_a = ctypes.addressof(buf_a)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    h_a = ep_a.register_memory_region("a", addr_a, 0, 256)
    h_b = ep_b.register_memory_region("b", ctypes.addressof(buf_b), 0, 256)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    h_br = ep_a.register_remote_memory_region("rb", info_b["mr_info"]["b"])

    test_data = b"hello_from_b"
    ctypes.memmove(ctypes.addressof(buf_b), test_data, 12)

    def run_a():
        ep_a.connect(info_b)
        st = ep_a.async_read([(h_a, h_br, 0, 0, len(test_data))]).wait()
        if st != 0:
            raise RuntimeError(f"read: {st}")
        if bytes(buf_a[: len(test_data)]) != test_data:
            raise RuntimeError("read data mismatch")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        time.sleep(25)
        ep_b.shutdown()

    _sync_run("test_async_read", run_a, run_b)


# ── skip test ──


def test_recv_timeout(
    ip_a: str = "0.0.0.0",
    port_a: int = 10007,
    ip_b: str = "0.0.0.0",
    port_b: int = 10008,
):
    buf_a = ctypes.create_string_buffer(32)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        time.sleep(1.0)
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        fut = ep_a.async_recv((ctypes.addressof(buf_a), 0, 5))
        result = fut.wait_for(0.3)
        if result is not None:
            raise RuntimeError(f"expected None, got {result}")
        ep_a.shutdown()

    _sync_run("test_recv_timeout", run_a, run_b)


def test_send_timeout_ms(
    ip_a: str = "0.0.0.0",
    port_a: int = 10009,
    ip_b: str = "0.0.0.0",
    port_b: int = 10010,
):
    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        st = ep_b.async_recv((ctypes.addressof(buf_b), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        ctypes.memmove(ctypes.addressof(buf_a), b"world", 5)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 5), timeout_ms=10000).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        ep_a.shutdown()

    _sync_run("test_send_timeout_ms", run_a, run_b)


def test_default_timeout(
    ip_a: str = "0.0.0.0",
    port_a: int = 10011,
    ip_b: str = "0.0.0.0",
    port_b: int = 10012,
):
    buf_a = ctypes.create_string_buffer(32)
    buf_b = ctypes.create_string_buffer(32)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        st = ep_b.async_recv((ctypes.addressof(buf_b), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        ctypes.memmove(ctypes.addressof(buf_a), b"test!", 5)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        ep_a.shutdown()

    _sync_run("test_default_timeout", run_a, run_b)


def test_exact_size_mismatch(
    ip_a: str = "0.0.0.0",
    port_a: int = 10016,
    ip_b: str = "0.0.0.0",
    port_b: int = 10017,
):
    buf_a = ctypes.create_string_buffer(32)
    buf_b = ctypes.create_string_buffer(32)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        st = ep_b.async_recv((ctypes.addressof(buf_b), 0, 4), exact_size=True).wait()
        if st != -1:
            raise RuntimeError(f"expected TCP_FAILED(-1), got {st}")
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        ctypes.memmove(ctypes.addressof(buf_a), b"overflow", 8)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 8)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        ep_a.shutdown()

    _sync_run("test_exact_size_mismatch", run_a, run_b)


def test_overflow_truncate(
    ip_a: str = "0.0.0.0",
    port_a: int = 10013,
    ip_b: str = "0.0.0.0",
    port_b: int = 10014,
):
    buf_a = ctypes.create_string_buffer(64)
    buf_b = ctypes.create_string_buffer(64)

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)

    def run_b():
        ep_b.connect(ep_a.endpoint_info())
        st = ep_b.async_recv((ctypes.addressof(buf_b), 0, 4)).wait()
        if st != 0:
            raise RuntimeError(f"recv1: {st}")
        if bytes(buf_b[:4]) != b"LONG":
            raise RuntimeError(f"truncated: {bytes(buf_b[:4])}")
        st = ep_b.async_recv((ctypes.addressof(buf_b), 4, 5)).wait()
        if st != 0:
            raise RuntimeError(f"recv2: {st}")
        if bytes(buf_b[4:9]) != b"HELLO":
            raise RuntimeError(f"follow-up: {bytes(buf_b[4:9])}")
        ep_b.shutdown()

    def run_a():
        ep_a.connect(ep_b.endpoint_info())
        ctypes.memmove(ctypes.addressof(buf_a), b"LONGDATA", 8)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 8)).wait()
        if st != 0:
            raise RuntimeError(f"send1: {st}")
        ctypes.memmove(ctypes.addressof(buf_a), b"HELLO", 5)
        st = ep_a.async_send((ctypes.addressof(buf_a), 0, 5)).wait()
        if st != 0:
            raise RuntimeError(f"send2: {st}")
        ep_a.shutdown()

    _sync_run("test_overflow_truncate", run_a, run_b)


def test_mr_name_validation():
    ep = TcpEndpoint(port=0)
    buf = ctypes.create_string_buffer(32)

    h = ep.register_memory_region("valid", ctypes.addressof(buf), 0, 32)
    if h < 0:
        raise RuntimeError(f"valid name: {h}")

    h = ep.register_memory_region("", ctypes.addressof(buf), 0, 32)
    if h != -1:
        raise RuntimeError(f"empty name should return -1, got {h}")

    h = ep.register_memory_region("valid", ctypes.addressof(buf), 0, 32)
    if h != -1:
        raise RuntimeError(f"duplicate name should return -1, got {h}")

    ep.shutdown()


def test_connect_unreachable():
    ep = TcpEndpoint(port=10015)
    unreachable = {"host": "127.0.0.1", "port": 65535, "mr_info": {}}
    ep.connect(unreachable)
    if ep.is_connected():
        raise RuntimeError("should not be connected")
    ep.shutdown()


# ── parameterized torch tests (device="cpu" or "cuda") ──


def _make_tensor(shape, device, dtype, **kw):
    """Create a tensor on the given device.  CPU tensor for recv on cuda path
    uses ctypes buffer so data_ptr() gives host pointer (needed for cudaMemcpy)."""
    return torch.randn(
        shape,
        dtype=dtype,
        device=device if isinstance(device, torch.device) else torch.device(device),
        **kw,
    )


def test_torch_send_recv(
    device="cpu",
    dtype=torch.float32,
    ip_a: str = "0.0.0.0",
    port_a: int = 10101,
    ip_b: str = "0.0.0.0",
    port_b: int = 10102,
):
    """Round-trip: A send full → B recv → B send slice → A recv."""
    SZ, SL = 32, 5  # elements
    t_a = _make_tensor(SZ, device, dtype)
    t_b = _make_tensor(SZ, device, dtype)
    expected = t_a.clone()
    n_bytes = SZ * 4
    sl_bytes = SL * 4

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()

    def run_a():
        ep_a.connect(info_b)
        time.sleep(5)
        st = ep_a.async_send((t_a.data_ptr(), 0, n_bytes)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        st = ep_a.async_recv((t_a.data_ptr(), 10 * 4, sl_bytes)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        if not torch.equal(t_a[10:15], t_b[20:25]):
            raise RuntimeError("slice mismatch")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        st = ep_b.async_recv((t_b.data_ptr(), 0, n_bytes)).wait()
        if st != 0:
            raise RuntimeError(f"recv: {st}")
        if not torch.equal(expected, t_b):
            raise RuntimeError("full tensor mismatch")
        time.sleep(5)
        st = ep_b.async_send((t_b.data_ptr(), 20 * 4, sl_bytes)).wait()
        if st != 0:
            raise RuntimeError(f"send: {st}")
        ep_b.shutdown()

    _sync_run(f"test_torch_send_recv_{device}", run_a, run_b, 120)


def test_torch_write(
    device="cpu",
    dtype=torch.float32,
    ip_a: str = "0.0.0.0",
    port_a: int = 10103,
    ip_b: str = "0.0.0.0",
    port_b: int = 10104,
):
    """One-sided write: A async_write → B verifies data received."""
    SZ = 64
    t_a = _make_tensor(SZ, device, dtype)
    t_b = _make_tensor(SZ, device, dtype)
    expected = t_a.clone()

    n_bytes = SZ * 4

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    h_a = ep_a.register_memory_region("a", t_a.data_ptr(), 0, n_bytes)
    h_b = ep_b.register_memory_region("b", t_b.data_ptr(), 0, n_bytes)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    h_br = ep_a.register_remote_memory_region("rb", info_b["mr_info"]["b"])

    def run_a():
        ep_a.connect(info_b)
        st = ep_a.async_write([(h_a, h_br, 0, 0, n_bytes)]).wait()
        if st != 0:
            raise RuntimeError(f"write: {st}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        for _ in range(40):
            if torch.equal(expected, t_b):
                break
            time.sleep(0.5)
        if not torch.equal(expected, t_b):
            raise RuntimeError("write data not received")
        ep_b.shutdown()

    _sync_run(f"test_torch_write_{device}", run_a, run_b)


def test_torch_read(
    device="cpu",
    dtype=torch.float32,
    ip_a: str = "0.0.0.0",
    port_a: int = 10105,
    ip_b: str = "0.0.0.0",
    port_b: int = 10106,
):
    """One-sided read: B buffer pre-filled, A async_read and verifies."""
    dsize = 4
    SZ = 64
    t_a = _make_tensor(SZ, device, dtype)
    t_b = _make_tensor(SZ, device, dtype)
    expected = t_b.clone()

    n_bytes = SZ * dsize

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    h_a = ep_a.register_memory_region("a", t_a.data_ptr(), 0, n_bytes)
    h_b = ep_b.register_memory_region("b", t_b.data_ptr(), 0, n_bytes)
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    h_br = ep_a.register_remote_memory_region("rb", info_b["mr_info"]["b"])

    def run_a():
        ep_a.connect(info_b)
        st = ep_a.async_read([(h_a, h_br, 0, 0, n_bytes)]).wait()
        if st != 0:
            raise RuntimeError(f"read: {st}")
        if not torch.equal(t_a, expected):
            raise RuntimeError("read data mismatch")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        time.sleep(20)
        ep_b.shutdown()

    _sync_run(f"test_torch_read_{device}", run_a, run_b)


def test_torch_write_batch(
    device="cpu",
    dtype=torch.float32,
    n_batch=4,
    ip_a: str = "0.0.0.0",
    port_a: int = 10107,
    ip_b: str = "0.0.0.0",
    port_b: int = 10108,
):
    """One async_write with multiple assignments."""
    dsize = 4
    SZ = 64
    t_a_batch = [_make_tensor(SZ, device, dtype) for i in range(n_batch)]
    t_b_batch = [_make_tensor(SZ, device, dtype) for i in range(n_batch)]
    expected_batch = [i.clone() for i in t_a_batch]

    n_bytes = SZ * dsize

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    h_a_batch = [
        ep_a.register_memory_region(f"a_{i}", t_a_batch[i].data_ptr(), 0, n_bytes)
        for i in range(n_batch)
    ]
    h_b_batch = [
        ep_b.register_memory_region(f"b_{i}", t_b_batch[i].data_ptr(), 0, n_bytes)
        for i in range(n_batch)
    ]
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    h_br_batch = [
        ep_a.register_remote_memory_region(f"rb_{i}", info_b["mr_info"][f"b_{i}"])
        for i in range(n_batch)
    ]

    def run_a():
        ep_a.connect(info_b)
        assigns = [
            (h_a_batch[i], h_br_batch[i], i * dsize, i * dsize, dsize)
            for i in range(n_batch)
        ]
        st = ep_a.async_write(assigns).wait()
        if st != 0:
            raise RuntimeError(f"write batch: {st}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        time.sleep(3)
        for i in range(n_batch):
            time.sleep(2)
            if not torch.equal(t_b_batch[i][i], expected_batch[i][i]):
                raise RuntimeError(f"batch {i}: mismatch")
        ep_b.shutdown()

    _sync_run(f"test_torch_write_batch_{device}", run_a, run_b)


def test_torch_read_batch(
    device="cpu",
    dtype=torch.float32,
    n_batch=4,
    ip_a: str = "0.0.0.0",
    port_a: int = 10109,
    ip_b: str = "0.0.0.0",
    port_b: int = 10110,
):
    """One async_read with multiple assignments."""
    dsize = 4
    SZ = 64
    t_a_batch = [_make_tensor(SZ, device, dtype) for i in range(n_batch)]
    t_b_batch = [_make_tensor(SZ, device, dtype) for i in range(n_batch)]
    expected_batch = [i.clone() for i in t_b_batch]

    n_bytes = SZ * dsize

    ep_a = TcpEndpoint(ip=ip_a, port=port_a)
    ep_b = TcpEndpoint(ip=ip_b, port=port_b)
    h_a_batch = [
        ep_a.register_memory_region(f"a_{i}", t_a_batch[i].data_ptr(), 0, n_bytes)
        for i in range(n_batch)
    ]
    h_b_batch = [
        ep_b.register_memory_region(f"b_{i}", t_b_batch[i].data_ptr(), 0, n_bytes)
        for i in range(n_batch)
    ]
    info_a = ep_a.endpoint_info()
    info_b = ep_b.endpoint_info()
    h_br_batch = [
        ep_a.register_remote_memory_region(f"rb_{i}", info_b["mr_info"][f"b_{i}"])
        for i in range(n_batch)
    ]

    def run_a():
        ep_a.connect(info_b)
        assigns = [
            (h_a_batch[i], h_br_batch[i], i * dsize, i * dsize, dsize)
            for i in range(n_batch)
        ]
        st = ep_a.async_read(assigns).wait()
        if st != 0:
            raise RuntimeError(f"read batch: {st}")
        ep_a.shutdown()

    def run_b():
        ep_b.connect(info_a)
        time.sleep(3)
        for i in range(n_batch):
            time.sleep(2)
            if not torch.equal(t_a_batch[i][i], expected_batch[i][i]):
                raise RuntimeError(f"batch {i}: mismatch")
        ep_b.shutdown()

    _sync_run(f"test_torch_read_batch_{device}", run_a, run_b)


# ── main ─────────────────────────────────────────────────

if __name__ == "__main__":
    test_async_send_recv()
    test_async_send2recv()
    test_async_write()
    test_async_read()

    if not _torch_skip():
        device_list = ["cpu", "cuda"]
        if _cuda_skip():
            print("No Cuda, Skip", flush=True)
            device_list = [
                "cpu",
            ]

        for dev in device_list:
            test_torch_send_recv(device=dev)
            test_torch_write(device=dev)
            test_torch_read(device=dev)
            test_torch_write_batch(device=dev)
            test_torch_read_batch(device=dev)
