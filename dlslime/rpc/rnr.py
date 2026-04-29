"""Soft-RNR detection from sysfs counters.

Hard RNR (retry-limit exhausted) already surfaces as ``ConnectionError``
on the affected RPC because the QP transitions to ERROR and the next
CQE has ``IBV_WC_RNR_RETRY_EXC_ERR``. Soft RNR — the HW retried and
eventually succeeded — is invisible at the work-completion level. The
only signal lives in the Mellanox sysfs counter ``out_of_buffer``,
which increments once per RNR event regardless of whether the retry
later succeeded.

Typical use::

    from dlslime.rpc.rnr import SoftRnrMonitor

    monitor = SoftRnrMonitor()           # baselines all visible mlx5_* devices
    serve(agent, MyService(), peer="x")  # ... time passes ...
    print(monitor.delta())               # {"mlx5_0": 3, "mlx5_1": 0, ...}

The counter is "since boot" on the device, so we snapshot at construction
and report deltas. Polling is cheap (a single sysfs read per device);
poll on whatever cadence makes sense for your monitoring stack.
"""

import glob
from pathlib import Path

_DEFAULT_COUNTER = "out_of_buffer"


def _enumerate_devices() -> list[tuple[str, int]]:
    """Return every (device_name, port) that has hw_counters/out_of_buffer."""
    found: list[tuple[str, int]] = []
    for path in sorted(
        glob.glob("/sys/class/infiniband/*/ports/*/hw_counters/out_of_buffer")
    ):
        # /sys/class/infiniband/<dev>/ports/<port>/hw_counters/out_of_buffer
        parts = Path(path).parts
        # parts: ('/', 'sys', 'class', 'infiniband', <dev>, 'ports', <port>, ...)
        found.append((parts[4], int(parts[6])))
    return found


def read_rnr_counter(device: str, port: int = 1, name: str = _DEFAULT_COUNTER) -> int:
    """Read a single hw_counter; returns -1 if the path is unreadable."""
    p = Path(f"/sys/class/infiniband/{device}/ports/{port}/hw_counters/{name}")
    try:
        return int(p.read_text().strip())
    except (OSError, ValueError):
        return -1


class SoftRnrMonitor:
    """Snapshot RNR counters and report deltas.

    Args:
        devices: Iterable of (device, port) pairs to watch. If ``None``,
                 autodetects every ``mlx5_*`` device exposing
                 ``out_of_buffer``.
        counter: Counter name (default ``out_of_buffer``). Other useful
                 ones: ``rnr_nak_retry_err``, ``local_ack_timeout_err``,
                 ``np_cnp_sent`` (we sent ECN), ``rp_cnp_handled``
                 (peer told us to slow down).
    """

    def __init__(self, devices=None, counter: str = _DEFAULT_COUNTER):
        self._counter = counter
        self._devices = list(devices) if devices is not None else _enumerate_devices()
        self._baseline = {
            dp: read_rnr_counter(*dp, name=counter) for dp in self._devices
        }

    def snapshot(self) -> None:
        """Re-baseline against the current counter values."""
        self._baseline = {
            dp: read_rnr_counter(*dp, name=self._counter) for dp in self._devices
        }

    def delta(self) -> dict[str, int]:
        """Return ``{f"{dev}:{port}": current - baseline}``.

        Negative values mean the counter wrapped or the device is gone;
        treat as 0 for alerting.
        """
        out: dict[str, int] = {}
        for (dev, port), base in self._baseline.items():
            cur = read_rnr_counter(dev, port, name=self._counter)
            out[f"{dev}:{port}"] = max(0, cur - base) if base >= 0 and cur >= 0 else 0
        return out

    def total_delta(self) -> int:
        """Sum across all watched devices — quick "any RNR happening?" knob."""
        return sum(self.delta().values())
