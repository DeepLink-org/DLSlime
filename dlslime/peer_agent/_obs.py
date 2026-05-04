"""Lightweight observability helpers gated by the DLSLIME_TIMING env var.

Export ``DLSLIME_TIMING=1`` to enable the probes. When disabled, the
`_tlog` function returns in a single attribute lookup — cheap enough to
leave sprinkled through the handshake path.
"""

from __future__ import annotations

import os
import time

# Gate for the fine-grained timing probes sprinkled through the handshake
# path. Export DLSLIME_TIMING=1 to enable; zero overhead otherwise.
_TIMING = os.environ.get("DLSLIME_TIMING", "") not in ("", "0", "false", "False")


def _tlog(msg: str) -> None:
    if _TIMING:
        # time.perf_counter is monotonic and has sub-microsecond resolution
        # on Linux; stamp in ms.
        print(f"[TLOG {time.perf_counter()*1000:.3f}ms] {msg}", flush=True)
