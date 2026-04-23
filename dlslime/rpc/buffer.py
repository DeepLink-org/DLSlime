"""GrowableBuffer — RDMA-registered pinned buffer with auto-resize."""

import torch


class GrowableBuffer:
    """Pinned CPU buffer that doubles when capacity is exceeded.

    On resize the old MR stays registered (handles are monotonic in
    DLSlime) and a new MR is created for the larger buffer.
    """

    def __init__(
        self,
        initial_size: int,
        pool=None,
        endpoint=None,
        name: str = "",
    ):
        self._pool = pool
        self._endpoint = endpoint
        self._name = name
        self.handler: int = -1  # MR handler id from RDMA registration
        self._buf = torch.empty(initial_size, dtype=torch.int8, pin_memory=True)
        self._register()

    # ── Public ───────────────────────────────────────

    @property
    def ptr(self) -> int:
        return self._buf.data_ptr()

    @property
    def capacity(self) -> int:
        return self._buf.numel()

    def ensure_capacity(self, needed: int):
        """Grow buffer (doubling) if needed.  Re-registers with RDMA NIC."""
        if needed <= self._buf.numel():
            return
        new_size = max(needed, self._buf.numel() * 2)
        self._buf = torch.empty(new_size, dtype=torch.int8, pin_memory=True)
        self._register()

    # ── Internal ─────────────────────────────────────

    def _register(self):
        if self._endpoint is not None:
            self.handler = self._endpoint.register_memory_region(
                self._name, self._buf.data_ptr(), 0, self._buf.numel()
            )
        elif self._pool is not None:
            self.handler = self._pool.register_memory_region(
                self._buf.data_ptr(), self._buf.numel(), self._name or None
            )
