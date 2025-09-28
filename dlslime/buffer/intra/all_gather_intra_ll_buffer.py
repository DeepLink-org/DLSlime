from typing import Any, Dict

import torch

from dlslime import _slime_c


class AllGatherIntraLLBuffer:
    def __init__(
        self, bs: int, msg_size: int, dtype: torch.dtype, rank: int, world_size: int
    ):
        self.bs = bs
        self.msg_size = msg_size
        self.dtype = dtype

        self.rank = rank
        self.world_size = world_size

        self._buffer = self.buffer = _slime_c.AllGatherIntraLLBuffer(
            self.bs, self.msg_size, self.dtype, self.world_size, self.rank
        )

    @property
    def buffer_info(self):
        return self._buffer.buffer_info()

    def connect_full_mesh(self, all_buffer_info):
        return self._buffer.connect_full_mesh(all_buffer_info)

    def all_gather_ll(self, x: torch.Tensor) -> torch.Tensor:
        return self._buffer.all_gather_ll(x)

    def all_gather_ll_hook(self, x: torch.Tensor) -> torch.Tensor:
        raise NotImplementedError("hook_mode is not supported in IntraNodeBuffer")
