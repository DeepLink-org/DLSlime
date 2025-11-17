from typing import Any, Dict

import torch
import torch.distributed as dist

from dlslime import _slime_c


class AllToAllIntraLLBuffer:
    def __init__(
        self,
        max_dispatch_per_msg,
        max_bs,
        rank: int,
        world_size: int,
        buffer_size: int,
    ):
        self.max_bs = max_bs
        self.max_dispatch_per_msg = max_dispatch_per_msg

        self.rank = rank
        self.world_size = world_size

        self._buffer = self.buffer = _slime_c.AllToAllIntraLLBuffer(
            self.max_dispatch_per_msg,
            self.max_bs,
            self.rank,
            self.world_size,
            buffer_size,
        )

    @staticmethod
    def get_buffer_size_hint(max_dispatch_per_msg, max_bs, max_msg_size, itemsize):
        return _slime_c.AllToAllIntraLLBuffer.get_buffer_size_hint(
            max_bs, max_msg_size, itemsize, max_dispatch_per_msg
        )

    @property
    def local_buffer(self):
        return self._buffer.get_local_buffer()

    @property
    def buffer_info(self):
        return self._buffer.buffer_info()

    def set_max_bs(self, bs):
        self._buffer.set_max_bs(bs)

    def connect_full_mesh(self, group: dist.ProcessGroup):
        buffer_info = self.buffer_info
        all_buffer_info = [None for _ in range(group.size())]
        dist.all_gather_object(all_buffer_info, buffer_info, group=group)
        return self._buffer.connect_full_mesh(all_buffer_info)

    def all_to_all_ll(
        self, x: torch.Tensor, is_transpose=False, mask=None
    ) -> torch.Tensor:
        return self._buffer.all_to_all_ll(x, is_transpose, mask)
