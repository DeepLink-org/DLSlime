from typing import Callable, Tuple

import torch

from dlslime import _slime_c
from dlslime.buffer.inter.init_nvshmem import setup_nvshmem_env


class AllGatherInterLLBuffer:
    def __init__(
        self,
        bs: int,
        msg_size: int,
        dtype: torch.dtype,
        world_size: int,
        rank: int,
        num_concurrency: int = 1,
        allow_nvlink: bool = True,
        qp_num: int = 8,
    ):
        self.bs = bs
        self.msg_size = msg_size
        self.dtype = dtype

        self.rank = rank
        self.world_size = world_size

        self.allow_nvlink = allow_nvlink

        setup_nvshmem_env(qp_num=qp_num)

        self._buffer = self.buffer = _slime_c.AllGatherInterLLBuffer(
            self.bs,
            self.msg_size,
            self.dtype,
            self.world_size,
            self.rank,
            num_concurrency,
            allow_nvlink,
        )

    @property
    def buffer_info(self):
        return self._buffer.buffer_info()

    def connect_full_mesh(self, all_buffer_info):
        return self._buffer.connect_full_mesh(all_buffer_info)

    def all_gather_ll(self, input_: torch.Tensor, tag: int = 0) -> torch.Tensor:
        return self._buffer.all_gather_ll(input_, tag)

    def all_gather_ll_hook(
        self, input_: torch.Tensor, tag: int = 0
    ) -> Tuple[torch.Tensor, Callable]:
        output, hook = self._buffer.all_gather_ll_hook(input_, tag)
        return output, hook
