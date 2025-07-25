from typing import Any, Dict, List

from dlslime import _slime_c
from dlslime.assignment import Assignment

from .base_endpoint import BaseEndpoint


class NVLinkEndpoint(BaseEndpoint):

    def __init__(self):
        self._ctx: _slime_c.nvlink_context = _slime_c.nvlink_context()
        self.initialize()

    @property
    def mr_info(self):
        return self.endpoint_info['mr_info']

    @property
    def endpoint_info(self):
        return self._ctx.endpoint_info()

    def initialize(self):
        pass

    def register_memory_region(self, mr_key: str, addr: int, offset: int, length: int) -> int:
        return self._ctx.register_memory_region(mr_key, addr, offset, length)

    def register_remote_memory_region(self, mr_key: str, remote_mr_info: Dict[str, Any]) -> int:
        return self._ctx.register_memory_region(mr_key, remote_mr_info)

    def reload_memory_pool(self):
        raise NotImplementedError

    def connect(self, endpoint_info: Dict[str, Any]) -> int:
        return self._ctx.connect(endpoint_info)

    def read_batch(self, batch: List[Assignment], cuda_stream: int = 0):
        self._ctx.read_batch(
            [
                _slime_c.Assignment(
                    assign.mr_key,
                    assign.target_offset,
                    assign.source_offset,
                    assign.length,
                ) for assign in batch
            ],
            cuda_stream,
        )
