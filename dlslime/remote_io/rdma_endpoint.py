from typing import Any, Callable, Dict, List

from dlslime import _slime_c
from dlslime.assignment import Assignment

from .base_endpoint import BaseEndpoint


class RDMAEndpoint(BaseEndpoint):
    """Manages RDMA endpoint lifecycle including resource allocation and data
    operations.

    An RDMA endpoint represents a communication entity with:
    - Memory Region (MR) registration
    - Peer connection establishment
    - Queue Pair (QP) management
    - Completion Queue (CQ) handling
    """

    def __init__(
        self,
        device_name: str,
        ib_port: int = 1,
        link_type: str = 'RoCE',
    ):
        """Initialize an RDMA endpoint bound to specific hardware resources.

        Args:
            device_name: RDMA NIC device name (e.g. 'mlx5_0')
            ib_port: InfiniBand physical port number (1-based indexing)
            transport_type: Underlying transport ('RoCE' or 'InfiniBand')
        """
        self._ctx: _slime_c.rdma_context = _slime_c.rdma_context()
        self.initialize(device_name, ib_port, link_type)
        self.assignment_with_callback = {}

    @property
    def mr_info(self) -> Dict[str, Any]:
        return self.endpoint_info['mr_info']

    @property
    def rdma_info(self) -> Dict[str, Any]:
        return self.endpoint_info['rdma_info']

    @property
    def endpoint_info(self) -> Dict[str, Any]:
        """Retrieve local endpoint parameters for peer connection setup.

        Returns:
            Dictionary containing:
            - 'gid': Global Identifier (IPv6 format for RoCE)
            - 'qp_num': Queue Pair number
            - 'lid': Local ID (InfiniBand only)
        """
        return self._ctx.endpoint_info()

    def initialize(
        self,
        device_name: str,
        ib_port: int,
        transport_type: str,
    ) -> int:
        """Configure the endpoint with hardware resources.

        Returns:
            0 on success, non-zero error code matching IBV_ERROR_* codes
        """
        return self._ctx.init_rdma_context(device_name, ib_port, transport_type)

    def connect(self, remote_endpoint_info: Dict[str, Any]) -> None:
        """Establish RC (Reliable Connection) to a remote endpoint.

        Args:
            remote_endpoint_info: Dictionary from remote's local_endpoint_info()
        """
        self._ctx.connect(remote_endpoint_info)
        self._ctx.launch_future()  # Start background CQ polling

    def register_memory_region(
        self,
        mr_key: str,
        addr: int,
        offset: int,
        length: int,
    ) -> None:
        """Register a Memory Region (MR) for RDMA operations.

        Args:
            mr_identifier: Unique key to reference this MR
            virtual_address: Starting VA of the memory block
            length_bytes: Size of the region in bytes
        """
        self._ctx.register_memory_region(mr_key, addr + offset, length)

    def register_remote_memory_region(self, mr_key: str, remote_mr_info: dict) -> None:
        """Register a Remote Memory Region (MR) for RDMA operations.

        Args:
            remote_mr_info:
                - key: mr_key
                - value: mr_info
        """
        self._ctx.register_remote_memory_region(mr_key, remote_mr_info)

    def reload_memory_pool(self):
        return self._ctx.reload_memory_pool()

    def send_batch(
        self,
        batch: List[Assignment],
        async_op=False,
    ) -> _slime_c.RDMAAssignment:
        rdma_assignment = self._ctx.submit(
            _slime_c.OpCode.SEND,
            [
                _slime_c.Assignment(
                    assign.mr_key,
                    assign.target_offset,
                    assign.source_offset,
                    assign.length,
                ) for assign in batch
            ],
            None,
        )
        if not async_op:
            return rdma_assignment.wait()
        else:
            return rdma_assignment

    def recv_batch(
        self,
        batch: List[Assignment],
        async_op=False,
    ) -> _slime_c.RDMAAssignment:
        rdma_assignment = self._ctx.submit(
            _slime_c.OpCode.RECV,
            [
                _slime_c.Assignment(
                    assign.mr_key,
                    assign.target_offset,
                    assign.source_offset,
                    assign.length,
                ) for assign in batch
            ],
            None,
        )
        if not async_op:
            return rdma_assignment.wait()
        else:
            return rdma_assignment

    def read_batch_with_callback(self, batch: List[Assignment], callback: Callable[[int], None]):
        callback_obj_id = id(callback)

        def delete_assignment_callback(code: int):
            callback(code)
            del self.assignment_with_callback[callback_obj_id]

        rdma_assignment = self._ctx.submit(
            _slime_c.OpCode.READ,
            [
                _slime_c.Assignment(
                    assign.mr_key,
                    assign.target_offset,
                    assign.source_offset,
                    assign.length,
                ) for assign in batch
            ],
            delete_assignment_callback,
        )
        self.assignment_with_callback[callback_obj_id] = rdma_assignment
        return rdma_assignment

    def read_batch(
        self,
        batch: List[Assignment],
        async_op=False,
    ) -> int:
        """Perform batched read from remote MR to local buffer.

        Args:
            remote_mr_key: Target MR identifier registered at remote
            remote_offset: Offset in remote MR (bytes)
            local_buffer_addr: Local destination VA
            read_size: Data size in bytes

        Returns:
            ibv_wc_status code (0 = IBV_WC_SUCCESS)
        """
        rdma_assignment = self._ctx.submit(
            _slime_c.OpCode.READ,
            [
                _slime_c.Assignment(
                    assign.mr_key,
                    assign.target_offset,
                    assign.source_offset,
                    assign.length,
                ) for assign in batch
            ],
            None,
        )
        if async_op:
            return rdma_assignment
        else:
            return rdma_assignment.wait()

    def write_batch(
        self,
        batch: List[Assignment],
        async_op=False,
    ) -> int:
        """Perform batched read from remote MR to local buffer.

        Args:
            remote_mr_key: Target MR identifier registered at remote
            remote_offset: Offset in remote MR (bytes)
            local_buffer_addr: Local destination VA
            read_size: Data size in bytes

        Returns:
            ibv_wc_status code (0 = IBV_WC_SUCCESS)
        """
        rdma_assignment = self._ctx.submit(
            _slime_c.OpCode.WRITE,
            [
                _slime_c.Assignment(
                    assign.mr_key,
                    assign.target_offset,
                    assign.source_offset,
                    assign.length,
                ) for assign in batch
            ],
            None,
        )
        if async_op:
            return rdma_assignment
        else:
            return rdma_assignment.wait()

    def stop(self):
        """Safely stops the endpoint by terminating all background activities
        and releasing resources."""
        self._ctx.stop_future()
