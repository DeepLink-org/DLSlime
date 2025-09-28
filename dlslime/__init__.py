from dlslime import _slime_c
from .assignment import Assignment

if _slime_c._BUILD_RDMA:
    from dlslime._slime_c import available_nic, OpCode
    from .remote_io.rdma_endpoint import RDMAEndpoint
if _slime_c._BUILD_NVLINK:
    from .remote_io.nvlink_endpoint import NVLinkEndpoint


__all__ = ["OpCode", "available_nic", "Assignment", "NVLinkEndpoint", "RDMAEndpoint"]
