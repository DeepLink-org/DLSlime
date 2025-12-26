from __future__ import annotations

import collections.abc
import typing

import typing_extensions

__all__: list[str] = [
    "Assignment",
    "DeviceSignal",
    "OpCode",
    "RDMAContext",
    "RDMAEndpoint",
    "RDMAWorker",
    "SlimeImmRecvFuture",
    "SlimeReadWriteFuture",
    "SlimeRecvFuture",
    "SlimeSendFuture",
    "available_nic",
    "socket_id",
]

class Assignment:
    @typing.overload
    def __init__(
        self,
        arg0: typing.SupportsInt,
        arg1: typing.SupportsInt,
        arg2: typing.SupportsInt,
        arg3: typing.SupportsInt,
    ) -> None: ...
    @typing.overload
    def __init__(
        self,
        arg0: typing.SupportsInt,
        arg1: typing.SupportsInt,
        arg2: typing.SupportsInt,
        arg3: typing.SupportsInt,
        arg4: typing.SupportsInt,
    ) -> None: ...

class DeviceSignal:
    def wait(self, arg0: typing.SupportsInt) -> None: ...

class OpCode:
    """
    Members:

      READ

      WRITE

      WRITE_WITH_IMM_DATA

      SEND

      RECV
    """

    READ: typing.ClassVar[OpCode]  # value = <OpCode.READ: 0>
    RECV: typing.ClassVar[OpCode]  # value = <OpCode.RECV: 3>
    SEND: typing.ClassVar[OpCode]  # value = <OpCode.SEND: 2>
    WRITE: typing.ClassVar[OpCode]  # value = <OpCode.WRITE: 1>
    WRITE_WITH_IMM_DATA: typing.ClassVar[
        OpCode
    ]  # value = <OpCode.WRITE_WITH_IMM_DATA: 5>
    __members__: typing.ClassVar[
        dict[str, OpCode]
    ]  # value = {'READ': <OpCode.READ: 0>, 'WRITE': <OpCode.WRITE: 1>, 'WRITE_WITH_IMM_DATA': <OpCode.WRITE_WITH_IMM_DATA: 5>, 'SEND': <OpCode.SEND: 2>, 'RECV': <OpCode.RECV: 3>}
    def __eq__(self, other: typing.Any) -> bool: ...
    def __getstate__(self) -> int: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __init__(self, value: typing.SupportsInt) -> None: ...
    def __int__(self) -> int: ...
    def __ne__(self, other: typing.Any) -> bool: ...
    def __repr__(self) -> str: ...
    def __setstate__(self, state: typing.SupportsInt) -> None: ...
    def __str__(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...

class RDMAContext:
    def __init__(self) -> None: ...
    def init(self, arg0: str, arg1: typing.SupportsInt, arg2: str) -> int: ...
    def launch_future(self) -> None: ...
    def reload_memory_pool(self) -> int: ...
    def stop_future(self) -> None: ...

class RDMAEndpoint:
    @typing.overload
    def __init__(
        self,
        context: RDMAContext = None,
        num_qp: typing.SupportsInt = 1,
        worker: ... = None,
    ) -> None: ...
    @typing.overload
    def __init__(
        self,
        device_name: str = "",
        ib_port: typing.SupportsInt = 1,
        link_type: str = "RoCE",
        num_qp: typing.SupportsInt = 1,
        worker: ... = None,
    ) -> None: ...
    def connect(self, arg0: dict) -> None: ...
    def endpoint_info(self) -> dict: ...
    def imm_recv(self, stream: typing.Any = None) -> SlimeImmRecvFuture: ...
    def process(self) -> int: ...
    def read(
        self,
        assign: collections.abc.Sequence[
            tuple[
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
            ]
        ],
        stream: typing.Any = None,
    ) -> SlimeReadWriteFuture: ...
    def recv(
        self,
        chunk: tuple[typing.SupportsInt, typing.SupportsInt, typing.SupportsInt],
        stream_handler: typing.Any = None,
    ) -> SlimeRecvFuture: ...
    def register_memory_region(
        self,
        arg0: typing.SupportsInt,
        arg1: typing.SupportsInt,
        arg2: typing.SupportsInt,
    ) -> int: ...
    def register_remote_memory_region(
        self, arg0: typing.SupportsInt, arg1: dict
    ) -> int: ...
    def send(
        self,
        chunk: tuple[typing.SupportsInt, typing.SupportsInt, typing.SupportsInt],
        stream_handler: typing.Any = None,
    ) -> SlimeSendFuture: ...
    def write(
        self,
        assign: collections.abc.Sequence[
            tuple[
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
            ]
        ],
        stream: typing.Any = None,
    ) -> SlimeReadWriteFuture: ...
    def write_with_imm(
        self,
        assign: collections.abc.Sequence[
            tuple[
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
                typing.SupportsInt,
            ]
        ],
        imm_data: typing.SupportsInt = 0,
        stream: typing.Any = None,
    ) -> SlimeReadWriteFuture: ...

class RDMAWorker:
    @typing.overload
    def __init__(self, dev_name: str, id: typing.SupportsInt) -> None: ...
    @typing.overload
    def __init__(
        self, socket_id: typing.SupportsInt, id: typing.SupportsInt
    ) -> None: ...
    def add_endpoint(self, endpoint: RDMAEndpoint) -> int: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...

class SlimeImmRecvFuture:
    def imm_data(self) -> int: ...
    def wait(self) -> int: ...

class SlimeReadWriteFuture:
    def wait(self) -> int: ...

class SlimeRecvFuture:
    def wait(self) -> int: ...

class SlimeSendFuture:
    def wait(self) -> int: ...

def available_nic() -> list[str]: ...
def socket_id(arg0: str) -> int: ...

_BUILD_INTER_OPS: bool = False
_BUILD_INTRA_OPS: bool = False
_BUILD_NVLINK: bool = False
_BUILD_NVSHMEM: bool = False
_BUILD_RDMA: bool = True
