"""SlimeRPC error types raised across the wire."""


class RpcError(Exception):
    """Base class for SlimeRPC client-visible errors."""


class RpcTimeoutError(RpcError, TimeoutError):
    """Raised when ``future.wait(timeout=...)`` expires before reply."""


class RemoteRpcError(RpcError):
    """Raised on the client when the remote handler raised an exception.

    Attributes:
        type_name: Short class name of the remote exception (best-effort).
        traceback: Formatted traceback string from the remote process.
    """

    def __init__(
        self,
        message: str,
        *,
        type_name: str | None = None,
        traceback: str | None = None,
    ):
        full = message if not type_name else f"{type_name}: {message}"
        super().__init__(full)
        self.type_name = type_name
        self.traceback = traceback

    def __str__(self) -> str:
        base = super().__str__()
        if self.traceback:
            return f"{base}\n--- remote traceback ---\n{self.traceback}"
        return base
