"""nanoctrl — shared NanoCtrl lifecycle client for NanoInfra services."""

__all__ = ["NanoCtrlClient"]


def __getattr__(name: str):
    if name == "NanoCtrlClient":
        from .client import NanoCtrlClient

        return NanoCtrlClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
