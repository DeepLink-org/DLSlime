from ._slime_c import *
import os

from .logging import get_logger, set_log_level

try:
    from .peer_agent import PeerAgent, start_peer_agent
except ImportError as e:
    # peer_agent may not be available if dependencies are missing
    # Store the error so we can show it when user tries to import
    _peer_agent_import_error = e

    def start_peer_agent(*args, **kwargs):
        raise ImportError(
            "PeerAgent requires 'httpx' and 'redis' packages. "
            "Install them with: pip install httpx redis"
        ) from _peer_agent_import_error

    def PeerAgent(*args, **kwargs):
        raise ImportError(
            "PeerAgent requires 'httpx' and 'redis' packages. "
            "Install them with: pip install httpx redis"
        ) from _peer_agent_import_error


def __getattr__(name):
    # Lazily expose dlslime.ctrl.NanoCtrlClient at the package root so a bare
    # `import dlslime` does not require `httpx` to be installed.
    if name == "NanoCtrlClient":
        from .ctrl import NanoCtrlClient

        return NanoCtrlClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_cmake_dir():
    return os.path.join(os.path.dirname(__file__), "share", "cmake", "dlslime")
