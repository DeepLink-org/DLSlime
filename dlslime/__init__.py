from ._slime_c import *
import os

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


def get_cmake_dir():
    return os.path.join(os.path.dirname(__file__), "share", "cmake", "dlslime")
