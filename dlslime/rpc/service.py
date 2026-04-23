"""serve() — worker-side event loop that dispatches incoming RPC calls."""

import ctypes
import logging

from .channel import Channel, FLAG_MASK, REPLY_BIT
from .registry import MethodRegistry

logger = logging.getLogger("slime.rpc")


def _log_disconnect(message: str) -> None:
    logger.info(message)
    print(message)


def serve(
    agent,
    service_instance,
    peer: str | None = None,
    *,
    channel: Channel | None = None,
):
    """Block forever, dispatching incoming RPC calls.

    Args:
        agent:   PeerAgent (used to resolve the channel when *channel* is ``None``).
        service_instance: Object with ``@method``-decorated functions.
        peer:    Peer alias to listen to.  Required when more than one
                 peer is connected.
        channel: Provide a pre-built Channel directly (skips agent lookup).
    """
    from .proxy import _get_or_create_channel  # avoid circular import

    registry = MethodRegistry(type(service_instance))

    if channel is None:
        if peer is None:
            peers = agent.get_connected_peers()
            if len(peers) != 1:
                raise ValueError(
                    f"serve() with peer=None requires exactly 1 connected peer, "
                    f"got {len(peers)}: {peers}.  Pass peer=... explicitly."
                )
            peer = next(iter(peers))
        channel = _get_or_create_channel(agent, peer)

    method_names = ", ".join(m.name for m in registry.by_tag.values())
    logger.info(
        f"SlimeRPC serving {type(service_instance).__name__} "
        f"[{method_names}] for peer={peer}"
    )

    while True:
        try:
            tag, ptr, nbytes, _keepalive = channel.recv_message()
        except ConnectionError:
            _log_disconnect(
                "SlimeRPC serve: peer disconnected while waiting for request."
            )
            return
        base_tag = tag & ~FLAG_MASK

        if tag & REPLY_BIT:
            # This is a reply — ignore in server loop
            continue

        method_info = registry.by_tag.get(base_tag)
        if method_info is None:
            logger.warning(f"Unknown RPC tag={base_tag} (raw={tag:#06x}), ignoring")
            continue

        if method_info.raw:
            # raw mode: pass (channel, ptr, nbytes) directly
            result = method_info.fn(service_instance, channel, ptr, nbytes)
        else:
            args = method_info.deserialize_args(ptr, nbytes)
            result = method_info.fn(service_instance, *args)

        # Send reply if the method returned something
        if result is not None:
            try:
                if method_info.raw and isinstance(result, (bytes, memoryview)):
                    n = len(result)
                    chunk_size = getattr(channel, "_chunk_size", 4 * 1024 * 1024)
                    if n > chunk_size:
                        channel.send_chunked(
                            base_tag | REPLY_BIT, result, chunk_size=chunk_size
                        )
                        continue
                    channel.ensure_send_capacity(n)
                    ctypes.memmove(channel.ptr, result, n)
                else:
                    n = method_info.serialize_result(result, channel.ptr, channel.size)
                channel.send(base_tag | REPLY_BIT, n)
            except ConnectionError:
                _log_disconnect(
                    "SlimeRPC serve: peer disconnected while sending reply."
                )
                return


def serve_once(
    agent,
    service_instance,
    peer: str | None = None,
    *,
    channel: Channel | None = None,
) -> None:
    """Dispatch exactly one incoming RPC call, then return.

    Same arguments as ``serve()``.  Useful for integrating into an
    existing event loop rather than blocking forever.
    """
    from .proxy import _get_or_create_channel

    registry = MethodRegistry(type(service_instance))

    if channel is None:
        if peer is None:
            peers = agent.get_connected_peers()
            if len(peers) != 1:
                raise ValueError(
                    f"serve_once() with peer=None requires exactly 1 peer, "
                    f"got {len(peers)}: {peers}."
                )
            peer = next(iter(peers))
        channel = _get_or_create_channel(agent, peer)

    try:
        tag, ptr, nbytes, _keepalive = channel.recv_message()
    except ConnectionError:
        _log_disconnect(
            "SlimeRPC serve_once: peer disconnected while waiting for request."
        )
        return

    base_tag = tag & ~FLAG_MASK
    method_info = registry.by_tag.get(base_tag)
    if method_info is None:
        logger.warning(f"Unknown RPC tag={base_tag} (raw={tag:#06x}), ignoring")
        return

    if method_info.raw:
        result = method_info.fn(service_instance, channel, ptr, nbytes)
    else:
        args = method_info.deserialize_args(ptr, nbytes)
        result = method_info.fn(service_instance, *args)

    if result is not None:
        try:
            if method_info.raw and isinstance(result, (bytes, memoryview)):
                n = len(result)
                chunk_size = getattr(channel, "_chunk_size", 4 * 1024 * 1024)
                if n > chunk_size:
                    channel.send_chunked(
                        base_tag | REPLY_BIT, result, chunk_size=chunk_size
                    )
                    return
                channel.ensure_send_capacity(n)
                ctypes.memmove(channel.ptr, result, n)
            else:
                n = method_info.serialize_result(result, channel.ptr, channel.size)
            channel.send(base_tag | REPLY_BIT, n)
        except ConnectionError:
            _log_disconnect(
                "SlimeRPC serve_once: peer disconnected while sending reply."
            )
            return
