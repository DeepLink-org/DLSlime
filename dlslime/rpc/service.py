"""serve() — worker-side event loop that dispatches incoming RPC calls.

Concurrency policy
==================

The serve loop hands each request to a ``ThreadPoolExecutor``. Whether
multiple handlers can run concurrently is governed by:

1. ``max_workers`` (or ``SLIME_RPC_SERVER_WORKERS``): hard upper bound on
   parallelism. **Default is 1** because most service objects (PyTorch
   model runners, NCCL workers, anything holding a CUDA stream) are not
   thread-safe. Raise this only if you know the service is reentrant.

2. ``@method(parallel=True)``: opt-in per-method flag. Methods marked
   ``parallel=True`` skip the service-wide lock. Methods without the
   flag (the default) run under a single per-service ``threading.Lock``,
   so even if ``max_workers > 1`` they execute serially relative to each
   other and to other serial methods on the same instance.

If the handler raises, an exception reply is sent back so the client
future does not hang. The reply tag has both ``REPLY_BIT`` and
``EXC_BIT`` set, and the payload is a pickled
``{"type", "message", "traceback"}`` dict.

The datapath runs in C++: ``RpcSession::poll_request`` for inbound
requests and ``RpcSession::post_reply`` for replies. The Python serve
loop never touches the Channel directly.
"""

import concurrent.futures
import ctypes
import os
import pickle
import threading
import traceback as _traceback

from dlslime.logging import get_logger
from .channel import Channel
from .registry import MethodRegistry

logger = get_logger("rpc")

# Flag bits mirrored from dlslime/csrc/rpc/rpc_constants.h. We only need
# the ones the server inspects on inbound traffic / sets on replies.
EXC_BIT = 1 << 27
REPLY_BIT = 1 << 29
CHUNK_BIT = 1 << 30


def _log_disconnect(message: str) -> None:
    logger.info(message)
    print(message)


def serve(
    agent,
    service_instance,
    peer: str | None = None,
    *,
    channel: Channel | None = None,
    max_workers: int | None = None,
):
    """Block forever, dispatching incoming RPC calls.

    Args:
        agent:   PeerAgent (used to resolve the channel when *channel* is ``None``).
        service_instance: Object with ``@method``-decorated functions.
        peer:    Peer alias to listen to.  Required when more than one
                 peer is connected.
        channel: Provide a pre-built Channel directly (skips agent lookup).
        max_workers: Override the dispatch pool size. Defaults to
                 ``SLIME_RPC_SERVER_WORKERS`` env var, or 1 if unset.
    """
    from .proxy import _get_or_create_channel, _get_or_create_runtime

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

    if max_workers is None:
        max_workers = int(os.environ.get("SLIME_RPC_SERVER_WORKERS", "1"))
    max_workers = max(1, max_workers)

    # Per-service lock guards non-parallel methods. If only one worker is
    # configured the lock is essentially a no-op but cheap to hold.
    service_lock = threading.Lock()

    runtime = _get_or_create_runtime(channel, for_server=True)
    _serve_loop(
        runtime.session,
        service_instance,
        channel,
        registry,
        max_workers,
        service_lock,
    )


def _dispatch_request(
    service_instance,
    session,
    channel: Channel,
    method_info,
    base_tag: int,
    request_id: int,
    slot_id: int,
    payload_ptr: int,
    payload_nbytes: int,
    service_lock: threading.Lock,
) -> None:
    # ``payload_ptr`` is borrowed: the C++ recv slot is stable from the
    # moment recv_loop dispatched the request until we call post_reply
    # below. The handler must finish reading the bytes before the reply
    # is posted; after post_reply the peer is allowed to overwrite the
    # slot.
    if method_info.raw and method_info.inplace:
        _dispatch_request_inplace(
            service_instance,
            session,
            channel,
            method_info,
            base_tag,
            request_id,
            slot_id,
            payload_ptr,
            payload_nbytes,
            service_lock,
        )
        return

    try:
        result = _invoke_handler(
            service_instance,
            channel,
            method_info,
            payload_ptr,
            payload_nbytes,
            service_lock,
        )
        if result is None:
            # Fire-and-forget method. The client still expects *some* reply
            # so its future and slot can be released; send an empty reply.
            _send_reply(session, base_tag, request_id, slot_id, b"")
            return

        if method_info.raw and isinstance(result, (bytes, memoryview)):
            data = bytes(result)
        else:
            data = method_info.serialize_result_bytes(result)

        cap = session.payload_capacity
        if len(data) > cap:
            raise RuntimeError(
                "RPC reply exceeds the per-inflight slot capacity. "
                f"Reply is {len(data)} B, slot payload limit is {cap} B."
            )
        _send_reply(session, base_tag, request_id, slot_id, data)
    except ConnectionError:
        _log_disconnect("SlimeRPC serve: peer disconnected while sending reply.")
    except BaseException as exc:
        logger.exception(
            "SlimeRPC handler failed for method=%s request_id=%d",
            method_info.name,
            request_id,
        )
        try:
            _send_exception_reply(session, base_tag, request_id, slot_id, exc)
        except BaseException:
            logger.exception(
                "Failed to send exception reply for request_id=%d", request_id
            )


def _dispatch_request_inplace(
    service_instance,
    session,
    channel: Channel,
    method_info,
    base_tag: int,
    request_id: int,
    slot_id: int,
    payload_ptr: int,
    payload_nbytes: int,
    service_lock: threading.Lock,
) -> None:
    """Zero-copy reply path for ``raw=True, inplace=True`` handlers.

    The handler runs inside the C++ session under send-mutex; it writes
    the reply directly into the registered send buffer and returns the
    number of bytes written. No intermediate ``bytes`` allocation, no
    memcpy out of Python heap.
    """
    parallel = method_info.parallel

    def writer(resp_ptr: int, resp_cap: int) -> int:
        # ``resp_ptr`` points at HEADER_SIZE bytes inside the registered
        # send buffer; the handler may write up to ``resp_cap`` bytes
        # there. The returned int is the actual reply size.
        if parallel:
            return method_info.fn(
                service_instance,
                payload_ptr,
                payload_nbytes,
                resp_ptr,
                resp_cap,
            )
        with service_lock:
            return method_info.fn(
                service_instance,
                payload_ptr,
                payload_nbytes,
                resp_ptr,
                resp_cap,
            )

    try:
        session.post_reply_inplace(
            base_tag | REPLY_BIT,
            request_id,
            slot_id,
            writer,
        )
    except ConnectionError:
        _log_disconnect(
            "SlimeRPC serve: peer disconnected while sending inplace reply."
        )
    except BaseException as exc:
        logger.exception(
            "SlimeRPC inplace handler failed for method=%s request_id=%d",
            method_info.name,
            request_id,
        )
        try:
            _send_exception_reply(session, base_tag, request_id, slot_id, exc)
        except BaseException:
            logger.exception(
                "Failed to send exception reply for request_id=%d", request_id
            )


def _invoke_handler(
    service_instance,
    channel,
    method_info,
    payload_ptr: int,
    payload_nbytes: int,
    service_lock,
):
    """Run the user handler under per-service lock unless it opted into parallel.

    Raw handlers receive ``(channel, payload_ptr, payload_nbytes)`` directly:
    ``payload_ptr`` is a uintptr_t pointing into the C++ recv slot, valid
    until post_reply runs. Typed handlers go through ``ctypes.string_at``
    to materialise a ``bytes`` for ``pickle.loads`` — that's the single
    unavoidable copy on the typed path.
    """
    parallel = getattr(method_info, "parallel", False)

    def _run():
        if method_info.raw:
            return method_info.fn(
                service_instance, channel, payload_ptr, payload_nbytes
            )
        payload = ctypes.string_at(payload_ptr, payload_nbytes)
        args = method_info.deserialize_args_bytes(payload)
        return method_info.fn(service_instance, *args)

    if parallel:
        return _run()
    with service_lock:
        return _run()


def _send_reply(
    session, base_tag: int, request_id: int, slot_id: int, data: bytes
) -> None:
    session.post_reply(base_tag | REPLY_BIT, request_id, slot_id, data)


def _send_exception_reply(
    session, base_tag: int, request_id: int, slot_id: int, exc: BaseException
) -> None:
    info = {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": "".join(_traceback.format_exception(exc)),
    }
    data = pickle.dumps(info, protocol=pickle.HIGHEST_PROTOCOL)
    cap = session.payload_capacity
    if len(data) > cap:
        info["traceback"] = "<truncated: traceback exceeds slot size>"
        data = pickle.dumps(info, protocol=pickle.HIGHEST_PROTOCOL)
    session.post_reply(
        base_tag | REPLY_BIT | EXC_BIT,
        request_id,
        slot_id,
        data,
    )


def _serve_loop(
    session,
    service_instance,
    channel: Channel,
    registry: MethodRegistry,
    max_workers: int,
    service_lock: threading.Lock,
) -> None:
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="slime-rpc-handler",
    ) as executor:
        registry_by_tag = registry.by_tag  # local for hot loop
        while True:
            req = session.poll_request()
            if req is None:
                _log_disconnect("SlimeRPC serve: session closed; exiting.")
                return

            # Snapshot every attribute once — each access crosses the
            # pybind boundary, and the small-RPC path is sensitive to it.
            raw_tag = req.raw_tag
            base_tag = req.base_tag
            request_id = req.request_id
            slot_id = req.slot_id
            payload_ptr = req.payload_ptr
            payload_nbytes = req.payload_nbytes

            if raw_tag & REPLY_BIT:
                # Replies must never appear on poll_request(); the recv
                # pump dispatches them to the client futures. If this
                # ever fires, treat as a wire-level invariant violation
                # and log loudly.
                logger.warning(
                    "SlimeRPC serve: unexpected reply on inbound queue "
                    "tag=%#x request_id=%d",
                    raw_tag,
                    request_id,
                )
                continue

            if raw_tag & CHUNK_BIT:
                logger.warning(
                    "SlimeRPC serve ignored chunked request tag=%#x " "request_id=%d",
                    raw_tag,
                    request_id,
                )
                _send_exception_reply(
                    session,
                    base_tag,
                    request_id,
                    slot_id,
                    RuntimeError("Chunked transfers are disabled"),
                )
                continue

            method_info = registry_by_tag.get(base_tag)
            if method_info is None:
                logger.warning(
                    "Unknown RPC tag=%d (raw=%#06x), ignoring",
                    base_tag,
                    raw_tag,
                )
                _send_exception_reply(
                    session,
                    base_tag,
                    request_id,
                    slot_id,
                    RuntimeError(f"Unknown RPC tag {base_tag} on remote service"),
                )
                continue

            # Capture ptr+nbytes (ints) into the executor closure rather
            # than the InboundRequest object: the slot pointer stays
            # valid until _dispatch_request calls post_reply, but we
            # don't want the executor thread reaching back into the
            # InboundRequest after that point.
            executor.submit(
                _dispatch_request,
                service_instance,
                session,
                channel,
                method_info,
                base_tag,
                request_id,
                slot_id,
                payload_ptr,
                payload_nbytes,
                service_lock,
            )


def serve_once(
    agent,
    service_instance,
    peer: str | None = None,
    *,
    channel: Channel | None = None,
) -> None:
    """Dispatch exactly one incoming RPC call, then return.

    Useful for integrating into an existing event loop. Runs the handler
    inline (no executor); concurrency policy from ``serve()`` does not
    apply because there is no overlap.
    """
    from .proxy import _get_or_create_channel, _get_or_create_runtime

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

    runtime = _get_or_create_runtime(channel, for_server=True)
    _serve_once(runtime.session, service_instance, channel, registry)


def _serve_once(
    session, service_instance, channel: Channel, registry: MethodRegistry
) -> None:
    req = session.poll_request()
    if req is None:
        _log_disconnect("SlimeRPC serve_once: session closed.")
        return

    if req.raw_tag & CHUNK_BIT:
        logger.warning(
            "serve_once ignored chunked request tag=%#x",
            req.raw_tag,
        )
        _send_exception_reply(
            session,
            req.base_tag,
            req.request_id,
            req.slot_id,
            RuntimeError("Chunked transfers are disabled"),
        )
        return

    method_info = registry.by_tag.get(req.base_tag)
    if method_info is None:
        logger.warning(
            "Unknown RPC tag=%d (raw=%#06x), ignoring",
            req.base_tag,
            req.raw_tag,
        )
        _send_exception_reply(
            session,
            req.base_tag,
            req.request_id,
            req.slot_id,
            RuntimeError(f"Unknown RPC tag {req.base_tag}"),
        )
        return

    dummy_lock = threading.Lock()

    if method_info.raw and method_info.inplace:
        _dispatch_request_inplace(
            service_instance,
            session,
            channel,
            method_info,
            req.base_tag,
            req.request_id,
            req.slot_id,
            req.payload_ptr,
            req.payload_nbytes,
            dummy_lock,
        )
        return

    try:
        result = _invoke_handler(
            service_instance,
            channel,
            method_info,
            req.payload_ptr,
            req.payload_nbytes,
            dummy_lock,
        )
    except ConnectionError:
        _log_disconnect("SlimeRPC serve_once: peer disconnected during handler.")
        return
    except BaseException as exc:
        logger.exception(
            "SlimeRPC serve_once handler failed for method=%s request_id=%d",
            method_info.name,
            req.request_id,
        )
        try:
            _send_exception_reply(
                session, req.base_tag, req.request_id, req.slot_id, exc
            )
        except ConnectionError:
            _log_disconnect(
                "serve_once: peer disconnected while sending exception reply."
            )
        return

    if result is None:
        try:
            _send_reply(session, req.base_tag, req.request_id, req.slot_id, b"")
        except ConnectionError:
            _log_disconnect("serve_once: peer disconnected while sending empty reply.")
        return

    if method_info.raw and isinstance(result, (bytes, memoryview)):
        data = bytes(result)
    else:
        data = method_info.serialize_result_bytes(result)

    cap = session.payload_capacity
    if len(data) > cap:
        try:
            _send_exception_reply(
                session,
                req.base_tag,
                req.request_id,
                req.slot_id,
                RuntimeError(f"Reply {len(data)} B exceeds slot capacity {cap} B"),
            )
        except ConnectionError:
            pass
        return

    try:
        _send_reply(session, req.base_tag, req.request_id, req.slot_id, data)
    except ConnectionError:
        _log_disconnect("serve_once: peer disconnected while sending reply.")
