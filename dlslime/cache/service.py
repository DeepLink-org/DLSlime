"""Python service wrapper for DLSlimeCache."""

from __future__ import annotations

import contextlib
import ctypes
import json
import socket
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from dlslime._slime_c import Assignment, cache as _cache
from .types import assignment_from_json, manifest_to_json, stats_to_json

DEFAULT_SLAB_SIZE = 256 * 1024
DEFAULT_MEMORY_SIZE = 0
DEFAULT_CACHE_MR_NAME = "cache"

try:
    from nanoctrl import NanoCtrlClient
except Exception:  # pragma: no cover - optional integration
    NanoCtrlClient = None


def resolve_host_for_registration(host: str) -> str:
    if host in {"", "0.0.0.0", "::"}:
        return socket.gethostname()
    return host


class CacheService:
    """In-process DLSlimeCache service object.

    ``peer_agent`` is intentionally a composition dependency, not a base
    class. When present with a nonzero ``memory_size``, the service
    preallocates host memory and registers it as a PeerAgent MR so clients
    can write/read cache bytes through the existing RDMA endpoint path.
    """

    def __init__(
        self,
        peer_agent: Any | None = None,
        *,
        slab_size: int = DEFAULT_SLAB_SIZE,
        memory_size: int = DEFAULT_MEMORY_SIZE,
        cache_mr_name: str = DEFAULT_CACHE_MR_NAME,
    ) -> None:
        self.cache = _cache.CacheServer(slab_size=slab_size, memory_size=memory_size)
        self.peer_agent = peer_agent
        self.cache_mr_name = cache_mr_name
        self.cache_storage = None
        self.cache_mr_handle = None
        if self.peer_agent is not None and memory_size > 0:
            self.cache_storage = ctypes.create_string_buffer(memory_size)
            self.cache_mr_handle = self.peer_agent.register_memory_region(
                self.cache_mr_name,
                ctypes.addressof(self.cache_storage),
                0,
                memory_size,
            )

    @property
    def slab_size(self) -> int:
        return int(self.cache.slab_size())

    @property
    def memory_size(self) -> int:
        return int(self.cache.memory_size())

    def store_assignments(
        self,
        peer_agent_id: str,
        assignments: list[Assignment],
    ) -> _cache.AssignmentManifest:
        return self.cache.store_assignments(peer_agent_id, assignments)

    def query_assignments(
        self,
        peer_agent_id: str,
        version: int,
    ) -> _cache.AssignmentManifest | None:
        return self.cache.query_assignments(peer_agent_id, int(version))

    def delete_assignments(self, peer_agent_id: str, version: int) -> bool:
        return bool(self.cache.delete_assignments(peer_agent_id, int(version)))

    def stats(self) -> _cache.CacheStats:
        return self.cache.stats()

    def peer_agent_info(self) -> dict[str, Any]:
        if self.peer_agent is None:
            raise RuntimeError("cache service was started without a PeerAgent")
        alias = str(getattr(self.peer_agent, "alias", ""))
        if not alias:
            raise RuntimeError("cache service PeerAgent has no alias")
        if self.cache_mr_handle is None:
            raise RuntimeError(
                "cache service was started without preallocated cache memory"
            )
        resource = None
        if hasattr(self.peer_agent, "query_resource"):
            resource = self.peer_agent.query_resource(alias)
        return {
            "peer_agent_id": alias,
            "cache_mr_name": self.cache_mr_name,
            "cache_mr_handle": self.cache_mr_handle,
            "ctrl": getattr(self.peer_agent, "server_url", None),
            "scope": getattr(self.peer_agent, "redis_key_prefix", None) or None,
            "slab_size": self.slab_size,
            "memory_size": self.memory_size,
            "resource": resource,
        }


class CacheRequestHandler(BaseHTTPRequestHandler):
    server: "CacheHttpServer"

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send_json({"ok": True})
            return
        if self.path == "/stats":
            self._send_json(stats_to_json(self.server.service.stats()))
            return
        if self.path == "/peer-agent":
            self._handle_peer_agent()
            return
        self._send_error(HTTPStatus.NOT_FOUND, "unknown path")

    def do_POST(self) -> None:
        try:
            body = self._read_json()
            if self.path == "/store":
                self._handle_store(body)
                return
            if self.path == "/query":
                self._handle_query(body)
                return
            if self.path == "/delete":
                self._handle_delete(body)
                return
            self._send_error(HTTPStatus.NOT_FOUND, "unknown path")
        except KeyError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, f"missing field: {exc}")
        except (RuntimeError, ValueError) as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.server.quiet:
            return
        super().log_message(fmt, *args)

    def _handle_store(self, body: dict[str, Any]) -> None:
        peer_agent_id = str(body["peer_agent_id"])
        assignments = [assignment_from_json(item) for item in body["assignments"]]
        manifest = self.server.service.store_assignments(peer_agent_id, assignments)
        self._send_json(manifest_to_json(manifest))

    def _handle_query(self, body: dict[str, Any]) -> None:
        peer_agent_id = str(body["peer_agent_id"])
        version = int(body["version"])
        manifest = self.server.service.query_assignments(peer_agent_id, version)
        if manifest is None:
            self._send_error(HTTPStatus.NOT_FOUND, "assignment manifest not found")
            return
        self._send_json(manifest_to_json(manifest))

    def _handle_delete(self, body: dict[str, Any]) -> None:
        peer_agent_id = str(body["peer_agent_id"])
        version = int(body["version"])
        existed = self.server.service.delete_assignments(peer_agent_id, version)
        self._send_json({"deleted": existed})

    def _handle_peer_agent(self) -> None:
        try:
            self._send_json(self.server.service.peer_agent_info())
        except RuntimeError as exc:
            self._send_error(HTTPStatus.SERVICE_UNAVAILABLE, str(exc))

    def _read_json(self) -> dict[str, Any]:
        nbytes = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(nbytes)
        if not data:
            return {}
        obj = json.loads(data.decode("utf-8"))
        if not isinstance(obj, dict):
            raise ValueError("request body must be a JSON object")
        return obj

    def _send_json(
        self, obj: dict[str, Any], status: HTTPStatus = HTTPStatus.OK
    ) -> None:
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"error": message}, status)


class CacheHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        addr: tuple[str, int],
        peer_agent: Any | None = None,
        service: CacheService | None = None,
        *,
        slab_size: int = DEFAULT_SLAB_SIZE,
        memory_size: int = DEFAULT_MEMORY_SIZE,
        cache_mr_name: str = DEFAULT_CACHE_MR_NAME,
        quiet: bool = False,
    ) -> None:
        super().__init__(addr, CacheRequestHandler)
        self.service = service or CacheService(
            slab_size=slab_size,
            memory_size=memory_size,
            cache_mr_name=cache_mr_name,
            peer_agent=peer_agent,
        )
        self.quiet = quiet


class NanoCtrlRegistration:
    """Register a cache HTTP service in NanoCtrl's generic service registry."""

    def __init__(
        self,
        *,
        ctrl: str,
        service_id: str,
        host: str,
        port: int,
        scope: str | None = None,
        advertise_host: str | None = None,
        heartbeat_interval: float = 15.0,
    ) -> None:
        if NanoCtrlClient is None:
            raise RuntimeError(
                "nanoctrl Python package is not importable. Install NanoCtrl or run without NanoCtrl registration."
            )
        self.ctrl = ctrl
        self.service_id = service_id
        self.host = host
        self.port = int(port)
        self.scope = scope
        self.advertise_host = advertise_host or resolve_host_for_registration(host)
        self.heartbeat_interval = heartbeat_interval
        self.client = NanoCtrlClient(address=ctrl, scope=scope)

    def start(self) -> None:
        self.client.check_connection()
        self._register()
        self.client.start_heartbeat(
            interval=self.heartbeat_interval,
            on_not_found=self._register,
            name=f"cache-service-hb:{self.service_id}",
        )

    def stop(self) -> None:
        with contextlib.suppress(Exception):
            self.client.stop()

    def _register(self) -> None:
        ok = self.client.register(
            self.service_id,
            "cache",
            endpoint={
                "host": self.advertise_host,
                "port": self.port,
                "protocol": "http",
            },
            metadata={
                "p2p_host": self.advertise_host,
                "p2p_port": self.port,
            },
        )
        if not ok:
            raise RuntimeError(
                f"NanoCtrl rejected cache service registration: {self.service_id}"
            )
