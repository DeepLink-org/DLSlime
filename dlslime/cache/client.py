"""Client wrapper for DLSlimeCache services."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from dlslime._slime_c import Assignment
from .types import assignment_to_json

try:
    from nanoctrl import NanoCtrlClient
except Exception:  # pragma: no cover - optional integration
    NanoCtrlClient = None


def post_json(
    url: str, path: str, payload: dict[str, Any], *, timeout: float = 5.0
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{url.rstrip('/')}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # Local cache services should not accidentally go through HTTP proxies.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(f"{path} failed with HTTP {exc.code}: {body}") from exc


def get_json(url: str, path: str, *, timeout: float = 5.0) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{url.rstrip('/')}{path}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(f"{path} failed with HTTP {exc.code}: {body}") from exc


def discover_cache_url(
    *,
    ctrl: str,
    scope: str | None = None,
    service_id: str | None = "cache:0",
) -> str:
    if NanoCtrlClient is None:
        raise RuntimeError(
            "nanoctrl Python package is not importable. Install NanoCtrl or pass a direct cache URL."
        )

    client = NanoCtrlClient(address=ctrl, scope=scope)
    client.check_connection()

    info = client.get_entity_info(service_id) if service_id else None
    if info is None:
        caches = client.list_entities(kind="cache")
        if not caches:
            raise RuntimeError("No NanoCtrl service with kind='cache' found")
        info = caches[0]

    endpoint = info.get("endpoint") or {}
    host = endpoint["host"]
    port = int(endpoint["port"])
    return f"http://{host}:{port}"


class CacheClient:
    """HTTP client for a DLSlimeCache service.

    ``peer_agent`` is optional composition. When present, ``store`` uses
    ``peer_agent.alias`` as the default owner id for assignment batches.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        peer_agent: Any | None = None,
        ctrl: str | None = None,
        scope: str | None = None,
        service_id: str | None = "cache:0",
        timeout: float = 5.0,
    ) -> None:
        if url is None:
            if ctrl is None:
                raise ValueError("CacheClient requires either url or ctrl")
            url = discover_cache_url(ctrl=ctrl, scope=scope, service_id=service_id)
        self.url = url.rstrip("/")
        self.peer_agent = peer_agent
        self.timeout = timeout

    def server_peer_agent(self) -> dict[str, Any]:
        try:
            return get_json(self.url, "/peer-agent", timeout=self.timeout)
        except RuntimeError as exc:
            message = str(exc)
            if "/peer-agent failed with HTTP 503" in message:
                raise RuntimeError(
                    "cache service did not publish a usable PeerAgent. "
                    "Restart it with `dlslime-cache stop` and then "
                    "`dlslime-cache start --ctrl http://127.0.0.1:3000 --memory-size 1G`."
                ) from exc
            raise

    def connect_to_server(
        self,
        *,
        peer_agent: Any | None = None,
        alias: str | None = None,
        ctrl: str | None = None,
        scope: str | None = None,
        local_device: str | None = None,
        peer_device: str | None = None,
        ib_port: int = 1,
        qp_num: int = 1,
        timeout_sec: float = 60.0,
    ) -> dict[str, Any]:
        info = self.server_peer_agent()
        server_alias = str(info["peer_agent_id"])
        agent = peer_agent or self.peer_agent

        if agent is None or not hasattr(agent, "set_desired_topology"):
            server_url = ctrl or info.get("ctrl")
            if not server_url:
                raise RuntimeError("cache service did not publish a NanoCtrl address")
            from dlslime import PeerAgent

            agent = PeerAgent(
                alias=alias,
                server_url=server_url,
                device=local_device,
                ib_port=ib_port,
                qp_num=qp_num,
                scope=scope if scope is not None else info.get("scope"),
            )
        self.peer_agent = agent

        agent.set_desired_topology(
            server_alias,
            local_device=local_device,
            peer_device=peer_device,
            ib_port=ib_port,
            qp_num=qp_num,
        )
        agent.wait_for_peers([server_alias], timeout_sec=timeout_sec)
        return info

    def default_peer_agent_id(self) -> str:
        return self.peer_agent_id(self.peer_agent)

    @staticmethod
    def peer_agent_id(peer_agent: Any | None) -> str:
        alias = getattr(peer_agent, "alias", None)
        if not alias:
            raise ValueError(
                "peer_agent_id must be provided when no peer_agent.alias is available"
            )
        return str(alias)

    def store(
        self,
        assignments: list[Assignment],
        *,
        peer_agent: Any | None = None,
        peer_agent_id: str | None = None,
    ) -> dict[str, Any]:
        owner = peer_agent_id or self.peer_agent_id(peer_agent or self.peer_agent)
        return post_json(
            self.url,
            "/store",
            {
                "peer_agent_id": owner,
                "assignments": [assignment_to_json(a) for a in assignments],
            },
            timeout=self.timeout,
        )

    def query(self, peer_agent_id: str, version: int) -> dict[str, Any]:
        return post_json(
            self.url,
            "/query",
            {"peer_agent_id": peer_agent_id, "version": int(version)},
            timeout=self.timeout,
        )

    def load(self, peer_agent_id: str, version: int) -> dict[str, Any]:
        return self.query(peer_agent_id, version)

    def delete(self, peer_agent_id: str, version: int) -> bool:
        result = post_json(
            self.url,
            "/delete",
            {"peer_agent_id": peer_agent_id, "version": int(version)},
            timeout=self.timeout,
        )
        return bool(result["deleted"])
