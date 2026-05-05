"""Command line entry point for DLSlimeCache."""

from __future__ import annotations

import contextlib
import errno
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Sequence

from jsonargparse import ActionConfigFile, ArgumentParser

from .service import (
    CacheHttpServer,
    DEFAULT_CACHE_MR_NAME,
    DEFAULT_MEMORY_SIZE,
    DEFAULT_SLAB_SIZE,
    NanoCtrlRegistration,
    resolve_host_for_registration,
)

PROGRAM_NAME = "dlslime-cache"
INTERNAL_RUN_COMMAND = "__run"
DEFAULT_CTRL = "http://127.0.0.1:3000"
MIN_SLAB_SIZE = 128 * 1024
MAX_SLAB_SIZE = 1024**3


def parse_size(value) -> int:
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError("size must not be empty")
    units = {
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gib": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
        "tib": 1024**4,
    }
    lower = text.lower()
    for suffix in sorted(units, key=len, reverse=True):
        if lower.endswith(suffix):
            number = lower[: -len(suffix)].strip()
            if not number:
                raise ValueError(f"invalid size: {value!r}")
            return int(number) * units[suffix]
    return int(text)


def parse_slab_size(value) -> int:
    size = parse_size(value)
    if size < MIN_SLAB_SIZE or size > MAX_SLAB_SIZE:
        raise ValueError("--slab-size must be in [128K, 1G]")
    return size


def _add_service_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--config", action=ActionConfigFile, help="Path to a jsonargparse config file."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host.")
    parser.add_argument("--port", type=int, default=8765, help="Bind port.")
    parser.add_argument(
        "--slab-size",
        type=str,
        default=str(DEFAULT_SLAB_SIZE),
        help="Maximum assignment slab bytes in [128K, 1G]. Supports K/M/G suffixes.",
    )
    parser.add_argument(
        "--memory-size",
        type=str,
        default=str(DEFAULT_MEMORY_SIZE),
        help="Preallocated logical cache memory bytes; 0 disables capacity checks. Supports K/M/G suffixes.",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress HTTP access logs."
    )

    parser.add_argument(
        "--ctrl",
        default=DEFAULT_CTRL,
        help="NanoCtrl address for cache PeerAgent and service registration.",
    )
    parser.add_argument("--scope", default=None, help="Optional NanoCtrl scope.")
    parser.add_argument(
        "--service-id",
        default="cache:0",
        help="NanoCtrl service id when --ctrl is set.",
    )
    parser.add_argument(
        "--advertise-host",
        default=None,
        help="Host stored in NanoCtrl; defaults to --host.",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=15.0,
        help="NanoCtrl heartbeat interval seconds.",
    )

    parser.add_argument(
        "--peer-agent-alias",
        default=None,
        help="Optional PeerAgent alias to compose into the service.",
    )
    parser.add_argument(
        "--peer-agent-ctrl",
        default=None,
        help="NanoCtrl URL for the composed PeerAgent.",
    )
    parser.add_argument(
        "--peer-agent-device",
        default=None,
        help="Preferred RDMA device for the composed PeerAgent.",
    )
    parser.add_argument(
        "--peer-agent-ib-port",
        type=int,
        default=1,
        help="Preferred IB port for the composed PeerAgent.",
    )
    parser.add_argument(
        "--peer-agent-link-type",
        default=None,
        help="Preferred link type for the composed PeerAgent.",
    )
    parser.add_argument(
        "--peer-agent-qp-num",
        type=int,
        default=1,
        help="QP count for the composed PeerAgent.",
    )
    parser.add_argument(
        "--cache-mr-name",
        default=DEFAULT_CACHE_MR_NAME,
        help="PeerAgent MR name for cache storage.",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Run without PeerAgent/RDMA cache storage.",
    )


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog=PROGRAM_NAME,
        description="DLSlimeCache service CLI.",
    )
    subcommands = parser.add_subcommands(required=True)

    start = ArgumentParser(
        description="Start a DLSlimeCache service in the background."
    )
    _add_service_args(start)
    start.add_argument(
        "--health-host", default=None, help="Optional health-check host override."
    )
    start.add_argument(
        "--health-port",
        type=int,
        default=None,
        help="Optional health-check port override.",
    )
    start.add_argument("--log-file", type=Path, default=None, help="Log file path.")
    start.add_argument(
        "--wait", type=float, default=8.0, help="Seconds to wait for health check."
    )
    subcommands.add_subcommand("start", start)

    status = ArgumentParser(description="Show DLSlimeCache background service status.")
    status.add_argument(
        "--address", default=None, help="Service address for health check."
    )
    subcommands.add_subcommand("status", status)

    stop = ArgumentParser(description="Stop a DLSlimeCache background service.")
    stop.add_argument(
        "--timeout", type=float, default=8.0, help="Graceful stop timeout in seconds."
    )
    stop.add_argument(
        "--force", action="store_true", help="Force kill if graceful stop times out."
    )
    subcommands.add_subcommand("stop", stop)
    return parser


def build_internal_run_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog=f"{PROGRAM_NAME} {INTERNAL_RUN_COMMAND}",
        description="Run a DLSlimeCache service in the foreground.",
    )
    _add_service_args(parser)
    return parser


def runtime_dir() -> Path:
    if path := os.environ.get("DLSLIME_CACHE_RUNTIME_DIR"):
        return Path(path)
    if path := os.environ.get("XDG_RUNTIME_DIR"):
        return Path(path) / "dlslime-cache"
    return Path("/tmp/dlslime-cache")


def pid_file() -> Path:
    return runtime_dir() / f"{PROGRAM_NAME}.pid"


def meta_file() -> Path:
    return runtime_dir() / f"{PROGRAM_NAME}.meta.json"


def default_log_file() -> Path:
    return runtime_dir() / f"{PROGRAM_NAME}.log"


def read_pid() -> int | None:
    try:
        return int(pid_file().read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def read_meta() -> dict | None:
    try:
        return json.loads(meta_file().read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_runtime(meta: dict) -> None:
    runtime_dir().mkdir(parents=True, exist_ok=True)
    pid_file().write_text(str(meta["pid"]), encoding="utf-8")
    meta_file().write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")


def cleanup_runtime() -> None:
    with contextlib.suppress(FileNotFoundError):
        pid_file().unlink()
    with contextlib.suppress(FileNotFoundError):
        meta_file().unlink()


def is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def signal_pid(pid: int, sig: signal.Signals) -> None:
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        return


def normalize_address(address: str) -> str:
    if address.startswith(("http://", "https://")):
        return address.rstrip("/")
    return f"http://{address.rstrip('/')}"


def resolve_access_host(bind_host: str, override_host: str | None = None) -> str:
    if override_host:
        return override_host
    if bind_host in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return bind_host


def service_address(host: str, port: int) -> str:
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return normalize_address(f"{host}:{port}")


def check_health(address: str, timeout: float = 1.5) -> None:
    req = urllib.request.Request(f"{normalize_address(address)}/healthz", method="GET")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=timeout) as resp:
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"health check returned HTTP {resp.status}")


def health_error_message(exc: BaseException) -> str:
    reason = getattr(exc, "reason", exc)
    err_no = getattr(reason, "errno", None)
    if isinstance(reason, ConnectionRefusedError) or err_no == errno.ECONNREFUSED:
        return "connection refused; no cache service is listening"
    if isinstance(reason, PermissionError) or err_no in {errno.EACCES, errno.EPERM}:
        return "permission denied while checking health"
    if isinstance(reason, TimeoutError) or err_no == errno.ETIMEDOUT:
        return "connection timed out"
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    text = str(reason).strip() or reason.__class__.__name__
    return text


def print_health(address: str) -> None:
    try:
        check_health(address)
    except (OSError, RuntimeError, urllib.error.URLError) as exc:
        print(f"health: down ({health_error_message(exc)})")
    else:
        print("health: ok")


def print_recent_log(path: Path, start_offset: int = 0, max_lines: int = 80) -> None:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            f.seek(start_offset)
            lines = f.read().splitlines()
    except OSError:
        return
    if not lines:
        return
    print()
    print("recent log:")
    for line in lines[-max_lines:]:
        print(line)


def internal_run_argv_from_cfg(cfg) -> list[str]:
    memory_size = parse_size(cfg.memory_size)
    slab_size = parse_slab_size(cfg.slab_size)
    argv = [
        INTERNAL_RUN_COMMAND,
        "--host",
        str(cfg.host),
        "--port",
        str(cfg.port),
        "--slab-size",
        str(slab_size),
        "--memory-size",
        str(memory_size),
    ]
    if cfg.quiet:
        argv.append("--quiet")
    if cfg.metadata_only:
        argv.append("--metadata-only")

    for name in (
        "ctrl",
        "scope",
        "service_id",
        "advertise_host",
        "heartbeat_interval",
        "peer_agent_alias",
        "peer_agent_ctrl",
        "peer_agent_device",
        "peer_agent_ib_port",
        "peer_agent_link_type",
        "peer_agent_qp_num",
        "cache_mr_name",
    ):
        value = getattr(cfg, name)
        if value is not None:
            argv.extend([f"--{name.replace('_', '-')}", str(value)])
    return argv


def _make_peer_agent(cfg):
    if cfg.metadata_only:
        return None
    if cfg.ctrl is None and cfg.peer_agent_alias is None:
        return None

    from dlslime import PeerAgent

    peer_agent_alias = cfg.peer_agent_alias
    if peer_agent_alias is None:
        peer_agent_alias = f"{str(cfg.service_id).replace('cache:', 'cache-agent:', 1)}"
    server_url = cfg.peer_agent_ctrl or cfg.ctrl or "http://127.0.0.1:3000"
    return PeerAgent(
        alias=peer_agent_alias,
        server_url=server_url,
        device=cfg.peer_agent_device,
        ib_port=cfg.peer_agent_ib_port,
        link_type=cfg.peer_agent_link_type,
        qp_num=cfg.peer_agent_qp_num,
        scope=cfg.scope,
    )


def service_mode_error(cfg) -> str | None:
    try:
        slab_size = parse_slab_size(cfg.slab_size)
        memory_size = parse_size(cfg.memory_size)
    except ValueError as exc:
        return str(exc)
    if cfg.metadata_only:
        return None
    if memory_size <= 0:
        return (
            "dlslime-cache data mode requires --memory-size > 0. "
            "Use --metadata-only only for metadata/control-plane tests."
        )
    if memory_size % slab_size != 0:
        return "dlslime-cache --memory-size must be a multiple of --slab-size"
    if cfg.ctrl is None and cfg.peer_agent_ctrl is None:
        return (
            "dlslime-cache data mode requires NanoCtrl. "
            "Pass --ctrl http://127.0.0.1:3000 or use --metadata-only."
        )
    return None


def cmd_run_foreground(cfg) -> int:
    if error := service_mode_error(cfg):
        print(error, file=sys.stderr)
        return 2

    memory_size = parse_size(cfg.memory_size)
    slab_size = parse_slab_size(cfg.slab_size)
    peer_agent = _make_peer_agent(cfg)
    server = CacheHttpServer(
        (cfg.host, cfg.port),
        peer_agent,
        slab_size=slab_size,
        memory_size=memory_size,
        cache_mr_name=cfg.cache_mr_name,
        quiet=cfg.quiet,
    )
    registration = None

    if cfg.ctrl is not None and not cfg.metadata_only:
        registration = NanoCtrlRegistration(
            ctrl=cfg.ctrl,
            service_id=cfg.service_id,
            host=cfg.host,
            port=cfg.port,
            scope=cfg.scope,
            advertise_host=cfg.advertise_host,
            heartbeat_interval=cfg.heartbeat_interval,
        )
        registration.start()

    def shutdown(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, shutdown)

    print(
        f"DLSlimeCache serving on http://{cfg.host}:{cfg.port} "
        f"(slab_size={slab_size}, memory_size={memory_size})",
        flush=True,
    )
    if registration is not None:
        advertised = cfg.advertise_host or resolve_host_for_registration(cfg.host)
        print(
            f"registered with NanoCtrl {cfg.ctrl}: service_id={cfg.service_id} "
            f"role=cache endpoint=http://{advertised}:{cfg.port}",
            flush=True,
        )
    if peer_agent is not None:
        print(f"composed PeerAgent alias={peer_agent.alias}", flush=True)
        if memory_size > 0:
            print(f"registered cache MR name={cfg.cache_mr_name}", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if registration is not None:
            registration.stop()
        if peer_agent is not None:
            with contextlib.suppress(Exception):
                peer_agent.shutdown()
        print("DLSlimeCache stopped.", file=sys.stderr, flush=True)
    return 0


def cmd_start(cfg) -> int:
    if error := service_mode_error(cfg):
        print(error, file=sys.stderr)
        return 2

    target_host = resolve_access_host(cfg.host, cfg.health_host)
    target_port = cfg.health_port or cfg.port
    target_address = service_address(target_host, target_port)

    pid = read_pid()
    if pid is not None:
        if is_pid_running(pid):
            print(f"{PROGRAM_NAME} is already running (pid={pid})")
            return 0
        cleanup_runtime()

    with contextlib.suppress(Exception):
        check_health(target_address, timeout=0.8)
        print(
            f"{PROGRAM_NAME} appears to be already running at "
            f"{target_address} (no pid file managed by this CLI)"
        )
        return 0

    log_path = cfg.log_file or default_log_file()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_start_offset = log_path.stat().st_size if log_path.exists() else 0

    run_argv = internal_run_argv_from_cfg(cfg)
    cmd = [sys.executable, "-m", "dlslime.cache.cli", *run_argv]
    with log_path.open("a", encoding="utf-8") as out:
        child = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    write_runtime(
        {
            "pid": child.pid,
            "address": target_address,
            "cmd": cmd,
            "log": str(log_path),
            "started_at": int(time.time()),
        }
    )

    deadline = time.monotonic() + float(cfg.wait)
    while time.monotonic() < deadline:
        status = child.poll()
        if status is not None:
            cleanup_runtime()
            print_recent_log(log_path, log_start_offset)
            print(
                f"{PROGRAM_NAME} failed to start (exit={status}). See log: {log_path}",
                file=sys.stderr,
            )
            return 1
        with contextlib.suppress(Exception):
            check_health(target_address, timeout=0.8)
            print(f"{PROGRAM_NAME} started (pid={child.pid})")
            print(f"address: {target_address}")
            print(f"log: {log_path}")
            return 0
        time.sleep(0.2)

    print(
        f"{PROGRAM_NAME} process started (pid={child.pid}), but health check timed out"
    )
    print(f"address: {target_address}")
    print(f"log: {log_path}")
    print_recent_log(log_path, log_start_offset)
    return 0


def cmd_status(cfg) -> int:
    meta = read_meta()
    address = cfg.address or (meta or {}).get("address") or "http://127.0.0.1:8765"
    pid = read_pid()
    if pid is None:
        print(f"{PROGRAM_NAME} is not running")
        print(f"reason: no pid file at {pid_file()}")
        print(f"address: {address}")
        print_health(address)
        print(f"hint: start it with `{PROGRAM_NAME} start`")
        return 1

    running = is_pid_running(pid)
    print(f"pid: {pid}")
    print(f"process: {'running' if running else 'not running'}")
    print(f"address: {address}")
    print_health(address)
    if meta is not None:
        print(f"log: {meta.get('log')}")

    if not running:
        cleanup_runtime()
        return 1
    return 0


def cmd_stop(cfg) -> int:
    pid = read_pid()
    if pid is None:
        print(f"{PROGRAM_NAME} is not running")
        return 0

    if not is_pid_running(pid):
        cleanup_runtime()
        print(f"{PROGRAM_NAME} is not running (stale pid file cleaned)")
        return 0

    signal_pid(pid, signal.SIGTERM)
    deadline = time.monotonic() + float(cfg.timeout)
    while time.monotonic() < deadline:
        if not is_pid_running(pid):
            cleanup_runtime()
            print(f"{PROGRAM_NAME} stopped (pid={pid})")
            return 0
        time.sleep(0.2)

    if cfg.force:
        signal_pid(pid, signal.SIGKILL)
        cleanup_runtime()
        print(f"{PROGRAM_NAME} killed (pid={pid})")
        return 0

    print(
        f"{PROGRAM_NAME} did not stop within {float(cfg.timeout):.1f}s; rerun with --force",
        file=sys.stderr,
    )
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    if args and args[0] == INTERNAL_RUN_COMMAND:
        cfg = build_internal_run_parser().parse_args(args[1:])
        return cmd_run_foreground(cfg)

    parser = build_parser()
    cfg = parser.parse_args(args)
    if cfg.subcommand == "start":
        return cmd_start(cfg.start)
    if cfg.subcommand == "status":
        return cmd_status(cfg.status)
    if cfg.subcommand == "stop":
        return cmd_stop(cfg.stop)
    parser.error(f"unknown subcommand: {cfg.subcommand}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
