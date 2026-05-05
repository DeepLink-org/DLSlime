"""Tests for the dlslime-cache CLI parser."""

from urllib.error import URLError

from dlslime.cache.cli import (
    build_parser,
    cmd_status,
    DEFAULT_CTRL,
    default_log_file,
    internal_run_argv_from_cfg,
    meta_file,
    parse_size,
    parse_slab_size,
    pid_file,
    runtime_dir,
    service_mode_error,
)


def test_parser_program_name():
    assert build_parser().prog == "dlslime-cache"


def test_public_subcommands_are_lifecycle_only():
    parser = build_parser()

    assert sorted(parser._subcommands_action.choices) == ["start", "status", "stop"]


def test_start_parser_defaults():
    cfg = build_parser().parse_args(["start"])
    start = cfg.start

    assert cfg.subcommand == "start"
    assert start.host == "127.0.0.1"
    assert start.port == 8765
    assert parse_slab_size(start.slab_size) == 256 * 1024
    assert parse_size(start.memory_size) == 0
    assert start.ctrl == DEFAULT_CTRL
    assert start.service_id == "cache:0"
    assert start.peer_agent_alias is None
    assert start.cache_mr_name == "cache"
    assert start.metadata_only is False


def test_start_parser_accepts_peer_agent_and_nanoctrl_args():
    cfg = build_parser().parse_args(
        [
            "start",
            "--host=0.0.0.0",
            "--port=9000",
            "--slab-size=128K",
            "--memory-size=1G",
            "--ctrl=127.0.0.1:3000",
            "--scope=s0",
            "--service-id=cache:test",
            "--peer-agent-alias=cache-agent:0",
            "--peer-agent-device=mlx5_0",
            "--peer-agent-link-type=RoCE",
            "--peer-agent-qp-num=2",
            "--cache-mr-name=cache0",
        ]
    )
    start = cfg.start

    assert start.host == "0.0.0.0"
    assert start.port == 9000
    assert parse_slab_size(start.slab_size) == 128 * 1024
    assert parse_size(start.memory_size) == 1024**3
    assert start.ctrl == "127.0.0.1:3000"
    assert start.scope == "s0"
    assert start.service_id == "cache:test"
    assert start.peer_agent_alias == "cache-agent:0"
    assert start.peer_agent_device == "mlx5_0"
    assert start.peer_agent_link_type == "RoCE"
    assert start.peer_agent_qp_num == 2
    assert start.cache_mr_name == "cache0"


def test_start_parser_accepts_background_args_and_service_args():
    cfg = build_parser().parse_args(
        [
            "start",
            "--host=0.0.0.0",
            "--port=9001",
            "--slab-size=128K",
            "--memory-size=512M",
            "--quiet",
            "--ctrl=127.0.0.1:3000",
            "--service-id=cache:bg",
            "--health-host=127.0.0.1",
            "--health-port=9001",
            "--log-file=/tmp/dlslime-cache-test.log",
            "--wait=0.5",
        ]
    )
    start = cfg.start

    assert cfg.subcommand == "start"
    assert start.host == "0.0.0.0"
    assert start.port == 9001
    assert parse_slab_size(start.slab_size) == 128 * 1024
    assert parse_size(start.memory_size) == 512 * 1024**2
    assert start.quiet is True
    assert start.ctrl == "127.0.0.1:3000"
    assert start.service_id == "cache:bg"
    assert start.health_host == "127.0.0.1"
    assert start.health_port == 9001
    assert str(start.log_file) == "/tmp/dlslime-cache-test.log"
    assert start.wait == 0.5


def test_start_cfg_can_be_replayed_as_internal_run_argv():
    cfg = build_parser().parse_args(
        [
            "start",
            "--host=0.0.0.0",
            "--port=9002",
            "--quiet",
            "--peer-agent-alias=cache-agent:0",
            "--peer-agent-device=mlx5_0",
            "--metadata-only",
        ]
    )

    argv = internal_run_argv_from_cfg(cfg.start)

    assert argv[:9] == [
        "__run",
        "--host",
        "0.0.0.0",
        "--port",
        "9002",
        "--slab-size",
        "262144",
        "--memory-size",
        "0",
    ]
    assert "--quiet" in argv
    assert "--peer-agent-alias" in argv
    assert "cache-agent:0" in argv
    assert "--peer-agent-device" in argv
    assert "mlx5_0" in argv
    assert "--cache-mr-name" in argv
    assert "cache" in argv
    assert "--metadata-only" in argv


def test_start_data_mode_requires_memory_size():
    cfg = build_parser().parse_args(["start"]).start

    assert "requires --memory-size > 0" in service_mode_error(cfg)


def test_start_metadata_only_allows_zero_memory_size():
    cfg = build_parser().parse_args(["start", "--metadata-only"]).start

    assert service_mode_error(cfg) is None


def test_start_rejects_slab_size_outside_supported_range():
    cfg = (
        build_parser()
        .parse_args(["start", "--slab-size=64K", "--memory-size=1G"])
        .start
    )

    assert "[128K, 1G]" in service_mode_error(cfg)


def test_start_rejects_memory_size_not_aligned_to_slab_size():
    cfg = (
        build_parser()
        .parse_args(["start", "--slab-size=256K", "--memory-size=384K"])
        .start
    )

    assert "multiple of --slab-size" in service_mode_error(cfg)


def test_runtime_dir_env_override(monkeypatch):
    monkeypatch.setenv("DLSLIME_CACHE_RUNTIME_DIR", "/tmp/dlslime-cache-tests")

    assert str(runtime_dir()) == "/tmp/dlslime-cache-tests"


def test_runtime_files_use_dlslime_cache_name(monkeypatch):
    monkeypatch.setenv("DLSLIME_CACHE_RUNTIME_DIR", "/tmp/dlslime-cache-tests")

    assert str(pid_file()) == "/tmp/dlslime-cache-tests/dlslime-cache.pid"
    assert str(meta_file()) == "/tmp/dlslime-cache-tests/dlslime-cache.meta.json"
    assert str(default_log_file()) == "/tmp/dlslime-cache-tests/dlslime-cache.log"


def test_parse_size_accepts_binary_suffixes():
    assert parse_size("0") == 0
    assert parse_size("256K") == 256 * 1024
    assert parse_size("4G") == 4 * 1024**3
    assert parse_size("1GiB") == 1024**3


def test_parse_slab_size_accepts_supported_range():
    assert parse_slab_size("128K") == 128 * 1024
    assert parse_slab_size("256K") == 256 * 1024
    assert parse_slab_size("1G") == 1024**3


def test_parse_slab_size_rejects_out_of_range():
    for value in ("64K", str(1024**3 + 1)):
        try:
            parse_slab_size(value)
        except ValueError as exc:
            assert "[128K, 1G]" in str(exc)
        else:
            raise AssertionError(f"expected parse_slab_size({value!r}) to fail")


def test_status_without_pid_is_human_readable(monkeypatch, capsys):
    monkeypatch.setenv("DLSLIME_CACHE_RUNTIME_DIR", "/tmp/dlslime-cache-tests")

    def fail_health(address, timeout=1.5):
        raise URLError(ConnectionRefusedError(111, "Connection refused"))

    monkeypatch.setattr("dlslime.cache.cli.check_health", fail_health)
    cfg = build_parser().parse_args(["status"]).status

    assert cmd_status(cfg) == 1
    out = capsys.readouterr().out

    assert "dlslime-cache is not running" in out
    assert "reason: no pid file at /tmp/dlslime-cache-tests/dlslime-cache.pid" in out
    assert "address: http://127.0.0.1:8765" in out
    assert "health: down (connection refused; no cache service is listening)" in out
    assert "hint: start it with `dlslime-cache start`" in out
    assert "<urlopen error" not in out
