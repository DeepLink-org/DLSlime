//! NanoCtrl: Control plane server for NanoInfra
//!
//! NanoCtrl is stateless and supports multiple scopes sharing the same instance.
//! Scope is determined by clients (NanoRoute, EngineServer, peer_agent) via
//! `NANOCTRL_SCOPE` env var.

mod error;
mod handlers;
mod models;
mod net;
mod redis_repo;
mod state;

use axum::{
    routing::{get, post},
    Router,
};
use clap::{Args as ClapArgs, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::redis_repo::{LuaScripts, RedisRepo};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    server: ServerArgs,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run NanoCtrl server in the foreground.
    Server(ServerArgs),
    /// Start NanoCtrl server in the background.
    Start(StartArgs),
    /// Show NanoCtrl background server status.
    Status(StatusArgs),
    /// Stop NanoCtrl background server.
    Stop(StopArgs),
}

#[derive(ClapArgs, Clone, Debug)]
struct ServerArgs {
    /// Bind host for the HTTP server.
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    /// Bind port for the HTTP server.
    #[arg(long, default_value_t = 3000)]
    port: u16,
    /// Redis connection URL.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
}

#[derive(ClapArgs, Clone, Debug)]
struct StartArgs {
    #[command(flatten)]
    server: ServerArgs,
    /// Optional health-check host override.
    #[arg(long)]
    health_host: Option<String>,
    /// Optional health-check port override.
    #[arg(long)]
    health_port: Option<u16>,
    /// Log file path.
    #[arg(long)]
    log_file: Option<PathBuf>,
    /// Seconds to wait for health check.
    #[arg(long, default_value_t = 8.0)]
    wait: f64,
}

#[derive(ClapArgs, Clone, Debug)]
struct StatusArgs {
    /// Server address for health check.
    #[arg(long)]
    address: Option<String>,
}

#[derive(ClapArgs, Clone, Debug)]
struct StopArgs {
    /// Graceful stop timeout in seconds.
    #[arg(long, default_value_t = 8.0)]
    timeout: f64,
    /// Force kill if graceful stop times out.
    #[arg(long)]
    force: bool,
}

#[derive(Serialize, Deserialize)]
struct RuntimeMeta {
    pid: u32,
    address: String,
    cmd: Vec<String>,
    log: String,
    started_at: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("NANOCTRL_RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Server(args)) => run_server(args).await,
        Some(Commands::Start(args)) => start_server(args).await,
        Some(Commands::Status(args)) => status_server(args),
        Some(Commands::Stop(args)) => stop_server(args),
        None => run_server(cli.server).await,
    }
}

async fn run_server(args: ServerArgs) -> anyhow::Result<()> {
    let redis_url = std::env::var("NANOCTRL_REDIS_URL").unwrap_or(args.redis_url);
    tracing::info!("Using Redis URL: {}", redis_url);

    // Load Lua scripts from external files
    let scripts = LuaScripts::load()?;
    tracing::info!("Loaded embedded Lua scripts");

    // Create Redis repository with connection pool
    let repo = RedisRepo::new(&redis_url, scripts)?;
    tracing::info!("Redis connection pool initialized");

    // Warm up: verify Redis is reachable
    {
        tracing::info!("Warming up Redis connection...");
        let mut conn = repo.conn().await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to connect to Redis at {redis_url}. Please start Redis or set \
                 NANOCTRL_REDIS_URL / --redis-url to a reachable Redis instance. Details: {e}"
            )
        })?;
        let _: String = redis::cmd("PING")
            .query_async(&mut *conn)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                "Failed to ping Redis at {redis_url}. Please verify the Redis service is healthy. \
                 Details: {e}"
            )
            })?;
        tracing::info!("Redis connection established successfully");
    }

    let app = Router::new()
        // Health
        .route("/", get(handlers::util::root))
        // Generic heartbeat (unified for all entity types)
        .route("/heartbeat", post(handlers::util::heartbeat))
        // Peer agent
        .route("/start_peer_agent", post(handlers::peer::start_peer_agent))
        .route("/query", post(handlers::peer::query))
        .route("/cleanup", post(handlers::peer::cleanup))
        // RDMA
        .route(
            "/v1/desired_topology/:agent_id",
            post(handlers::rdma::set_desired_topology),
        )
        .route("/register_mr", post(handlers::rdma::register_mr))
        .route("/get_mr_info", post(handlers::rdma::get_mr_info))
        // Generic service/entity registry
        .route("/register", post(handlers::entity::register))
        .route("/unregister", post(handlers::entity::unregister))
        .route("/get_entity_info", post(handlers::entity::get_entity_info))
        .route("/list_entities", post(handlers::entity::list_entities))
        // Utility
        .route(
            "/get_redis_address",
            post(handlers::util::get_redis_address),
        )
        .layer(
            ServiceBuilder::new().layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &axum::http::Request<_>| {
                        tracing::info_span!(
                            "http_request",
                            method = %request.method(),
                            uri = %request.uri(),
                        )
                    })
                    .on_request(|request: &axum::http::Request<_>, _span: &tracing::Span| {
                        tracing::debug!("Incoming request: {} {}", request.method(), request.uri());
                    })
                    .on_response(
                        |response: &axum::http::Response<_>,
                         latency: std::time::Duration,
                         _span: &tracing::Span| {
                            tracing::info!(
                                status = %response.status(),
                                latency_us = latency.as_micros(),
                                "api done"
                            );
                        },
                    )
                    .on_failure(
                        |error: tower_http::classify::ServerErrorsFailureClass,
                         latency: std::time::Duration,
                         _span: &tracing::Span| {
                            tracing::error!("Request failed: {:?}, latency={:?}", error, latency);
                        },
                    ),
            ),
        )
        .with_state(repo);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .map_err(|e| {
            anyhow::anyhow!("Invalid server address {}:{}: {}", args.host, args.port, e)
        })?;
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn start_server(args: StartArgs) -> anyhow::Result<()> {
    let target_host = resolve_access_host(&args.server.host, args.health_host.as_deref());
    let target_port = args.health_port.unwrap_or(args.server.port);
    let target_address = format!("{target_host}:{target_port}");

    if let Some(pid) = read_pid()? {
        if is_pid_running(pid) {
            println!("nanoctrl is already running (pid={pid})");
            return Ok(());
        }
        cleanup_runtime()?;
    }

    if check_health(&target_address, Duration::from_millis(800)).is_ok() {
        println!(
            "nanoctrl appears to be already running at {} (no pid file managed by this CLI)",
            normalize_address(&target_address)
        );
        return Ok(());
    }

    let log_path = args.log_file.clone().unwrap_or_else(default_log_file);
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let log_start_offset = fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let err_log = log.try_clone()?;

    let exe = std::env::current_exe()?;
    let cmd_args = vec![
        "server".to_string(),
        "--host".to_string(),
        args.server.host.clone(),
        "--port".to_string(),
        args.server.port.to_string(),
        "--redis-url".to_string(),
        args.server.redis_url.clone(),
    ];
    let mut cmd = Command::new(&exe);
    cmd.args(&cmd_args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err_log));
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("Failed to start nanoctrl server. Details: {e}"))?;

    let server_address = normalize_address(&target_address);
    write_runtime(RuntimeMeta {
        pid: child.id(),
        address: server_address.clone(),
        cmd: std::iter::once(exe.display().to_string())
            .chain(cmd_args.iter().cloned())
            .collect(),
        log: log_path.display().to_string(),
        started_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    })?;

    let deadline = std::time::Instant::now() + Duration::from_secs_f64(args.wait);
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            cleanup_runtime()?;
            print_recent_log(&log_path, log_start_offset, 80);
            anyhow::bail!(
                "nanoctrl failed to start (exit={}). See log: {}",
                status,
                log_path.display()
            );
        }
        if check_health(&target_address, Duration::from_millis(800)).is_ok() {
            println!("Local node IP: {target_host}");
            println!();
            println!("nanoctrl started (pid={})", child.id());
            println!("address: {server_address}");
            println!("log: {}", log_path.display());
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    println!("Local node IP: {target_host}");
    println!();
    println!(
        "nanoctrl process started (pid={}), but health check timed out",
        child.id()
    );
    println!("address: {server_address}");
    println!("log: {}", log_path.display());
    print_recent_log(&log_path, log_start_offset, 80);
    Ok(())
}

fn status_server(args: StatusArgs) -> anyhow::Result<()> {
    let meta = read_meta()?;
    let address = args
        .address
        .or_else(|| meta.as_ref().map(|m| m.address.clone()))
        .unwrap_or_else(|| "http://127.0.0.1:3000".to_string());

    let Some(pid) = read_pid()? else {
        println!("nanoctrl is not running (no pid file)");
        print_health(&address);
        std::process::exit(1);
    };

    let running = is_pid_running(pid);
    println!("pid: {pid}");
    println!(
        "process: {}",
        if running { "running" } else { "not running" }
    );
    println!("address: {address}");
    print_health(&address);
    if let Some(meta) = meta {
        println!("log: {}", meta.log);
    }

    if !running {
        cleanup_runtime()?;
        std::process::exit(1);
    }
    Ok(())
}

fn stop_server(args: StopArgs) -> anyhow::Result<()> {
    let Some(pid) = read_pid()? else {
        println!("nanoctrl is not running");
        return Ok(());
    };

    if !is_pid_running(pid) {
        cleanup_runtime()?;
        println!("nanoctrl is not running (stale pid file cleaned)");
        return Ok(());
    }

    signal_pid(pid, "TERM")?;
    let deadline = std::time::Instant::now() + Duration::from_secs_f64(args.timeout);
    while std::time::Instant::now() < deadline {
        if !is_pid_running(pid) {
            cleanup_runtime()?;
            println!("nanoctrl stopped (pid={pid})");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    if args.force {
        signal_pid(pid, "KILL")?;
        cleanup_runtime()?;
        println!("nanoctrl killed (pid={pid})");
        return Ok(());
    }

    anyhow::bail!(
        "nanoctrl did not stop within {:.1}s; rerun with --force",
        args.timeout
    )
}

fn runtime_dir() -> PathBuf {
    if let Ok(path) = std::env::var("NANOCTRL_RUNTIME_DIR") {
        return PathBuf::from(path);
    }
    if let Ok(path) = std::env::var("XDG_RUNTIME_DIR") {
        return PathBuf::from(path).join("nanoctrl");
    }
    PathBuf::from("/tmp/nanoctrl")
}

fn pid_file() -> PathBuf {
    runtime_dir().join("nanoctrl.pid")
}

fn meta_file() -> PathBuf {
    runtime_dir().join("nanoctrl.meta.json")
}

fn default_log_file() -> PathBuf {
    runtime_dir().join("nanoctrl.log")
}

fn read_pid() -> anyhow::Result<Option<u32>> {
    let path = pid_file();
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    Ok(text.trim().parse::<u32>().ok())
}

fn read_meta() -> anyhow::Result<Option<RuntimeMeta>> {
    let path = meta_file();
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&text).ok())
}

fn write_runtime(meta: RuntimeMeta) -> anyhow::Result<()> {
    fs::create_dir_all(runtime_dir())?;
    fs::write(pid_file(), meta.pid.to_string())?;
    fs::write(meta_file(), serde_json::to_string_pretty(&meta)?)?;
    Ok(())
}

fn cleanup_runtime() -> anyhow::Result<()> {
    match fs::remove_file(pid_file()) {
        Ok(()) | Err(_) => {}
    }
    match fs::remove_file(meta_file()) {
        Ok(()) | Err(_) => {}
    }
    Ok(())
}

fn is_pid_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn signal_pid(pid: u32, signal: &str) -> anyhow::Result<()> {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()?;
    if status.success() || !is_pid_running(pid) {
        Ok(())
    } else {
        anyhow::bail!("failed to send SIG{signal} to pid {pid}")
    }
}

fn print_recent_log(path: &std::path::Path, start_offset: u64, max_lines: usize) {
    let Ok(mut file) = fs::File::open(path) else {
        return;
    };
    if file.seek(SeekFrom::Start(start_offset)).is_err() {
        return;
    };
    let mut text = String::new();
    if file.read_to_string(&mut text).is_err() {
        return;
    }
    let lines: Vec<&str> = text.lines().collect();
    if lines.is_empty() {
        return;
    }
    let start = lines.len().saturating_sub(max_lines);
    println!();
    println!("recent log:");
    for line in &lines[start..] {
        println!("{line}");
    }
}

fn normalize_address(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", address.trim_end_matches('/'))
    }
}

fn check_health(address: &str, timeout: Duration) -> anyhow::Result<()> {
    let normalized = normalize_address(address);
    let without_scheme = normalized
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("only http health checks are supported"))?;
    let addr = without_scheme
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve {without_scheme}"))?;
    let mut stream = std::net::TcpStream::connect_timeout(&addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(b"GET / HTTP/1.1\r\nHost: nanoctrl\r\nConnection: close\r\n\r\n")?;
    let mut buf = [0_u8; 64];
    let n = stream.read(&mut buf)?;
    let response = std::str::from_utf8(&buf[..n]).unwrap_or("");
    if response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        anyhow::bail!("health check returned non-200 response")
    }
}

fn print_health(address: &str) {
    match check_health(address, Duration::from_millis(1500)) {
        Ok(()) => println!("health: ok"),
        Err(e) => println!("health: down ({e})"),
    }
}

fn resolve_access_host(bind_host: &str, override_host: Option<&str>) -> String {
    if let Some(host) = override_host {
        return host.to_string();
    }
    if bind_host != "0.0.0.0" {
        return bind_host.to_string();
    }
    detect_hostname_ip()
        .or_else(detect_public_host)
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

fn detect_hostname_ip() -> Option<String> {
    for flag in ["-i", "-I"] {
        let output = Command::new("hostname").arg(flag).output().ok()?;
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(candidate) = stdout
            .split_whitespace()
            .find(|s| !s.is_empty() && *s != "0.0.0.0" && *s != "127.0.0.1")
        {
            return Some(candidate.to_string());
        }
    }
    None
}

fn detect_public_host() -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let host = socket.local_addr().ok()?.ip().to_string();
    if host == "0.0.0.0" || host == "127.0.0.1" {
        None
    } else {
        Some(host)
    }
}
