//! `nanoctrl obs` subcommands: query observability snapshots from Redis.
//!
//! Each PeerAgent (when `DLSLIME_OBS=1`) periodically writes a JSON snapshot
//! to `{scope}:obs:peer:{peer_id}`. This module scans those keys and
//! presents them as human-readable tables or `--json` output.

use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ────────────────────────── CLI args ──────────────────────────

#[derive(ClapArgs, Clone, Debug)]
pub struct ObsArgs {
    #[command(subcommand)]
    pub command: ObsCommands,
}

#[derive(Subcommand, Clone, Debug)]
pub enum ObsCommands {
    /// Cluster-wide summary (peers alive, total traffic, errors).
    Status(ObsQueryArgs),
    /// Per-peer breakdown table.
    Peers(ObsQueryArgs),
    /// Per-NIC breakdown table.
    Nics(ObsQueryArgs),
}

#[derive(ClapArgs, Clone, Debug)]
pub struct ObsQueryArgs {
    /// Redis connection URL.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    pub redis_url: String,
    /// Scope prefix (same as NANOCTRL_SCOPE on agents).
    #[arg(long)]
    pub scope: Option<String>,
    /// Snapshots older than this are marked stale (ms).
    #[arg(long, default_value_t = 45000)]
    pub stale_ms: u64,
    /// Output raw JSON instead of table.
    #[arg(long)]
    pub json: bool,
}

// ────────────────────────── Data types ──────────────────────────

#[derive(Debug)]
#[allow(dead_code)]
struct ObsSnapshot {
    key: String,
    peer_id: String,
    host: String,
    pid: u64,
    reported_at_ms: u64,
    age_ms: u64,
    stale: bool,
    summary: Value,
    nics: Vec<Value>,
    ewma_bandwidth_bps: f64,
    connections: Vec<Value>,
    raw: Value,
}

// ────────────────────────── Entry point ──────────────────────────

pub fn run_obs(args: ObsArgs) -> Result<()> {
    match args.command {
        ObsCommands::Status(q) => cmd_status(q),
        ObsCommands::Peers(q) => cmd_peers(q),
        ObsCommands::Nics(q) => cmd_nics(q),
    }
}

// ────────────────────────── Scan helper ──────────────────────────

fn scan_snapshots(args: &ObsQueryArgs) -> Result<Vec<ObsSnapshot>> {
    let redis_url = std::env::var("NANOCTRL_REDIS_URL").unwrap_or_else(|_| args.redis_url.clone());
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client.get_connection()?;

    let pattern = match &args.scope {
        Some(s) if !s.is_empty() => format!("{s}:obs:peer:*"),
        _ => "obs:peer:*".to_string(),
    };

    let keys: Vec<String> = redis::cmd("KEYS").arg(&pattern).query(&mut conn)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let mut snapshots = Vec::new();
    for key in &keys {
        let val: Option<String> = redis::cmd("GET").arg(key).query(&mut conn)?;
        let Some(val_str) = val else { continue };
        let Ok(raw) = serde_json::from_str::<Value>(&val_str) else {
            continue;
        };

        let reported_at_ms = raw
            .get("reported_at_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let age_ms = now_ms.saturating_sub(reported_at_ms);

        snapshots.push(ObsSnapshot {
            key: key.clone(),
            peer_id: raw
                .get("peer_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            host: raw
                .get("host")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            pid: raw.get("pid").and_then(Value::as_u64).unwrap_or(0),
            reported_at_ms,
            age_ms,
            stale: age_ms > args.stale_ms,
            summary: raw.get("summary").cloned().unwrap_or(Value::Null),
            nics: raw
                .get("nics")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            ewma_bandwidth_bps: raw
                .get("ewma_bandwidth_bps")
                .and_then(Value::as_f64)
                .unwrap_or(0.0),
            connections: raw
                .get("connections")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            raw,
        });
    }

    // Sort by peer_id for stable output
    snapshots.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
    Ok(snapshots)
}

// ────────────────────────── Formatters ──────────────────────────

fn human_bytes(b: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;
    if b >= TB {
        format!("{:.1}TB", b as f64 / TB as f64)
    } else if b >= GB {
        format!("{:.1}GB", b as f64 / GB as f64)
    } else if b >= MB {
        format!("{:.1}MB", b as f64 / MB as f64)
    } else if b >= KB {
        format!("{:.1}KB", b as f64 / KB as f64)
    } else {
        format!("{b}B")
    }
}

fn human_bps(bps: f64) -> String {
    if bps >= 1e12 {
        format!("{:.1}Tbps", bps / 1e12)
    } else if bps >= 1e9 {
        format!("{:.1}Gbps", bps / 1e9)
    } else if bps >= 1e6 {
        format!("{:.1}Mbps", bps / 1e6)
    } else if bps >= 1e3 {
        format!("{:.1}Kbps", bps / 1e3)
    } else {
        format!("{:.0}bps", bps)
    }
}

fn human_age(ms: u64) -> String {
    if ms >= 60_000 {
        format!("{}m", ms / 60_000)
    } else if ms >= 1_000 {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}

fn get_u64(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn get_i64(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

// ────────────────────────── Commands ──────────────────────────

fn cmd_status(args: ObsQueryArgs) -> Result<()> {
    let snapshots = scan_snapshots(&args)?;

    if args.json {
        let mut out = BTreeMap::new();
        let alive = snapshots.iter().filter(|s| !s.stale).count();
        let stale = snapshots.iter().filter(|s| s.stale).count();
        let total_assign: u64 = snapshots
            .iter()
            .map(|s| get_u64(&s.summary, "assign_total"))
            .sum();
        let total_bytes: u64 = snapshots
            .iter()
            .map(|s| get_u64(&s.summary, "completed_bytes_total"))
            .sum();
        let total_pending: i64 = snapshots
            .iter()
            .map(|s| get_i64(&s.summary, "pending_ops"))
            .sum();
        let total_errors: u64 = snapshots
            .iter()
            .map(|s| get_u64(&s.summary, "error_total"))
            .sum();
        let total_bw: f64 = snapshots.iter().map(|s| s.ewma_bandwidth_bps).sum();

        out.insert("alive", serde_json::json!(alive));
        out.insert("stale", serde_json::json!(stale));
        out.insert("assign_total", serde_json::json!(total_assign));
        out.insert("completed_bytes_total", serde_json::json!(total_bytes));
        out.insert("pending_ops", serde_json::json!(total_pending));
        out.insert("error_total", serde_json::json!(total_errors));
        out.insert("ewma_bandwidth_bps", serde_json::json!(total_bw));

        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    let scope_str = args.scope.as_deref().unwrap_or("(none)");
    println!("Redis: {}  Scope: {}", args.redis_url, scope_str);

    let alive = snapshots.iter().filter(|s| !s.stale).count();
    let stale = snapshots.iter().filter(|s| s.stale).count();
    println!("Peers: {} alive, {} stale", alive, stale);

    let total_assign: u64 = snapshots
        .iter()
        .map(|s| get_u64(&s.summary, "assign_total"))
        .sum();
    let total_batch: u64 = snapshots
        .iter()
        .map(|s| get_u64(&s.summary, "batch_total"))
        .sum();
    let total_bytes: u64 = snapshots
        .iter()
        .map(|s| get_u64(&s.summary, "completed_bytes_total"))
        .sum();
    let total_pending: i64 = snapshots
        .iter()
        .map(|s| get_i64(&s.summary, "pending_ops"))
        .sum();
    let total_errors: u64 = snapshots
        .iter()
        .map(|s| get_u64(&s.summary, "error_total"))
        .sum();
    let total_bw: f64 = snapshots.iter().map(|s| s.ewma_bandwidth_bps).sum();

    println!(
        "Total: assign={} batch={} bytes={} pending={} errors={}",
        total_assign,
        total_batch,
        human_bytes(total_bytes),
        total_pending,
        total_errors,
    );
    println!("Estimated BW: {} (EWMA)", human_bps(total_bw));

    Ok(())
}

fn cmd_peers(args: ObsQueryArgs) -> Result<()> {
    let snapshots = scan_snapshots(&args)?;

    if args.json {
        let arr: Vec<&Value> = snapshots.iter().map(|s| &s.raw).collect();
        println!("{}", serde_json::to_string_pretty(&arr)?);
        return Ok(());
    }

    // Table header
    println!(
        "{:<12} {:<15} {:<8} {:<6} {:<6} {:>8} {:>8} {:>10} {:>10} {:>8} {:>7}",
        "PEER",
        "HOST",
        "PID",
        "AGE",
        "STATE",
        "ASSIGN",
        "BATCH",
        "BW",
        "BYTES",
        "PENDING",
        "ERRORS"
    );

    for s in &snapshots {
        let state = if s.stale { "stale" } else { "alive" };
        println!(
            "{:<12} {:<15} {:<8} {:<6} {:<6} {:>8} {:>8} {:>10} {:>10} {:>8} {:>7}",
            s.peer_id,
            s.host,
            s.pid,
            human_age(s.age_ms),
            state,
            get_u64(&s.summary, "assign_total"),
            get_u64(&s.summary, "batch_total"),
            human_bps(s.ewma_bandwidth_bps),
            human_bytes(get_u64(&s.summary, "completed_bytes_total")),
            get_i64(&s.summary, "pending_ops"),
            get_u64(&s.summary, "error_total"),
        );
    }

    if snapshots.is_empty() {
        println!("(no obs snapshots found)");
    }

    Ok(())
}

fn cmd_nics(args: ObsQueryArgs) -> Result<()> {
    let snapshots = scan_snapshots(&args)?;

    if args.json {
        let mut arr = Vec::new();
        for s in &snapshots {
            for nic in &s.nics {
                let mut obj = nic.clone();
                if let Some(map) = obj.as_object_mut() {
                    map.insert("peer_id".to_string(), Value::String(s.peer_id.clone()));
                    map.insert("age_ms".to_string(), serde_json::json!(s.age_ms));
                }
                arr.push(obj);
            }
        }
        println!("{}", serde_json::to_string_pretty(&arr)?);
        return Ok(());
    }

    println!(
        "{:<12} {:<12} {:<6} {:>8} {:>8} {:>10} {:>10} {:>8} {:>7} {:>9} {:>6}",
        "PEER",
        "NIC",
        "AGE",
        "ASSIGN",
        "BATCH",
        "BW(post)",
        "BYTES",
        "PENDING",
        "ERRORS",
        "POST_FAIL",
        "CQ_ERR"
    );

    for s in &snapshots {
        for nic in &s.nics {
            let nic_name = nic.get("nic").and_then(Value::as_str).unwrap_or("?");
            let post_bytes = get_u64(nic, "post_bytes_total");
            // Estimate NIC-level BW from post bytes (rough, since we don't have per-NIC EWMA)
            let nic_bw_str = human_bytes(post_bytes);

            println!(
                "{:<12} {:<12} {:<6} {:>8} {:>8} {:>10} {:>10} {:>8} {:>7} {:>9} {:>6}",
                s.peer_id,
                nic_name,
                human_age(s.age_ms),
                get_u64(nic, "assign_total"),
                get_u64(nic, "batch_total"),
                nic_bw_str,
                human_bytes(get_u64(nic, "completed_bytes_total")),
                get_i64(nic, "pending_ops"),
                get_u64(nic, "error_total"),
                get_u64(nic, "post_failures_total"),
                get_u64(nic, "cq_errors_total"),
            );
        }
    }

    if snapshots.is_empty() {
        println!("(no obs snapshots found)");
    }

    Ok(())
}
