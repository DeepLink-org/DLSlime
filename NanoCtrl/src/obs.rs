//! `nanoctrl obs` subcommands: query observability snapshots from Redis.
//!
//! Each PeerAgent (when `DLSLIME_OBS=1`) periodically writes a JSON snapshot
//! to `{scope}:obs:peer:{peer_id}`. This module scans those keys and
//! presents them as human-readable tables or `--json` output.

use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use comfy_table::{presets::UTF8_FULL, Cell, CellAlignment, Table};
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
    /// Per-connection catalog (directed links between peers).
    ///
    /// v0 lists SRC/DST peer+NIC and STATE derived from each peer's
    /// reported `connections` list. BW / BYTES / PENDING / ERRORS are
    /// not yet accounted per connection — they render as `-` and will
    /// land in a follow-up PR that adds peer-pair dimensions to the
    /// C++ counters.
    Links(ObsQueryArgs),
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
    // Explicit lifecycle marker written by PeerAgent on graceful shutdown
    // (status = "stopped"). Absent for alive snapshots. Takes precedence
    // over the age-derived alive/stale state in the STATE column.
    status: Option<String>,
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
        ObsCommands::Links(q) => cmd_links(q),
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

    // SCAN, not KEYS: KEYS is O(N) and blocks Redis in real deployments.
    // SCAN iterates the keyspace in chunks and is safe under load.
    let mut keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next_cursor, chunk): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(200)
            .query(&mut conn)?;
        keys.extend(chunk);
        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Bulk fetch with MGET (empty vec → skip the round-trip entirely).
    let values: Vec<Option<String>> = if keys.is_empty() {
        Vec::new()
    } else {
        redis::cmd("MGET").arg(&keys).query(&mut conn)?
    };

    let mut snapshots = Vec::new();
    for (key, val) in keys.iter().zip(values.into_iter()) {
        let Some(val_str) = val else { continue };
        if let Some(snap) = parse_snapshot(key, &val_str, now_ms, args.stale_ms) {
            snapshots.push(snap);
        }
    }

    // Sort by peer_id for stable output
    snapshots.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
    Ok(snapshots)
}

/// Parse one Redis value into an ObsSnapshot. Extracted as a pure
/// function so it can be unit-tested without a live Redis instance.
fn parse_snapshot(key: &str, val_str: &str, now_ms: u64, stale_ms: u64) -> Option<ObsSnapshot> {
    let raw: Value = serde_json::from_str(val_str).ok()?;

    let reported_at_ms = raw
        .get("reported_at_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let age_ms = now_ms.saturating_sub(reported_at_ms);

    Some(ObsSnapshot {
        key: key.to_string(),
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
        stale: age_ms > stale_ms,
        status: raw
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_string),
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
    })
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

/// Map an ObsSnapshot to the STATE column string.
///
/// Priority: explicit `status` marker (e.g. "stopped" from graceful
/// shutdown) overrides the age-derived state. Otherwise fall back to
/// the stale/alive flag.
fn snapshot_state(s: &ObsSnapshot) -> &'static str {
    if let Some(status) = s.status.as_deref() {
        match status {
            "stopped" => return "stopped",
            "starting" => return "starting",
            _ => {}
        }
    }
    if s.stale {
        "stale"
    } else {
        "alive"
    }
}

/// Fresh comfy-table with the preset the obs subcommands use.
/// Headers passed in `headers` are laid out left-to-right; columns
/// listed in `right_aligned` are right-aligned (typical for numerics).
fn obs_table(headers: &[&str], right_aligned: &[usize]) -> Table {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(headers.iter().map(Cell::new));
    for idx in right_aligned {
        if let Some(col) = table.column_mut(*idx) {
            col.set_cell_alignment(CellAlignment::Right);
        }
    }
    table
}

/// Unicode dash rendered in columns that aren't accounted yet.
const UNKNOWN: &str = "-";

// ────────────────────────── Commands ──────────────────────────

fn cmd_status(args: ObsQueryArgs) -> Result<()> {
    let snapshots = scan_snapshots(&args)?;

    // Partition snapshots by the same rule the peers table uses.
    let alive = snapshots
        .iter()
        .filter(|s| snapshot_state(s) == "alive")
        .count();
    let stale = snapshots
        .iter()
        .filter(|s| snapshot_state(s) == "stale")
        .count();
    let stopped = snapshots
        .iter()
        .filter(|s| snapshot_state(s) == "stopped")
        .count();

    if args.json {
        let mut out = BTreeMap::new();
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
        out.insert("stopped", serde_json::json!(stopped));
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
    println!(
        "Peers: {} alive, {} stale, {} stopped",
        alive, stale, stopped
    );

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

    if snapshots.is_empty() {
        println!("(no obs snapshots found)");
        return Ok(());
    }

    let mut table = obs_table(
        &[
            "PEER", "HOST", "PID", "AGE", "STATE", "ASSIGN", "BATCH", "BW", "BYTES", "PENDING",
            "ERRORS",
        ],
        // Numeric columns: ASSIGN..ERRORS
        &[5, 6, 7, 8, 9, 10],
    );

    for s in &snapshots {
        table.add_row(vec![
            Cell::new(&s.peer_id),
            Cell::new(&s.host),
            Cell::new(s.pid),
            Cell::new(human_age(s.age_ms)),
            Cell::new(snapshot_state(s)),
            Cell::new(get_u64(&s.summary, "assign_total")),
            Cell::new(get_u64(&s.summary, "batch_total")),
            Cell::new(human_bps(s.ewma_bandwidth_bps)),
            Cell::new(human_bytes(get_u64(&s.summary, "completed_bytes_total"))),
            Cell::new(get_i64(&s.summary, "pending_ops")),
            Cell::new(get_u64(&s.summary, "error_total")),
        ]);
    }

    println!("{table}");
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

    if snapshots.is_empty() {
        println!("(no obs snapshots found)");
        return Ok(());
    }

    // Columns: PEER NIC AGE ASSIGN BATCH BW BYTES PENDING ERRORS POST_BYTES POST_FAIL CQ_ERR
    //   BW         = ewma_bandwidth_bps (real bandwidth, computed by PeerAgent reporter)
    //   BYTES      = completed_bytes_total (semantic)
    //   POST_BYTES = post_bytes_total     (transport-level, cumulative)
    let mut table = obs_table(
        &[
            "PEER",
            "NIC",
            "AGE",
            "ASSIGN",
            "BATCH",
            "BW",
            "BYTES",
            "PENDING",
            "ERRORS",
            "POST_BYTES",
            "POST_FAIL",
            "CQ_ERR",
        ],
        // Numeric columns: ASSIGN..CQ_ERR
        &[3, 4, 5, 6, 7, 8, 9, 10, 11],
    );

    let mut any_row = false;
    for s in &snapshots {
        for nic in &s.nics {
            any_row = true;
            let nic_name = nic.get("nic").and_then(Value::as_str).unwrap_or("?");
            let nic_bw_bps = nic
                .get("ewma_bandwidth_bps")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);

            table.add_row(vec![
                Cell::new(&s.peer_id),
                Cell::new(nic_name),
                Cell::new(human_age(s.age_ms)),
                Cell::new(get_u64(nic, "assign_total")),
                Cell::new(get_u64(nic, "batch_total")),
                Cell::new(human_bps(nic_bw_bps)),
                Cell::new(human_bytes(get_u64(nic, "completed_bytes_total"))),
                Cell::new(get_i64(nic, "pending_ops")),
                Cell::new(get_u64(nic, "error_total")),
                Cell::new(human_bytes(get_u64(nic, "post_bytes_total"))),
                Cell::new(get_u64(nic, "post_failures_total")),
                Cell::new(get_u64(nic, "cq_errors_total")),
            ]);
        }
    }

    if !any_row {
        println!("(no obs snapshots found)");
        return Ok(());
    }

    println!("{table}");
    Ok(())
}

/// Per-connection catalog. Fields SRC_PEER / SRC_NIC / DST_PEER / DST_NIC
/// / STATE are harvested from each peer's reported `connections` list.
///
/// v0 does NOT index traffic counters by (src_peer, src_nic, dst_peer,
/// dst_nic) — those columns render as `-`. Peer-pair accounting is a
/// deliberate follow-up.
fn cmd_links(args: ObsQueryArgs) -> Result<()> {
    let snapshots = scan_snapshots(&args)?;

    if args.json {
        let mut arr = Vec::new();
        for s in &snapshots {
            for conn in &s.connections {
                let mut obj = conn.clone();
                if let Some(map) = obj.as_object_mut() {
                    map.insert("src_peer".to_string(), Value::String(s.peer_id.clone()));
                    map.insert("age_ms".to_string(), serde_json::json!(s.age_ms));
                }
                arr.push(obj);
            }
        }
        println!("{}", serde_json::to_string_pretty(&arr)?);
        return Ok(());
    }

    if snapshots.is_empty() {
        println!("(no obs snapshots found)");
        return Ok(());
    }

    let mut table = obs_table(
        &[
            "SRC_PEER", "SRC_NIC", "DST_PEER", "DST_NIC", "STATE", "BW", "BYTES", "PENDING",
            "ERRORS",
        ],
        &[5, 6, 7, 8],
    );

    let mut any_row = false;
    for s in &snapshots {
        for conn in &s.connections {
            any_row = true;
            let src_nic = conn.get("local_nic").and_then(Value::as_str).unwrap_or("?");
            let dst_peer = conn.get("peer").and_then(Value::as_str).unwrap_or("?");
            let dst_nic = conn
                .get("remote_nic")
                .and_then(Value::as_str)
                .unwrap_or("?");

            // Prefer the explicit DirectedConnection.state field; fall
            // back to the connected bool when state is absent.
            let state_owned = conn
                .get("state")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| match conn.get("connected").and_then(Value::as_bool) {
                    Some(true) => "connected".to_string(),
                    Some(false) => "disconnected".to_string(),
                    None => "unknown".to_string(),
                });

            table.add_row(vec![
                Cell::new(&s.peer_id),
                Cell::new(src_nic),
                Cell::new(dst_peer),
                Cell::new(dst_nic),
                Cell::new(state_owned),
                Cell::new(UNKNOWN),
                Cell::new(UNKNOWN),
                Cell::new(UNKNOWN),
                Cell::new(UNKNOWN),
            ]);
        }
    }

    if !any_row {
        println!("(no connections reported)");
        return Ok(());
    }

    println!("{table}");
    println!();
    println!(
        "Note: per-link traffic counters (BW, BYTES, PENDING, ERRORS) are \
         not yet accounted in v0 and render as '{UNKNOWN}'. \
         See `nanoctrl obs nics` for per-NIC traffic."
    );
    Ok(())
}

// ────────────────────────── Tests ──────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot_json(reported_at_ms: u64) -> String {
        serde_json::json!({
            "schema_version": 1,
            "session_id": "alice:123:1700000000000",
            "peer_id": "alice",
            "host": "10.0.0.1",
            "pid": 123,
            "reported_at_ms": reported_at_ms,
            "summary": {
                "assign_total": 42,
                "batch_total": 7,
                "completed_bytes_total": 1048576,
                "pending_ops": 0,
                "error_total": 0,
                "pending_by_op": {"read": 0, "write": 0, "write_with_imm": 0}
            },
            "nics": [{
                "nic": "mlx5_0",
                "assign_total": 42,
                "batch_total": 7,
                "completed_bytes_total": 1048576,
                "post_bytes_total": 2097152,
                "pending_ops": 0,
                "error_total": 0,
                "post_failures_total": 0,
                "cq_errors_total": 0,
                "ewma_bandwidth_bps": 1.25e9
            }],
            "connections": [{
                "conn_id": "alice:mlx5_0->bob:mlx5_1",
                "peer": "bob",
                "local_nic": "mlx5_0",
                "remote_nic": "mlx5_1",
                "state": "connected",
                "connected": true
            }],
            "ewma_bandwidth_bps": 1.25e9
        })
        .to_string()
    }

    #[test]
    fn parse_snapshot_extracts_all_fields() {
        let now_ms = 1_700_000_010_000u64;
        let reported_at_ms = 1_700_000_005_000u64;
        let json = sample_snapshot_json(reported_at_ms);

        let snap = parse_snapshot("scope:obs:peer:alice", &json, now_ms, 45_000)
            .expect("parse should succeed");

        assert_eq!(snap.peer_id, "alice");
        assert_eq!(snap.host, "10.0.0.1");
        assert_eq!(snap.pid, 123);
        assert_eq!(snap.reported_at_ms, reported_at_ms);
        assert_eq!(snap.age_ms, 5_000);
        assert!(!snap.stale);
        assert_eq!(snap.nics.len(), 1);
        assert_eq!(snap.connections.len(), 1);
        assert!((snap.ewma_bandwidth_bps - 1.25e9).abs() < 1e-6);

        // Per-NIC ewma_bandwidth_bps should be preserved in the nic JSON.
        let nic_ewma = snap.nics[0]
            .get("ewma_bandwidth_bps")
            .and_then(Value::as_f64)
            .unwrap();
        assert!((nic_ewma - 1.25e9).abs() < 1e-6);
    }

    #[test]
    fn parse_snapshot_marks_stale_beyond_threshold() {
        let now_ms = 1_700_000_100_000u64;
        let reported_at_ms = 1_700_000_000_000u64; // 100s ago
        let json = sample_snapshot_json(reported_at_ms);

        let snap = parse_snapshot("scope:obs:peer:alice", &json, now_ms, 45_000).unwrap();
        assert!(snap.stale);
        assert!(snap.age_ms >= 100_000);
    }

    #[test]
    fn parse_snapshot_returns_none_on_invalid_json() {
        let snap = parse_snapshot("bad", "not json", 0, 45_000);
        assert!(snap.is_none());
    }

    #[test]
    fn parse_snapshot_defaults_when_fields_missing() {
        // Minimal payload — just enough to be valid JSON object.
        let snap = parse_snapshot("scope:obs:peer:mystery", "{}", 1_000_000, 45_000).unwrap();
        assert_eq!(snap.peer_id, "");
        assert_eq!(snap.reported_at_ms, 0);
        assert!(snap.stale); // age_ms = now_ms > stale_ms
        assert!(snap.nics.is_empty());
        assert!(snap.connections.is_empty());
        assert_eq!(snap.ewma_bandwidth_bps, 0.0);
    }

    #[test]
    fn human_bps_buckets() {
        assert_eq!(human_bps(0.0), "0bps");
        assert_eq!(human_bps(1_500.0), "1.5Kbps");
        assert_eq!(human_bps(1.25e9), "1.2Gbps");
    }

    #[test]
    fn snapshot_state_uses_status_marker_over_age() {
        // Fresh snapshot but explicit status="stopped" takes precedence.
        let now_ms = 1_700_000_010_000u64;
        let reported_at_ms = 1_700_000_009_000u64; // 1s ago, not stale
        let mut raw = serde_json::from_str::<Value>(&sample_snapshot_json(reported_at_ms)).unwrap();
        raw["status"] = Value::String("stopped".to_string());
        let json = raw.to_string();
        let snap = parse_snapshot("k", &json, now_ms, 45_000).unwrap();
        assert!(!snap.stale);
        assert_eq!(snapshot_state(&snap), "stopped");
    }

    #[test]
    fn snapshot_state_falls_back_to_stale_when_no_status() {
        let now_ms = 1_700_000_100_000u64;
        let reported_at_ms = 1_700_000_000_000u64;
        let json = sample_snapshot_json(reported_at_ms);
        let snap = parse_snapshot("k", &json, now_ms, 45_000).unwrap();
        assert_eq!(snapshot_state(&snap), "stale");
    }

    #[test]
    fn snapshot_state_alive_when_fresh_and_no_status() {
        let now_ms = 1_700_000_010_000u64;
        let reported_at_ms = 1_700_000_009_000u64;
        let json = sample_snapshot_json(reported_at_ms);
        let snap = parse_snapshot("k", &json, now_ms, 45_000).unwrap();
        assert_eq!(snapshot_state(&snap), "alive");
    }

    #[test]
    fn parse_snapshot_extracts_connection_fields_for_links() {
        // The links subcommand walks snapshot.connections — verify the
        // parser preserves the per-entry fields we surface in the table.
        let now_ms = 1_700_000_010_000u64;
        let reported_at_ms = 1_700_000_009_000u64;
        let json = sample_snapshot_json(reported_at_ms);
        let snap = parse_snapshot("k", &json, now_ms, 45_000).unwrap();
        assert_eq!(snap.connections.len(), 1);
        let c = &snap.connections[0];
        assert_eq!(c.get("peer").and_then(Value::as_str), Some("bob"));
        assert_eq!(c.get("local_nic").and_then(Value::as_str), Some("mlx5_0"));
        assert_eq!(c.get("remote_nic").and_then(Value::as_str), Some("mlx5_1"));
        assert_eq!(c.get("state").and_then(Value::as_str), Some("connected"));
    }
}
