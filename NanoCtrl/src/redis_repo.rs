//! Redis repository layer.
//!
//! Encapsulates all Redis operations behind a clean API so that handlers
//! never touch raw Redis commands directly.

use deadpool_redis::{Config as PoolConfig, Connection, Pool, Runtime};
use redis::AsyncCommands;
use serde_json::Value;

use crate::error::AppError;
use crate::models::*;

/// Entity TTL in seconds (for heartbeat mechanism).
///
/// All entities (engines, agents) must send heartbeat every 15 seconds.
/// TTL is set to 60 seconds to allow 4 missed heartbeats before expiration.
pub const ENTITY_TTL_SECS: usize = 60;

/// Lua scripts embedded once at compile time.
pub struct LuaScripts {
    pub register_engine: String,
    pub unregister_engine: String,
    pub heartbeat: String,
}

impl LuaScripts {
    /// Load all Lua scripts from embedded source.
    pub fn load() -> Result<Self, AppError> {
        Ok(Self {
            register_engine: include_str!("../lua/register_engine.lua").to_string(),
            unregister_engine: include_str!("../lua/unregister_engine.lua").to_string(),
            heartbeat: include_str!("../lua/heartbeat_engine.lua").to_string(),
        })
    }
}

/// Redis repository — the single entry-point for all Redis interactions.
#[derive(Clone)]
pub struct RedisRepo {
    pool: Pool,
    redis_url: String,
    scripts: std::sync::Arc<LuaScripts>,
}

impl RedisRepo {
    /// Create a new repository backed by a `deadpool-redis` connection pool.
    pub fn new(redis_url: &str, scripts: LuaScripts) -> Result<Self, AppError> {
        let cfg = PoolConfig::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| AppError::Internal(format!("Failed to create Redis pool: {e}")))?;
        Ok(Self {
            pool,
            redis_url: redis_url.to_string(),
            scripts: std::sync::Arc::new(scripts),
        })
    }

    /// Public accessor for the raw Redis URL.
    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    // ───────────────────────── helpers ─────────────────────────

    /// Get a connection from the pool.
    pub async fn conn(&self) -> Result<Connection, AppError> {
        Ok(self.pool.get().await?)
    }

    /// Build a scoped Redis key.
    ///
    /// ```text
    /// scoped_key(Some("sess"), &["engine", "p0"]) => "sess:engine:p0"
    /// scoped_key(None,         &["engine", "p0"]) => "engine:p0"
    /// ```
    pub fn scoped_key(&self, scope: Option<&str>, parts: &[&str]) -> String {
        let suffix = parts.join(":");
        match scope {
            Some(s) if !s.is_empty() => format!("{s}:{suffix}"),
            _ => suffix,
        }
    }

    // ─────────────────────── agent ops ─────────────────────────

    /// Register a peer agent. Returns the assigned agent name.
    #[allow(clippy::too_many_arguments)]
    pub async fn register_agent(
        &self,
        scope: Option<&str>,
        alias: Option<String>,
        name_prefix: &str,
        device: Option<&str>,
        ib_port: Option<u32>,
        link_type: Option<&str>,
        address: &str,
        resource: Option<&serde_json::Value>,
    ) -> Result<String, AppError> {
        let mut conn = self.conn().await?;

        let agent_name = if let Some(alias) = alias {
            alias
        } else {
            let counter_key = self.scoped_key(scope, &["agent_name_counter"]);
            let counter: i64 = redis::cmd("INCR")
                .arg(&counter_key)
                .query_async(&mut *conn)
                .await?;
            format!("{name_prefix}-{counter:x}")
        };

        let key = self.scoped_key(scope, &["agent", &agent_name]);
        let updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default()
            .to_string();
        let mut cmd = redis::cmd("HSET");
        cmd.arg(&key)
            .arg("addr")
            .arg(address)
            .arg("updated_at")
            .arg(updated_at)
            .arg("memory_keys")
            .arg("[]");
        if let Some(device) = device {
            cmd.arg("device").arg(device);
        }
        if let Some(ib_port) = ib_port {
            cmd.arg("ib_port").arg(ib_port.to_string());
        }
        if let Some(link_type) = link_type {
            cmd.arg("link_type").arg(link_type);
        }
        if let Some(resource) = resource {
            let resource_json = serde_json::to_string(resource)?;
            cmd.arg("resource").arg(&resource_json);
            cmd.arg("topology").arg(resource_json);
        }
        cmd.query_async::<()>(&mut *conn).await?;

        // Set TTL so stale agents expire if heartbeat stops
        redis::cmd("EXPIRE")
            .arg(&key)
            .arg(ENTITY_TTL_SECS)
            .query_async::<()>(&mut *conn)
            .await?;

        tracing::info!("Registered peer agent: {agent_name} (TTL: {ENTITY_TTL_SECS}s)");
        Ok(agent_name)
    }

    /// List all peer agents.
    pub async fn list_agents(&self, scope: Option<&str>) -> Result<Vec<PeerAgent>, AppError> {
        let mut conn = self.conn().await?;
        let pattern = self.scoped_key(scope, &["agent:*"]);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut *conn)
            .await?;

        let agent_prefix = self.scoped_key(scope, &["agent:"]);
        let mut agents = Vec::new();

        for key in keys {
            let data: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
                .arg(&key)
                .query_async(&mut *conn)
                .await?;

            if let Some(ip) = data.get("addr") {
                let name = key.strip_prefix(&agent_prefix).unwrap_or(&key).to_string();
                let resource = data
                    .get("resource")
                    .and_then(|s| serde_json::from_str::<Value>(s).ok());
                let memory_keys = data
                    .get("memory_keys")
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .unwrap_or_default();
                agents.push(PeerAgent {
                    name,
                    device: data.get("device").cloned().unwrap_or_default(),
                    ib_port: data
                        .get("ib_port")
                        .and_then(|x| x.parse().ok())
                        .unwrap_or(1),
                    link_type: data.get("link_type").cloned().unwrap_or_default(),
                    address: ip.clone(),
                    resource,
                    memory_keys,
                });
            }
        }
        Ok(agents)
    }

    /// Full cleanup for an agent: deletes registration, topology, exchange, inbox, MR keys,
    /// and notifies peers.
    pub async fn cleanup_agent(
        &self,
        scope: Option<&str>,
        agent_name: &str,
    ) -> Result<(), AppError> {
        let mut conn = self.conn().await?;

        // 1. Collect peers to notify BEFORE deleting anything
        let mut peers_to_notify = std::collections::HashSet::new();
        let spec_key = self.scoped_key(scope, &["spec:topology", agent_name]);
        if let Ok(Some(spec_str)) = redis::cmd("GET")
            .arg(&spec_key)
            .query_async::<Option<String>>(&mut *conn)
            .await
        {
            if let Ok(spec) = serde_json::from_str::<DesiredTopologySpec>(&spec_str) {
                for p in spec.target_peers {
                    peers_to_notify.insert(p);
                }
            }
        }
        // Scan all specs to find agents that have us as a target
        let spec_pattern = self.scoped_key(scope, &["spec:topology:*"]);
        let spec_prefix = self.scoped_key(scope, &["spec:topology:"]);
        let all_spec_keys: Vec<String> = redis::cmd("KEYS")
            .arg(&spec_pattern)
            .query_async(&mut *conn)
            .await
            .unwrap_or_default();
        for key in &all_spec_keys {
            if key == &spec_key {
                continue;
            }
            if let Ok(Some(s)) = redis::cmd("GET")
                .arg(key)
                .query_async::<Option<String>>(&mut *conn)
                .await
            {
                if let Ok(spec) = serde_json::from_str::<DesiredTopologySpec>(&s) {
                    if spec.target_peers.contains(&agent_name.to_string()) {
                        if let Some(other) = key.strip_prefix(&spec_prefix) {
                            peers_to_notify.insert(other.to_string());
                        }
                    }
                }
            }
        }

        // 2. Delete agent registration
        let agent_key = self.scoped_key(scope, &["agent", agent_name]);
        redis::cmd("DEL")
            .arg(&agent_key)
            .query_async::<()>(&mut *conn)
            .await?;

        // 3. Delete topology spec
        redis::cmd("DEL")
            .arg(&spec_key)
            .query_async::<()>(&mut *conn)
            .await
            .ok();

        // 4. Delete exchange info (both directions)
        for pattern_str in [
            self.scoped_key(scope, &[&format!("exchange:{agent_name}:*")]),
            self.scoped_key(scope, &[&format!("exchange:*:{agent_name}")]),
        ] {
            let keys: Vec<String> = redis::cmd("KEYS")
                .arg(&pattern_str)
                .query_async(&mut *conn)
                .await
                .unwrap_or_default();
            for k in &keys {
                redis::cmd("DEL")
                    .arg(k)
                    .query_async::<()>(&mut *conn)
                    .await
                    .ok();
            }
        }

        // 5. Delete inbox
        let inbox_key = self.scoped_key(scope, &["inbox", agent_name]);
        redis::cmd("DEL")
            .arg(&inbox_key)
            .query_async::<()>(&mut *conn)
            .await
            .ok();

        // 6. Delete all MRs for this agent
        let mr_pattern = self.scoped_key(scope, &[&format!("mr:{agent_name}:*")]);
        let mr_keys: Vec<String> = redis::cmd("KEYS")
            .arg(&mr_pattern)
            .query_async(&mut *conn)
            .await
            .unwrap_or_default();
        if !mr_keys.is_empty() {
            redis::cmd("DEL")
                .arg(&mr_keys)
                .query_async::<()>(&mut *conn)
                .await
                .ok();
        }

        // 7. Notify peers
        for peer in &peers_to_notify {
            let cleanup_event = serde_json::json!({
                "type": "cleanup",
                "peer": agent_name,
            });
            let peer_inbox = self.scoped_key(scope, &["inbox", peer]);
            redis::cmd("LPUSH")
                .arg(&peer_inbox)
                .arg(cleanup_event.to_string())
                .query_async::<()>(&mut *conn)
                .await
                .ok();
            tracing::info!("Notified peer {peer} to clean up connection with {agent_name}");
        }

        tracing::info!("Cleanup completed for agent: {agent_name}");
        Ok(())
    }

    // ────────────────────── engine ops ─────────────────────────

    /// Register an engine atomically using Lua script (HSET + EXPIRE + INCR + PUBLISH).
    /// Returns the new revision number.
    pub async fn register_engine(
        &self,
        scope: Option<&str>,
        body: &RegisterEngineBody,
    ) -> Result<i64, AppError> {
        let mut conn = self.conn().await?;
        let engine_key = self.scoped_key(scope, &["engine", &body.engine_id]);
        let revision_key = self.scoped_key(scope, &["nano_meta:engine_revision"]);
        let channel = self.scoped_key(scope, &["nano_events:engine_update"]);

        let zmq_address = format!("tcp://{}:{}", body.host, body.port);
        let payload = serde_json::json!({
            "id": body.engine_id,
            "role": body.role,
            "host": body.host,
            "port": body.port,
            "zmq_address": zmq_address,
            "world_size": body.world_size,
            "num_blocks": body.num_blocks,
            "peer_addrs": body.peer_addrs,
            "model_path": body.model_path,  // null if not provided (old engine)
        });
        let engine_info = serde_json::json!({
            "id": body.engine_id,
            "role": body.role,
            "world_size": body.world_size,
            "num_blocks": body.num_blocks,
            "host": body.host,
            "port": body.port,
            "peer_addrs": body.peer_addrs,
            "p2p_host": body.p2p_host.as_deref().unwrap_or_default(),
            "p2p_port": body.p2p_port.unwrap_or(0),
            "max_num_seqs": body.max_num_seqs.unwrap_or(0),
            "model_path": body.model_path,  // null if not provided (old engine)
        });

        let rev: i64 = redis::cmd("EVAL")
            .arg(&*self.scripts.register_engine)
            .arg(3) // number of keys
            .arg(&engine_key)
            .arg(&revision_key)
            .arg(&channel)
            .arg(&body.engine_id) // ARGV[1]
            .arg(&body.role) // ARGV[2]
            .arg(&body.host) // ARGV[3]
            .arg(body.port.to_string()) // ARGV[4]
            .arg(body.world_size.to_string()) // ARGV[5]
            .arg(body.num_blocks.to_string()) // ARGV[6]
            .arg(serde_json::to_string(&body.peer_addrs).unwrap_or_default()) // ARGV[7]
            .arg(engine_info.to_string()) // ARGV[8]
            .arg(payload.to_string()) // ARGV[9]
            .arg(ENTITY_TTL_SECS.to_string()) // ARGV[10]
            .arg(body.model_path.as_deref().unwrap_or("")) // ARGV[11]
            .query_async(&mut *conn)
            .await?;

        tracing::info!(
            "Registered engine: {} (revision: {}, TTL: {}s)",
            body.engine_id,
            rev,
            ENTITY_TTL_SECS
        );
        Ok(rev)
    }

    /// Unregister an engine atomically. Returns revision, or `NotFound`.
    pub async fn unregister_engine(
        &self,
        scope: Option<&str>,
        engine_id: &str,
    ) -> Result<i64, AppError> {
        let mut conn = self.conn().await?;
        let engine_key = self.scoped_key(scope, &["engine", engine_id]);
        let revision_key = self.scoped_key(scope, &["nano_meta:engine_revision"]);
        let channel = self.scoped_key(scope, &["nano_events:engine_update"]);

        let rev: i64 = redis::cmd("EVAL")
            .arg(&*self.scripts.unregister_engine)
            .arg(3)
            .arg(&engine_key)
            .arg(&revision_key)
            .arg(&channel)
            .arg(engine_id)
            .query_async(&mut *conn)
            .await?;

        if rev == 0 {
            tracing::warn!("Unregister engine: {engine_id} not found (already removed or never registered), treating as success");
            return Ok(0);
        }
        tracing::info!("Unregistered engine: {engine_id} (revision: {rev})");
        Ok(rev)
    }

    /// Generic heartbeat: refresh TTL for any entity type.
    /// `entity_type` is the Redis key prefix ("engine", "agent", etc.).
    /// Returns `true` if the entity exists, `false` otherwise.
    pub async fn heartbeat(
        &self,
        entity_type: &str,
        scope: Option<&str>,
        entity_id: &str,
    ) -> Result<bool, AppError> {
        let key = self.scoped_key(scope, &[entity_type, entity_id]);
        self.refresh_ttl(&key, ENTITY_TTL_SECS).await
    }

    /// Low-level: refresh TTL on any Redis key via Lua script.
    async fn refresh_ttl(&self, key: &str, ttl_secs: usize) -> Result<bool, AppError> {
        let mut conn = self.conn().await?;
        let result: i64 = redis::cmd("EVAL")
            .arg(&*self.scripts.heartbeat)
            .arg(1)
            .arg(key)
            .arg(ttl_secs.to_string())
            .query_async(&mut *conn)
            .await?;
        Ok(result == 1)
    }

    /// Get engine info by ID.
    pub async fn get_engine_info(
        &self,
        scope: Option<&str>,
        engine_id: &str,
    ) -> Result<Option<Value>, AppError> {
        let mut conn = self.conn().await?;
        let key = self.scoped_key(scope, &["engine", engine_id]);
        let info_str: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg("info")
            .query_async(&mut *conn)
            .await?;

        match info_str {
            Some(s) => Ok(serde_json::from_str(&s).ok()),
            None => Ok(None),
        }
    }

    /// List all registered engines.
    pub async fn list_engines(&self, scope: Option<&str>) -> Result<Vec<Value>, AppError> {
        let mut conn = self.conn().await?;
        let pattern = self.scoped_key(scope, &["engine:*"]);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut *conn)
            .await?;

        let mut engines = Vec::new();
        for key in keys {
            let info_str: Option<String> = redis::cmd("HGET")
                .arg(&key)
                .arg("info")
                .query_async(&mut *conn)
                .await
                .unwrap_or(None);
            if let Some(s) = info_str {
                if let Ok(v) = serde_json::from_str::<Value>(&s) {
                    engines.push(v);
                }
            }
        }
        Ok(engines)
    }

    // ─────────────────────── RDMA ops ──────────────────────────

    /// Save desired topology spec and push connect_peer messages to streams.
    pub async fn save_topology(
        &self,
        scope: Option<&str>,
        agent_id: &str,
        spec: &DesiredTopologySpec,
    ) -> Result<(), AppError> {
        let mut conn = self.conn().await?;
        let key = self.scoped_key(scope, &["spec:topology", agent_id]);
        let spec_json = serde_json::to_string(spec)?;
        conn.set::<_, _, ()>(&key, &spec_json).await?;

        // Push connect_peer messages to agent's stream
        let stream_key = self.scoped_key(scope, &["stream", agent_id]);
        for target_peer in &spec.target_peers {
            self.push_stream_connect(&mut conn, &stream_key, target_peer)
                .await;
        }

        // Symmetric: merge this agent into each target peer's spec
        if spec.symmetric {
            for target_peer in &spec.target_peers {
                let peer_key = self.scoped_key(scope, &["spec:topology", target_peer]);
                let existing: Option<String> = conn.get(&peer_key).await.ok().flatten();
                let mut peer_targets: Vec<String> = existing
                    .and_then(|s| serde_json::from_str::<DesiredTopologySpec>(&s).ok())
                    .map(|p| p.target_peers)
                    .unwrap_or_default();

                if !peer_targets.contains(&agent_id.to_string()) {
                    peer_targets.push(agent_id.to_string());
                }

                let peer_spec = DesiredTopologySpec {
                    target_peers: peer_targets,
                    min_bw: None,
                    symmetric: false,
                    scope: spec.scope.clone(),
                };
                if let Ok(peer_json) = serde_json::to_string(&peer_spec) {
                    let _: Result<(), _> = conn.set(&peer_key, &peer_json).await;
                }

                // Push connect_peer to peer's stream too
                let peer_stream = self.scoped_key(scope, &["stream", target_peer]);
                self.push_stream_connect(&mut conn, &peer_stream, agent_id)
                    .await;
            }
            tracing::info!(
                "Symmetric: merged {agent_id} into {} target peer(s) spec",
                spec.target_peers.len()
            );
        }

        tracing::info!(
            "Saved desired topology for agent {agent_id}: {} peer(s)",
            spec.target_peers.len()
        );
        Ok(())
    }

    /// Register a memory region.
    pub async fn register_mr(
        &self,
        scope: Option<&str>,
        body: &RegisterMrBody,
    ) -> Result<(), AppError> {
        let mut conn = self.conn().await?;
        let key = self.scoped_key(scope, &["mr", &body.agent_name, &body.mr_name]);
        let mr_info = serde_json::json!({
            "addr": body.addr,
            "length": body.length,
            "rkey": body.rkey,
            "lkey": body.lkey,
        });
        conn.set::<_, _, ()>(&key, mr_info.to_string()).await?;
        tracing::info!(
            "Registered MR: {} for agent: {}",
            body.mr_name,
            body.agent_name
        );
        Ok(())
    }

    /// Get memory region info.
    pub async fn get_mr_info(
        &self,
        scope: Option<&str>,
        dst: &str,
        mr_name: &str,
    ) -> Result<Option<MrInfo>, AppError> {
        let mut conn = self.conn().await?;
        let key = self.scoped_key(scope, &["mr", dst, mr_name]);
        let mr_str: Option<String> = conn.get(&key).await?;

        Ok(mr_str.and_then(|s| {
            let v: Value = serde_json::from_str(&s).ok()?;
            Some(MrInfo {
                addr: v["addr"].as_u64()?,
                length: v["length"].as_u64()? as usize,
                rkey: v["rkey"].as_u64()? as u32,
                lkey: v["lkey"].as_u64()? as u32,
            })
        }))
    }

    // ─────────────────── private helpers ───────────────────────

    /// Push a `connect_peer` message to a Redis Stream.
    async fn push_stream_connect(&self, conn: &mut Connection, stream_key: &str, peer: &str) {
        let timestamp = format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        );
        let message = vec![
            ("type", "connect_peer"),
            ("peer", peer),
            ("timestamp", timestamp.as_str()),
        ];
        if let Err(e) = redis::cmd("XADD")
            .arg(stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg("1000")
            .arg("*")
            .arg(&message)
            .query_async::<String>(&mut **conn)
            .await
        {
            tracing::warn!("Failed to push connect_peer to stream {stream_key}: {e}");
        }
    }
}
