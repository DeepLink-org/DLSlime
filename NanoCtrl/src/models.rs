use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub struct PeerAgent {
    pub name: String,
    pub device: String,
    pub ib_port: u32,
    pub link_type: String,
    pub address: String, // e.g., "ip:port"
    #[serde(default)]
    pub resource: Option<Value>,
    #[serde(default)]
    pub memory_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Deserialize)]
pub struct StartPeerAgentBody {
    #[serde(default)]
    pub alias: Option<String>, // Optional: if None, NanoCtrl generates unique name
    #[serde(default)]
    pub device: Option<String>,
    #[serde(default)]
    pub ib_port: Option<u32>,
    #[serde(default)]
    pub link_type: Option<String>,
    pub address: String, // IP address
    #[serde(default = "default_name_prefix")]
    pub name_prefix: String, // Prefix for auto-generated names
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
    #[serde(default)]
    pub resource: Option<Value>,
}

fn default_name_prefix() -> String {
    "agent".to_string()
}

/// Desired topology spec for declarative connection management.
/// Stored in Redis at spec:topology:{agent_id}
#[derive(Debug, Deserialize, Serialize)]
pub struct DesiredTopologySpec {
    pub target_peers: Vec<String>,
    #[serde(default)]
    pub min_bw: Option<String>, // e.g. "100Gbps", reserved for future use
    /// When true, also update each target_peer's spec to include this agent_id.
    #[serde(default)]
    pub symmetric: bool,
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MrInfo {
    pub addr: u64,
    pub length: usize,
    pub rkey: u32,
    pub lkey: u32,
}

#[derive(Debug, Deserialize)]
pub struct RegisterMrBody {
    pub agent_name: String,
    pub mr_name: String,
    pub addr: u64,
    pub length: usize,
    pub rkey: u32,
    #[serde(default)]
    pub lkey: u32, // Optional, local key (not needed for remote access)
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Deserialize)]
pub struct GetMrInfoBody {
    #[allow(dead_code)] // API field, reserved for future use
    pub src: String, // Who is asking
    pub dst: String, // Whose MR to get
    pub mr_name: String,
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Serialize)]
pub struct RegisterMrResponse {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct StartPeerAgentResponse {
    pub status: String,
    pub name: String,          // Assigned agent name (generated or provided)
    pub redis_address: String, // Redis address in format "host:port"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetMrInfoResponse {
    pub mr_info: Option<MrInfo>,
}

#[derive(Debug, Deserialize)]
pub struct CleanupBody {
    pub agent_name: String,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CleanupResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct GetRedisAddressBody {
    // Empty body, just need to query
}

#[derive(Debug, Serialize)]
pub struct GetRedisAddressResponse {
    pub status: String,
    pub redis_address: String, // Redis address in format "host:port"
}

#[derive(Debug, Deserialize)]
pub struct RegisterEngineBody {
    pub engine_id: String,
    pub role: String, // "prefill", "decode", "hybrid"
    pub world_size: u32,
    pub num_blocks: u32,
    pub host: String,
    pub port: u32,
    pub peer_addrs: Vec<String>, // Peer agent addresses
    #[serde(default)]
    pub p2p_host: Option<String>, // P2P free instruction host
    #[serde(default)]
    pub p2p_port: Option<u32>, // P2P free instruction port
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
    #[serde(default)]
    pub max_num_seqs: Option<u32>, // Max batch size (GDN slot count = max_num_seqs + 1)
    #[serde(default)]
    pub model_path: Option<String>, // Path to model/tokenizer directory
}

#[derive(Debug, Serialize)]
pub struct RegisterEngineResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct GetEngineInfoBody {
    pub engine_id: String,
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Serialize)]
pub struct GetEngineInfoResponse {
    pub status: String,
    pub engine_info: Option<serde_json::Value>, // Engine info as JSON
}

#[derive(Debug, Deserialize)]
pub struct UnregisterEngineBody {
    pub engine_id: String,
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Serialize)]
pub struct UnregisterEngineResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct ListEnginesBody {
    #[serde(default)]
    pub scope: Option<String>, // Scope for partitioning (from NANOCTRL_SCOPE env var on client)
}

#[derive(Debug, Serialize)]
pub struct ListEnginesResponse {
    pub status: String,
    pub engines: Vec<serde_json::Value>, // List of engine info as JSON
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatBody {
    pub entity_type: String,
    pub entity_id: String,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HeartbeatResponse {
    pub status: String,
    pub message: String,
}
