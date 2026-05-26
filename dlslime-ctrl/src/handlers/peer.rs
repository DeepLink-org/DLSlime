//! Peer agent handlers: start, query, cleanup.

use axum::{extract::State, response::IntoResponse, Json};

use crate::error::AppError;
use crate::models::*;
use crate::net;
use crate::redis_repo::RedisRepo;

pub async fn start_peer_agent(
    State(repo): State<RedisRepo>,
    Json(body): Json<StartPeerAgentBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!(
        "Registering agent: device={:?}, ib_port={:?}, link_type={:?}, address={}",
        body.device,
        body.ib_port,
        body.link_type,
        body.address
    );

    let resource = body.resource.clone();
    let agent_name = repo
        .register_agent(
            body.scope.as_deref(),
            body.alias,
            &body.name_prefix,
            body.device.as_deref(),
            body.ib_port,
            body.link_type.as_deref(),
            &body.address,
            resource.as_ref(),
        )
        .await?;

    let redis_address = net::resolve_redis_for_client(repo.redis_url(), &body.address);

    tracing::info!(
        "Sending response for agent {}: status=ok, redis_address={} (client address: {})",
        agent_name,
        redis_address,
        body.address
    );

    Ok(Json(StartPeerAgentResponse {
        status: "ok".to_string(),
        name: agent_name,
        redis_address,
        resource,
    }))
}

pub async fn query(
    State(repo): State<RedisRepo>,
    Json(body): Json<QueryBody>,
) -> Result<Json<Vec<PeerAgent>>, AppError> {
    let agents = repo.list_agents(body.scope.as_deref()).await?;
    Ok(Json(agents))
}

pub async fn cleanup(
    State(repo): State<RedisRepo>,
    Json(body): Json<CleanupBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Cleaning up agent: {}", body.agent_name);

    repo.cleanup_agent(body.scope.as_deref(), &body.agent_name)
        .await?;

    Ok(Json(CleanupResponse {
        status: "ok".to_string(),
        message: format!("Cleaned up agent: {}", body.agent_name),
    }))
}
