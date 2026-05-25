//! Utility handlers: health check, Redis address, generic heartbeat.

use axum::{extract::State, response::IntoResponse, Json};

use crate::error::AppError;
use crate::models::*;
use crate::net;
use crate::redis_repo::RedisRepo;

pub async fn root() -> &'static str {
    "DLSlime control plane Server Running"
}

pub async fn heartbeat(
    State(repo): State<RedisRepo>,
    Json(body): Json<HeartbeatBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::debug!("Heartbeat for {}:{}", body.entity_type, body.entity_id);

    let found = repo
        .heartbeat(&body.entity_type, body.scope.as_deref(), &body.entity_id)
        .await?;

    let (status, msg) = if found {
        (
            "ok",
            format!(
                "Heartbeat successful for {} {}",
                body.entity_type, body.entity_id
            ),
        )
    } else {
        (
            "not_found",
            format!(
                "{} {} not found. Please register first.",
                body.entity_type, body.entity_id
            ),
        )
    };

    Ok(Json(HeartbeatResponse {
        status: status.to_string(),
        message: msg,
    }))
}

pub async fn get_redis_address(
    State(repo): State<RedisRepo>,
    Json(_body): Json<GetRedisAddressBody>,
) -> Result<impl IntoResponse, AppError> {
    // Explicit advertise URL wins over any auto-detection. Set this when ctrl
    // and Redis are reachable at different addresses from the client's
    // perspective (e.g. inside Docker compose: ctrl talks to Redis via the
    // service-name DNS `redis://redis:6379`, but PeerAgents outside the
    // compose network must use the host-mapped port like `redis://<host>:16379`).
    let redis_address = match std::env::var("DLSLIME_CTRL_REDIS_ADVERTISE") {
        Ok(advertised) if !advertised.is_empty() => {
            tracing::debug!(
                "Returning advertised Redis URL: {} (ctrl-internal: {})",
                advertised,
                repo.redis_url()
            );
            advertised
        }
        _ => {
            let resolved = net::resolve_public_redis_url(repo.redis_url());
            tracing::debug!(
                "Returning Redis URL: {} (original: {})",
                resolved,
                repo.redis_url()
            );
            resolved
        }
    };

    Ok(Json(GetRedisAddressResponse {
        status: "ok".to_string(),
        redis_address,
    }))
}
