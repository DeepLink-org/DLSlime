//! Utility handlers: health check, Redis address, generic heartbeat.

use axum::{extract::State, response::IntoResponse, Json};

use crate::error::AppError;
use crate::models::*;
use crate::net;
use crate::redis_repo::RedisRepo;

pub async fn root() -> &'static str {
    "NanoCtrl Server Running"
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
    let redis_address = net::resolve_public_redis_url(repo.redis_url());
    tracing::debug!(
        "Returning Redis URL: {} (original: {})",
        redis_address,
        repo.redis_url()
    );
    Ok(Json(GetRedisAddressResponse {
        status: "ok".to_string(),
        redis_address,
    }))
}
