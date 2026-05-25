//! RDMA-related handlers: topology, memory region registration/query.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::error::AppError;
use crate::models::*;
use crate::redis_repo::RedisRepo;

pub async fn set_desired_topology(
    State(repo): State<RedisRepo>,
    Path(agent_id): Path<String>,
    Json(spec): Json<DesiredTopologySpec>,
) -> Result<impl IntoResponse, AppError> {
    repo.save_topology(spec.scope.as_deref(), &agent_id, &spec)
        .await?;

    Ok((StatusCode::OK, Json(json!({"status": "ok"}))))
}

pub async fn register_mr(
    State(repo): State<RedisRepo>,
    Json(body): Json<RegisterMrBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!(
        "Registering MR: agent={}, mr_name={}, addr={}, length={}, rkey={}, lkey={}",
        body.agent_name,
        body.mr_name,
        body.addr,
        body.length,
        body.rkey,
        body.lkey
    );

    repo.register_mr(body.scope.as_deref(), &body).await?;

    Ok(Json(RegisterMrResponse {
        status: "ok".to_string(),
    }))
}

pub async fn get_mr_info(
    State(repo): State<RedisRepo>,
    Json(body): Json<GetMrInfoBody>,
) -> Result<impl IntoResponse, AppError> {
    let mr_info = repo
        .get_mr_info(body.scope.as_deref(), &body.dst, &body.mr_name)
        .await?;

    Ok(Json(GetMrInfoResponse { mr_info }))
}
