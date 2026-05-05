//! Engine management handlers: register, unregister, info, list.

use axum::{extract::State, response::IntoResponse, Json};

use crate::error::AppError;
use crate::models::*;
use crate::redis_repo::RedisRepo;

pub async fn register_engine(
    State(repo): State<RedisRepo>,
    Json(body): Json<RegisterEngineBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!(
        "Registering engine: id={}, role={}, world_size={}, num_blocks={}, host={}, port={}, peer_addrs={:?}",
        body.engine_id, body.role, body.world_size, body.num_blocks, body.host, body.port, body.peer_addrs
    );

    let _rev = repo.register_engine(body.scope.as_deref(), &body).await?;

    Ok(Json(RegisterEngineResponse {
        status: "ok".to_string(),
        message: format!("Engine {} registered successfully", body.engine_id),
    }))
}

pub async fn unregister_engine(
    State(repo): State<RedisRepo>,
    Json(body): Json<UnregisterEngineBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Unregistering engine: {}", body.engine_id);

    repo.unregister_engine(body.scope.as_deref(), &body.engine_id)
        .await?;

    Ok(Json(UnregisterEngineResponse {
        status: "ok".to_string(),
        message: format!("Engine {} unregistered successfully", body.engine_id),
    }))
}

pub async fn get_engine_info(
    State(repo): State<RedisRepo>,
    Json(body): Json<GetEngineInfoBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Querying engine info for: {}", body.engine_id);

    let info = repo
        .get_engine_info(body.scope.as_deref(), &body.engine_id)
        .await?;

    match info {
        Some(engine_info) => {
            tracing::info!("Found engine info for: {}", body.engine_id);
            Ok(Json(GetEngineInfoResponse {
                status: "ok".to_string(),
                engine_info: Some(engine_info),
            }))
        }
        None => {
            tracing::warn!("Engine info not found for: {}", body.engine_id);
            Ok(Json(GetEngineInfoResponse {
                status: "not_found".to_string(),
                engine_info: None,
            }))
        }
    }
}

pub async fn list_engines(
    State(repo): State<RedisRepo>,
    Json(body): Json<ListEnginesBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::debug!("Listing all registered engines");

    let engines = repo.list_engines(body.scope.as_deref()).await?;
    tracing::debug!("Found {} registered engines", engines.len());

    Ok(Json(ListEnginesResponse {
        status: "ok".to_string(),
        engines,
    }))
}
