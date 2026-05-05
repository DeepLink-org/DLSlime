//! Generic entity management handlers: register, unregister, info, list.

use axum::{extract::State, response::IntoResponse, Json};

use crate::error::AppError;
use crate::models::*;
use crate::redis_repo::RedisRepo;

pub async fn register(
    State(repo): State<RedisRepo>,
    Json(body): Json<RegisterEntityBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!(
        "Registering entity: type={}, id={}, kind={}",
        body.entity_type,
        body.entity_id,
        body.kind
    );

    let _rev = repo.register_entity(body.scope.as_deref(), &body).await?;

    Ok(Json(RegisterEntityResponse {
        status: "ok".to_string(),
        message: format!(
            "Entity {}:{} registered successfully",
            body.entity_type, body.entity_id
        ),
    }))
}

pub async fn unregister(
    State(repo): State<RedisRepo>,
    Json(body): Json<UnregisterEntityBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!(
        "Unregistering entity: type={}, id={}",
        body.entity_type,
        body.entity_id
    );

    repo.unregister_entity(body.scope.as_deref(), &body.entity_type, &body.entity_id)
        .await?;

    Ok(Json(UnregisterEntityResponse {
        status: "ok".to_string(),
        message: format!(
            "Entity {}:{} unregistered successfully",
            body.entity_type, body.entity_id
        ),
    }))
}

pub async fn get_entity_info(
    State(repo): State<RedisRepo>,
    Json(body): Json<GetEntityInfoBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!(
        "Querying entity info for: type={}, id={}",
        body.entity_type,
        body.entity_id
    );

    let info = repo
        .get_entity_info(body.scope.as_deref(), &body.entity_type, &body.entity_id)
        .await?;

    match info {
        Some(entity_info) => Ok(Json(GetEntityInfoResponse {
            status: "ok".to_string(),
            entity_info: Some(entity_info),
        })),
        None => Ok(Json(GetEntityInfoResponse {
            status: "not_found".to_string(),
            entity_info: None,
        })),
    }
}

pub async fn list_entities(
    State(repo): State<RedisRepo>,
    Json(body): Json<ListEntitiesBody>,
) -> Result<impl IntoResponse, AppError> {
    tracing::debug!(
        "Listing entities: type={:?}, kind={:?}",
        body.entity_type,
        body.kind
    );

    let entities = repo
        .list_entities(
            body.scope.as_deref(),
            body.entity_type.as_deref(),
            body.kind.as_deref(),
        )
        .await?;

    Ok(Json(ListEntitiesResponse {
        status: "ok".to_string(),
        entities,
    }))
}
