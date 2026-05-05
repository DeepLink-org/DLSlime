//! Unified error type for NanoCtrl handlers.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Application-level error type.
///
/// All handler functions return `Result<T, AppError>` so that Redis failures,
/// serialization issues, and domain-level "not found" / "conflict" cases are
/// handled in one place.
#[derive(Debug)]
pub enum AppError {
    /// Redis connectivity or command error.
    Redis(redis::RedisError),
    /// Requested resource does not exist.
    #[allow(dead_code)]
    NotFound(String),
    /// Conflicting state (e.g. duplicate agent name).
    #[allow(dead_code)]
    Conflict(String),
    /// JSON (de)serialization failure.
    Serialization(serde_json::Error),
    /// Catch-all for other errors.
    Internal(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redis(e) => write!(f, "Redis error: {e}"),
            Self::NotFound(msg) => write!(f, "Not found: {msg}"),
            Self::Conflict(msg) => write!(f, "Conflict: {msg}"),
            Self::Serialization(e) => write!(f, "Serialization error: {e}"),
            Self::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::Redis(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Redis: {e}")),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            Self::Serialization(e) => (StatusCode::BAD_REQUEST, format!("Serialization: {e}")),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        tracing::error!("{self}");
        (
            status,
            Json(json!({ "status": "error", "message": message })),
        )
            .into_response()
    }
}

impl From<redis::RedisError> for AppError {
    fn from(e: redis::RedisError) -> Self {
        Self::Redis(e)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e)
    }
}

impl From<deadpool_redis::PoolError> for AppError {
    fn from(e: deadpool_redis::PoolError) -> Self {
        Self::Internal(format!("Redis pool: {e}"))
    }
}
