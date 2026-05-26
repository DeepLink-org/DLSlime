//! Legacy state module — now superseded by `redis_repo`.
//!
//! This module is intentionally left empty. All Redis state management
//! has been moved to [`crate::redis_repo::RedisRepo`], and Lua scripts
//! are loaded from external files in `lua/`.
