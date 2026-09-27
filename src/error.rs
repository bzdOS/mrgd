// START_AI_HEADER
// MODULE: matrix-hs/src/error.rs
// PURPOSE: Error type for the CS-API handlers.  Converts to axum IntoResponse so
//          handlers can use `?` and the HTTP layer returns a well-formed JSON error body
//          compatible with the Matrix error schema (errcode + error).
// DEPENDENCIES: axum, serde_json, thiserror
// PUBLIC_API: HsError
// END_AI_HEADER

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

// HsError:start
//   purpose: Unified error type for all CS-API handlers.
//            Variants map to Matrix-compatible errcode strings and HTTP status codes.
//            Handlers return Result<T, HsError>; axum calls into_response() on Err.
//   input:  variant construction at call site
//   output: HTTP response with JSON body {"errcode":"M_*","error":"..."}
//   sideEffects: none
// HsError:end
#[derive(Debug, Error)]
pub enum HsError {
    #[error("room not found: {0}")]
    RoomNotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    /// Room alias is already taken by another room (cluster-wide barrier rejected).
    /// Maps to Matrix errcode M_ROOM_IN_USE (HTTP 400).
    #[error("room alias already taken: {0}")]
    RoomInUse(String),

    /// Bearer token is missing, invalid, or has an invalid HMAC signature.
    /// Maps to Matrix errcode M_UNKNOWN_TOKEN (HTTP 401).
    #[error("unknown token: {0}")]
    UnknownToken(String),

    /// Upload body exceeded the server's configured media size limit.
    /// Maps to Matrix errcode M_TOO_LARGE (HTTP 413).
    #[error("payload too large: {0}")]
    TooLarge(String),
}

impl IntoResponse for HsError {
    fn into_response(self) -> Response {
        let (status, errcode, msg) = match &self {
            HsError::RoomNotFound(r) => (
                StatusCode::NOT_FOUND,
                "M_NOT_FOUND",
                format!("room not found: {r}"),
            ),
            HsError::BadRequest(m) => (StatusCode::BAD_REQUEST, "M_BAD_JSON", m.clone()),
            HsError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN", m.clone()),
            HsError::NotFound(m) => (StatusCode::NOT_FOUND, "M_NOT_FOUND", m.clone()),
            HsError::Forbidden(m) => (StatusCode::FORBIDDEN, "M_FORBIDDEN", m.clone()),
            HsError::RoomInUse(m) => (StatusCode::BAD_REQUEST, "M_ROOM_IN_USE", m.clone()),
            HsError::UnknownToken(m) => (StatusCode::UNAUTHORIZED, "M_UNKNOWN_TOKEN", m.clone()),
            HsError::TooLarge(m) => (StatusCode::PAYLOAD_TOO_LARGE, "M_TOO_LARGE", m.clone()),
        };
        (status, Json(json!({ "errcode": errcode, "error": msg }))).into_response()
    }
}
