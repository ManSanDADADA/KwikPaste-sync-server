use axum::{
    extract::rejection::{JsonRejection, QueryRejection},
    http::{header::RETRY_AFTER, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Request-level errors, serialized as `{"error", "message"}` (PROTOCOL.md §2).
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("server token is missing or wrong")]
    ServerToken,
    #[error("device token is invalid")]
    Unauthorized,
    #[error("not found")]
    NotFound,
    #[error("pairing code is invalid, expired or used up")]
    CodeInvalid,
    #[error("the inviting device is not online")]
    InviterOffline,
    #[error("the inviting device did not answer in time")]
    PairingTimeout,
    #[error("the group has reached its device limit")]
    GroupFull,
    #[error("request is too large")]
    TooLarge,
    #[error("too many requests")]
    RateLimited { retry_after: u64 },
    #[error("internal server error")]
    Internal(#[from] anyhow::Error),
}

impl ApiError {
    pub fn bad(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    /// The protocol error code, e.g. `codeInvalid`.
    pub fn code(&self) -> &'static str {
        self.parts().1
    }

    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "badRequest"),
            Self::ServerToken => (StatusCode::UNAUTHORIZED, "serverToken"),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::NotFound => (StatusCode::NOT_FOUND, "notFound"),
            Self::CodeInvalid => (StatusCode::NOT_FOUND, "codeInvalid"),
            Self::InviterOffline => (StatusCode::CONFLICT, "inviterOffline"),
            Self::PairingTimeout => (StatusCode::REQUEST_TIMEOUT, "pairingTimeout"),
            Self::GroupFull => (StatusCode::CONFLICT, "groupFull"),
            Self::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "tooLarge"),
            Self::RateLimited { .. } => (StatusCode::TOO_MANY_REQUESTS, "rateLimited"),
            Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        }
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        Self::Internal(err.into())
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        Self::Internal(err.into())
    }
}

impl From<JsonRejection> for ApiError {
    fn from(err: JsonRejection) -> Self {
        Self::BadRequest(err.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(err: QueryRejection) -> Self {
        Self::BadRequest(err.body_text())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Self::Internal(err) = &self {
            tracing::error!("request failed: {err:#}");
        }
        let (status, code) = self.parts();
        let body = Json(json!({ "error": code, "message": self.to_string() }));
        match self {
            Self::RateLimited { retry_after } => {
                (status, [(RETRY_AFTER, retry_after.to_string())], body).into_response()
            }
            _ => (status, body).into_response(),
        }
    }
}
