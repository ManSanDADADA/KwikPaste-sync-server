//! Request extractors that fail with the protocol's error body.

use crate::{db, error::ApiError, AppState};
use axum::{
    extract::{ConnectInfo, FromRequest, FromRequestParts, Query},
    http::{header::AUTHORIZATION, request::Parts},
    Json,
};
use std::net::{IpAddr, SocketAddr};

#[derive(FromRequest)]
#[from_request(via(Json), rejection(ApiError))]
pub struct ApiJson<T>(pub T);

#[derive(FromRequestParts)]
#[from_request(via(Query), rejection(ApiError))]
pub struct ApiQuery<T>(pub T);

/// A device authenticated by `Authorization: Bearer <device token>`.
pub struct Member {
    pub group: String,
    pub device: String,
}

impl FromRequestParts<AppState> for Member {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &AppState) -> Result<Self, ApiError> {
        let token = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or(ApiError::Unauthorized)?;
        let (group, device) =
            db::touch_device(&app.db, &crate::token_hash(token.trim()), &crate::now())
                .await?
                .ok_or(ApiError::Unauthorized)?;
        Ok(Self { group, device })
    }
}

/// The caller's IP; from `X-Real-IP` / `X-Forwarded-For` only when the proxy is trusted.
pub struct ClientIp(pub IpAddr);

impl FromRequestParts<AppState> for ClientIp {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &AppState) -> Result<Self, ApiError> {
        if app.config.trust_proxy {
            let header = |name: &str| parts.headers.get(name).and_then(|v| v.to_str().ok());
            let forwarded = header("x-real-ip")
                .or_else(|| header("x-forwarded-for").and_then(|v| v.rsplit(',').next()))
                .and_then(|v| v.trim().parse().ok());
            if let Some(ip) = forwarded {
                return Ok(Self(ip));
            }
        }
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map_or(IpAddr::from([0, 0, 0, 0]), |c| c.0.ip());
        Ok(Self(peer))
    }
}
