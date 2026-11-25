//! Router plus the small JSON endpoints: info, groups, pairing, devices (PROTOCOL.md §4).

use crate::{
    db::{self, Removed},
    error::ApiError,
    extract::{ApiJson, ClientIp, Member},
    frames,
    hub::devices_frame,
    limits::Bucket,
    now, pairing, random_token, stream, timestamp, token_hash, upload, AppState,
};
use axum::{
    extract::{Path, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use subtle::ConstantTimeEq;

/// How long `POST /v1/join` waits for the inviter's reply.
const JOIN_WAIT: Duration = Duration::from_secs(60);

pub fn router(app: AppState) -> Router {
    Router::new()
        .route("/v1/info", get(info))
        .route("/v1/groups", post(create_group))
        .route("/v1/pairing", post(create_code).delete(cancel_code))
        .route("/v1/join", post(join))
        .route("/v1/pairing/{session}/reply", post(reply))
        .route("/v1/join/{session}/complete", post(complete))
        .route("/v1/devices", get(list_devices))
        .route("/v1/devices/self", put(update_self))
        .route("/v1/devices/{device}", delete(remove_device))
        .route("/v1/entries", post(upload::upload))
        .route("/v1/events", get(stream::events))
        .fallback(|| async { ApiError::NotFound })
        .layer(middleware::from_fn_with_state(
            app.clone(),
            require_server_token,
        ))
        .with_state(app)
}

/// Self-hosted servers require `X-KwikPaste-Server-Token` on every request.
async fn require_server_token(State(app): State<AppState>, req: Request, next: Next) -> Response {
    if let Some(expected) = &app.config.server_token {
        let given = req
            .headers()
            .get("x-kwikpaste-server-token")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if !bool::from(given.ct_eq(expected.as_bytes())) {
            return ApiError::ServerToken.into_response();
        }
    }
    next.run(req).await
}

fn done() -> Json<Value> {
    Json(json!({}))
}

/// Device ids are the LAN identity ids: 20 hex characters.
fn check_device_id(id: &str) -> Result<(), ApiError> {
    if id.len() == 20 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ApiError::bad("deviceId must be 20 hex characters"))
    }
}

async fn info(State(app): State<AppState>) -> Json<Value> {
    Json(json!({
        "protocol": { "min": 1, "max": 1 },
        "mode": app.config.mode,
        "serverVersion": env!("CARGO_PKG_VERSION"),
        "policy": app.config.policy,
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewGroup {
    device_id: String,
    info: String,
}

async fn create_group(
    State(app): State<AppState>,
    ClientIp(ip): ClientIp,
    ApiJson(req): ApiJson<NewGroup>,
) -> Result<Json<Value>, ApiError> {
    app.limits.hit(Bucket::Groups, ip)?;
    check_device_id(&req.device_id)?;
    let group = uuid::Uuid::new_v4().to_string();
    let token = random_token();
    db::create_group(
        &app.db,
        &group,
        &req.device_id,
        &req.info,
        &token_hash(&token),
        &now(),
    )
    .await?;
    tracing::info!("group {group} created");
    Ok(Json(json!({ "groupId": group, "deviceToken": token })))
}

async fn create_code(State(app): State<AppState>, member: Member) -> Json<Value> {
    let (code, expires_at) = app.pairing.create_code(&member.group, &member.device);
    Json(json!({ "code": code, "expiresAt": timestamp(expires_at) }))
}

async fn cancel_code(State(app): State<AppState>, member: Member) -> Json<Value> {
    app.pairing.cancel_code(&member.group);
    done()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JoinRequest {
    code: String,
    device_id: String,
    spake: String,
}

/// Relays the joiner's SPAKE2 message to the inviter and waits for its answer.
async fn join(
    State(app): State<AppState>,
    ClientIp(ip): ClientIp,
    ApiJson(req): ApiJson<JoinRequest>,
) -> Result<Json<Value>, ApiError> {
    app.limits.hit(Bucket::Join, ip)?;
    check_device_id(&req.device_id)?;
    let join = app.pairing.start_join(req.code.trim(), &req.device_id)?;
    let request = json!({
        "type": "pairRequest",
        "session": join.session,
        "deviceId": req.device_id,
        "spake": req.spake,
    });
    if !app
        .hub
        .send(&join.group, &join.inviter, frames::encode(&request, &[]))
    {
        app.pairing.drop_session(&join.session);
        return Err(ApiError::InviterOffline);
    }
    match tokio::time::timeout(JOIN_WAIT, join.reply).await {
        Ok(Ok(reply)) => Ok(Json(json!({
            "session": join.session,
            "spake": reply.spake,
            "sealed": reply.sealed,
        }))),
        _ => {
            app.pairing.drop_session(&join.session);
            Err(ApiError::PairingTimeout)
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairReply {
    spake: String,
    sealed: String,
    join_proof: String,
}

async fn reply(
    State(app): State<AppState>,
    member: Member,
    Path(session): Path<String>,
    ApiJson(req): ApiJson<PairReply>,
) -> Result<Json<Value>, ApiError> {
    let proof = hex32(&req.join_proof, "joinProof")?;
    let reply = pairing::Reply {
        spake: req.spake,
        sealed: req.sealed,
    };
    app.pairing
        .reply(&session, &member.group, &member.device, reply, proof)?;
    Ok(done())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompleteJoin {
    join_secret: String,
    info: String,
}

async fn complete(
    State(app): State<AppState>,
    Path(session): Path<String>,
    ApiJson(req): ApiJson<CompleteJoin>,
) -> Result<Json<Value>, ApiError> {
    let secret = hex32(&req.join_secret, "joinSecret")?;
    let (group, device) = app.pairing.finish(&session, secret.as_bytes())?;
    let token = random_token();
    db::add_device(
        &app.db,
        &group,
        &device,
        &req.info,
        &token_hash(&token),
        app.config.policy.max_devices,
        &now(),
    )
    .await?;
    app.pairing.cancel_code(&group);
    app.hub.broadcast(&group, &devices_frame(), &[]);
    tracing::info!("device joined group {group}");
    Ok(Json(json!({ "groupId": group, "deviceToken": token })))
}

/// Decodes 64 hex characters (32 bytes).
fn hex32(value: &str, field: &str) -> Result<blake3::Hash, ApiError> {
    blake3::Hash::from_hex(value.trim())
        .map_err(|_| ApiError::bad(format!("{field} must be 64 hex characters")))
}

async fn list_devices(
    State(app): State<AppState>,
    member: Member,
) -> Result<Json<Vec<Value>>, ApiError> {
    let devices = db::list_devices(&app.db, &member.group).await?;
    Ok(Json(
        devices
            .into_iter()
            .map(|d| {
                let online = app.hub.is_online(&member.group, &d.device_id);
                json!({
                    "deviceId": d.device_id,
                    "info": d.info,
                    "createdAt": d.created_at,
                    "lastSeenAt": d.last_seen_at,
                    "online": online,
                })
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
struct DeviceInfo {
    info: String,
}

async fn update_self(
    State(app): State<AppState>,
    member: Member,
    ApiJson(req): ApiJson<DeviceInfo>,
) -> Result<Json<Value>, ApiError> {
    db::update_device_info(&app.db, &member.group, &member.device, &req.info).await?;
    app.hub
        .broadcast(&member.group, &devices_frame(), &[&member.device]);
    Ok(done())
}

/// Any member may remove any device; removing the last one deletes the group.
async fn remove_device(
    State(app): State<AppState>,
    member: Member,
    Path(device): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let removed = db::remove_device(&app.db, &member.group, &device)
        .await?
        .ok_or(ApiError::NotFound)?;
    app.hub.revoke(&member.group, &device);
    match removed {
        Removed::Device => {
            app.hub.broadcast(&member.group, &devices_frame(), &[]);
        }
        Removed::Group(blobs) => {
            app.pairing.forget_group(&member.group);
            app.blobs.remove(blobs).await;
            tracing::info!("group {} deleted: its last device left", member.group);
        }
    }
    Ok(done())
}
