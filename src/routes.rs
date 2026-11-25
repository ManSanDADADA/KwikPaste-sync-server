//! Router plus the small JSON endpoints: info, groups, pairing, devices (PROTOCOL.md §4).

use crate::{
    db::{self, Removed},
    error::ApiError,
    extract::{ApiJson, ApiQuery, ClientIp, Member},
    frames,
    hub::devices_frame,
    limits::Bucket,
    now, pairing, random_token, stats, stream, timestamp, token_hash, upload, App, AppState,
};
use axum::{
    extract::{Path, Request, State},
    http::HeaderMap,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{net::IpAddr, time::Duration};
use subtle::ConstantTimeEq;

/// How long `POST /v1/join` waits for the inviter's reply.
const JOIN_WAIT: Duration = Duration::from_secs(60);
const DEVICE_INFO_MAX: usize = 2 * 1024;

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
        .route("/v1/stats", get(stats_report))
        .fallback(|| async { ApiError::NotFound })
        .layer(middleware::from_fn_with_state(
            app.clone(),
            require_server_token,
        ))
        .layer(middleware::from_fn_with_state(app.clone(), count_responses))
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

/// Counts requests and their 4xx / 5xx answers for the statistics.
async fn count_responses(State(app): State<AppState>, req: Request, next: Next) -> Response {
    let response = next.run(req).await;
    let status = response.status();
    app.stats.add("http.requests", 1);
    if status.is_server_error() {
        app.stats.add("http.5xx", 1);
    } else if status.is_client_error() {
        app.stats.add("http.4xx", 1);
    }
    response
}

#[derive(Deserialize)]
struct StatsQuery {
    days: Option<u32>,
}

/// Aggregate statistics for the operator; only exists while `KP_SYNC_STATS_TOKEN` is set.
async fn stats_report(
    State(app): State<AppState>,
    headers: HeaderMap,
    ApiQuery(query): ApiQuery<StatsQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(expected) = &app.config.stats_token else {
        return Err(ApiError::NotFound);
    };
    let given = headers
        .get("x-kwikpaste-stats-token")
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    if !bool::from(given.ct_eq(expected.as_bytes())) {
        return Err(ApiError::Unauthorized);
    }
    Ok(Json(stats::report(&app, query.days.unwrap_or(30)).await?))
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

fn check_device_info(info: &str) -> Result<(), ApiError> {
    if info.len() <= DEVICE_INFO_MAX {
        Ok(())
    } else {
        Err(ApiError::TooLarge)
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
    app.limits
        .hit(Bucket::Groups, ip)
        .inspect_err(|_| app.stats.add("limit.groupsPerIp", 1))?;
    check_device_id(&req.device_id)?;
    check_device_info(&req.info)?;
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
    app.stats.add("groups.created", 1);
    tracing::info!("group {group} created");
    Ok(Json(json!({ "groupId": group, "deviceToken": token })))
}

async fn create_code(State(app): State<AppState>, member: Member) -> Json<Value> {
    let (code, expires_at) = app.pairing.create_code(&member.group, &member.device);
    app.stats.add("pair.codes", 1);
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

/// Counts a pairing step's failure by its error code.
fn count_pairing<T>(app: &App, result: Result<T, ApiError>) -> Result<T, ApiError> {
    if let Err(err) = &result {
        app.stats.add(&format!("pair.fail.{}", err.code()), 1);
    }
    result
}

async fn join(
    State(app): State<AppState>,
    ClientIp(ip): ClientIp,
    ApiJson(req): ApiJson<JoinRequest>,
) -> Result<Json<Value>, ApiError> {
    app.stats.add("pair.joins", 1);
    count_pairing(&app, relay_join(&app, ip, req).await)
}

/// Relays the joiner's SPAKE2 message to the inviter and waits for its answer.
async fn relay_join(app: &App, ip: IpAddr, req: JoinRequest) -> Result<Json<Value>, ApiError> {
    app.limits
        .hit(Bucket::Join, ip)
        .inspect_err(|_| app.stats.add("limit.joinsPerIp", 1))?;
    app.limits
        .join_allowed()
        .inspect_err(|_| app.stats.add("limit.joinFailures", 1))?;
    check_device_id(&req.device_id)?;
    let join = app
        .pairing
        .start_join(req.code.trim(), &req.device_id)
        .inspect_err(|err| {
            if matches!(err, ApiError::CodeInvalid) {
                app.limits.join_failed();
            }
        })?;
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
    count_pairing(&app, finish_join(&app, &session, req).await)
}

async fn finish_join(app: &App, session: &str, req: CompleteJoin) -> Result<Json<Value>, ApiError> {
    let secret = hex32(&req.join_secret, "joinSecret")?;
    check_device_info(&req.info)?;
    let (group, device) = app.pairing.finish(session, secret.as_bytes())?;
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
    app.stats.add("pair.ok", 1);
    app.stats.add("devices.joined", 1);
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
    check_device_info(&req.info)?;
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
    app.stats.add("devices.removed", 1);
    match removed {
        Removed::Device => {
            app.hub.broadcast(&member.group, &devices_frame(), &[]);
        }
        Removed::Group(blobs) => {
            app.pairing.forget_group(&member.group);
            app.blobs.remove(blobs).await;
            app.stats.add("groups.deleted", 1);
            tracing::info!("group {} deleted: its last device left", member.group);
        }
    }
    Ok(done())
}
