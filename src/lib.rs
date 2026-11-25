//! KwikPaste remote sync server: an end-to-end-encrypted last-writer-wins store plus a
//! change stream, per `docs/PROTOCOL.md`.

mod blobs;
pub mod config;
mod db;
mod error;
mod extract;
mod frames;
mod hub;
mod limits;
mod pairing;
mod routes;
mod stream;
mod upload;

use anyhow::Context;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{SecondsFormat, TimeDelta, Utc};
use rand::RngCore;
use sqlx::SqlitePool;
use std::{
    collections::HashSet,
    future::Future,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::net::TcpListener;

pub use config::Config;

pub struct App {
    config: Config,
    db: SqlitePool,
    blobs: blobs::Blobs,
    hub: hub::Hub,
    pairing: pairing::Pairing,
    limits: limits::Limits,
    /// Serializes store + forward so live entries go out in `seq` order.
    store_lock: tokio::sync::Mutex<()>,
    /// Stored bytes over all groups; changed under `store_lock`, recounted on every purge.
    stored: AtomicU64,
}

pub type AppState = Arc<App>;

/// Serves the API on `listener` until `shutdown` resolves.
pub async fn serve(
    config: Config,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let db = db::open(&config.data_dir).await?;
    let blobs = blobs::Blobs::open(&config.data_dir).context("cannot create blobs directory")?;
    let stored = AtomicU64::new(db::total_bytes(&db).await?);
    let app = Arc::new(App {
        limits: limits::Limits::new(config.limits.clone()),
        config,
        db,
        blobs,
        hub: hub::Hub::default(),
        pairing: pairing::Pairing::default(),
        store_lock: tokio::sync::Mutex::new(()),
        stored,
    });

    let purger = tokio::spawn(purge_loop(app.clone()));
    let router = routes::router(app.clone());
    let closing = app.clone();
    let result = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        closing.hub.close_all();
        closing.pairing.clear();
    })
    .await;
    purger.abort();
    app.db.close().await;
    Ok(result?)
}

async fn purge_loop(app: AppState) {
    let mut tick = tokio::time::interval(Duration::from_secs(600));
    loop {
        tick.tick().await;
        if let Err(err) = purge(&app).await {
            tracing::warn!("purge failed: {err:#}");
        }
    }
}

/// Drops expired text, idle groups (official), orphan blob files and stale in-memory state.
async fn purge(app: &App) -> anyhow::Result<()> {
    let expired = db::delete_expired(&app.db, &now()).await?;
    if !expired.is_empty() {
        tracing::info!("purged {} expired blob(s)", expired.len());
    }
    app.blobs.remove(expired).await;

    if let Some(days) = app.config.limits.idle_group_days {
        let cutoff = timestamp(Utc::now() - TimeDelta::days(days));
        for group in db::idle_groups(&app.db, &cutoff).await? {
            if app.hub.group_online(&group) {
                continue;
            }
            let blobs = db::delete_group(&app.db, &group).await?;
            app.blobs.remove(blobs).await;
            app.pairing.forget_group(&group);
            tracing::info!("deleted idle group {group}");
        }
    }

    {
        let _lock = app.store_lock.lock().await;
        app.stored
            .store(db::total_bytes(&app.db).await?, Ordering::Relaxed);
    }

    let referenced: HashSet<String> = db::blob_names(&app.db).await?.into_iter().collect();
    let orphans = app.blobs.remove_orphans(&referenced).await?;
    if orphans > 0 {
        tracing::info!("deleted {orphans} orphan blob file(s)");
    }

    app.pairing.purge();
    app.limits.purge();
    Ok(())
}

/// RFC 3339 UTC with milliseconds; fixed width, so stored values compare as strings.
fn timestamp(t: chrono::DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn now() -> String {
    timestamp(Utc::now())
}

/// 32 random bytes, base64url without padding (43 characters).
pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn token_hash(token: &str) -> [u8; 32] {
    *blake3::hash(token.as_bytes()).as_bytes()
}
