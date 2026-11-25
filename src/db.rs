//! SQLite storage. Every write transaction starts with a write so the lock is taken up front.

use crate::{error::ApiError, frames::hlc_newer};
use anyhow::Context;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    FromRow, SqlitePool,
};
use std::{path::Path, time::Duration};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS devices (
    group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    token_hash BLOB NOT NULL UNIQUE,
    info TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (group_id, device_id)
);
CREATE TABLE IF NOT EXISTS entries (
    group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    entry_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    class TEXT NOT NULL,
    hlc TEXT NOT NULL,
    from_device TEXT NOT NULL,
    body BLOB,
    blob TEXT,
    expires_at TEXT,
    PRIMARY KEY (group_id, entry_id)
);
CREATE INDEX IF NOT EXISTS entries_by_seq ON entries (group_id, seq);
CREATE INDEX IF NOT EXISTS entries_by_expiry ON entries (expires_at) WHERE expires_at IS NOT NULL;
";

pub async fn open(data_dir: &Path) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(data_dir.join("sync.sqlite3"))
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(10));
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .context("cannot open sync.sqlite3")?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    Ok(pool)
}

pub async fn create_group(
    pool: &SqlitePool,
    group_id: &str,
    device_id: &str,
    info: &str,
    token_hash: &[u8],
    now: &str,
) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO groups (id, created_at) VALUES (?, ?)")
        .bind(group_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    insert_device(&mut tx, group_id, device_id, info, token_hash, now).await?;
    tx.commit().await?;
    Ok(())
}

/// Registers a device; re-pairing an existing device id replaces its token.
pub async fn add_device(
    pool: &SqlitePool,
    group_id: &str,
    device_id: &str,
    info: &str,
    token_hash: &[u8],
    max_devices: u32,
    now: &str,
) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    // A no-op write: takes the write lock and tells whether the group still exists.
    let touched = sqlx::query("UPDATE groups SET seq = seq WHERE id = ?")
        .bind(group_id)
        .execute(&mut *tx)
        .await?;
    if touched.rows_affected() == 0 {
        return Err(ApiError::CodeInvalid);
    }
    insert_device(&mut tx, group_id, device_id, info, token_hash, now).await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE group_id = ?")
        .bind(group_id)
        .fetch_one(&mut *tx)
        .await?;
    if count > i64::from(max_devices) {
        return Err(ApiError::GroupFull);
    }
    tx.commit().await?;
    Ok(())
}

async fn insert_device(
    tx: &mut sqlx::SqliteConnection,
    group_id: &str,
    device_id: &str,
    info: &str,
    token_hash: &[u8],
    now: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO devices (group_id, device_id, token_hash, info, created_at, last_seen_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (group_id, device_id) DO UPDATE SET
             token_hash = excluded.token_hash, info = excluded.info, last_seen_at = excluded.last_seen_at",
    )
    .bind(group_id)
    .bind(device_id)
    .bind(token_hash)
    .bind(info)
    .bind(now)
    .bind(now)
    .execute(tx)
    .await?;
    Ok(())
}

/// Looks a device up by its token hash and records it as seen.
pub async fn touch_device(
    pool: &SqlitePool,
    token_hash: &[u8],
    now: &str,
) -> Result<Option<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "UPDATE devices SET last_seen_at = ? WHERE token_hash = ? RETURNING group_id, device_id",
    )
    .bind(now)
    .bind(token_hash)
    .fetch_optional(pool)
    .await
}

#[derive(FromRow)]
pub struct DeviceRow {
    pub device_id: String,
    pub info: String,
    pub created_at: String,
    pub last_seen_at: String,
}

pub async fn list_devices(
    pool: &SqlitePool,
    group_id: &str,
) -> Result<Vec<DeviceRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT device_id, info, created_at, last_seen_at FROM devices
         WHERE group_id = ? ORDER BY created_at, device_id",
    )
    .bind(group_id)
    .fetch_all(pool)
    .await
}

pub async fn update_device_info(
    pool: &SqlitePool,
    group_id: &str,
    device_id: &str,
    info: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE devices SET info = ? WHERE group_id = ? AND device_id = ?")
        .bind(info)
        .bind(group_id)
        .bind(device_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub enum Removed {
    Device,
    /// The last device left; the group is gone and these blob files must be deleted.
    Group(Vec<String>),
}

pub async fn remove_device(
    pool: &SqlitePool,
    group_id: &str,
    device_id: &str,
) -> Result<Option<Removed>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let deleted = sqlx::query("DELETE FROM devices WHERE group_id = ? AND device_id = ?")
        .bind(group_id)
        .bind(device_id)
        .execute(&mut *tx)
        .await?;
    if deleted.rows_affected() == 0 {
        return Ok(None);
    }
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE group_id = ?")
        .bind(group_id)
        .fetch_one(&mut *tx)
        .await?;
    let removed = if left == 0 {
        Removed::Group(delete_group_in(&mut tx, group_id).await?)
    } else {
        Removed::Device
    };
    tx.commit().await?;
    Ok(Some(removed))
}

pub async fn delete_group(pool: &SqlitePool, group_id: &str) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let blobs = delete_group_in(&mut tx, group_id).await?;
    tx.commit().await?;
    Ok(blobs)
}

async fn delete_group_in(
    tx: &mut sqlx::SqliteConnection,
    group_id: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let blobs: Vec<Option<String>> =
        sqlx::query_scalar("DELETE FROM entries WHERE group_id = ? RETURNING blob")
            .bind(group_id)
            .fetch_all(&mut *tx)
            .await?;
    sqlx::query("DELETE FROM groups WHERE id = ?")
        .bind(group_id)
        .execute(&mut *tx)
        .await?;
    Ok(blobs.into_iter().flatten().collect())
}

pub struct NewEntry<'a> {
    pub group_id: &'a str,
    pub entry_id: &'a str,
    pub kind: &'a str,
    pub class: &'a str,
    pub hlc: &'a str,
    pub from_device: &'a str,
    pub body: Option<&'a [u8]>,
    pub blob: Option<&'a str>,
    pub expires_at: Option<&'a str>,
}

pub enum Stored {
    /// Stored under `seq`; `replaced_blob` belonged to the previous version.
    New {
        seq: i64,
        replaced_blob: Option<String>,
    },
    Stale,
}

/// Last-writer-wins store: keeps only the newest version per entry id, each under a new `seq`.
pub async fn store_entry(pool: &SqlitePool, e: &NewEntry<'_>) -> Result<Stored, ApiError> {
    let mut tx = pool.begin().await?;
    let seq: Option<i64> =
        sqlx::query_scalar("UPDATE groups SET seq = seq + 1 WHERE id = ? RETURNING seq")
            .bind(e.group_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(seq) = seq else {
        return Err(ApiError::Unauthorized);
    };
    let existing: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT hlc, blob FROM entries WHERE group_id = ? AND entry_id = ?")
            .bind(e.group_id)
            .bind(e.entry_id)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some((old_hlc, _)) = &existing {
        if !hlc_newer(e.hlc, old_hlc) {
            return Ok(Stored::Stale);
        }
    }
    sqlx::query(
        "INSERT INTO entries (group_id, entry_id, seq, kind, class, hlc, from_device, body, blob, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (group_id, entry_id) DO UPDATE SET
             seq = excluded.seq, kind = excluded.kind, class = excluded.class, hlc = excluded.hlc,
             from_device = excluded.from_device, body = excluded.body, blob = excluded.blob,
             expires_at = excluded.expires_at",
    )
    .bind(e.group_id)
    .bind(e.entry_id)
    .bind(seq)
    .bind(e.kind)
    .bind(e.class)
    .bind(e.hlc)
    .bind(e.from_device)
    .bind(e.body)
    .bind(e.blob)
    .bind(e.expires_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Stored::New {
        seq,
        replaced_blob: existing.and_then(|(_, blob)| blob),
    })
}

pub async fn max_seq(pool: &SqlitePool, group_id: &str) -> Result<i64, sqlx::Error> {
    Ok(sqlx::query_scalar("SELECT seq FROM groups WHERE id = ?")
        .bind(group_id)
        .fetch_optional(pool)
        .await?
        .unwrap_or(0))
}

#[derive(FromRow)]
pub struct EntryRow {
    pub seq: i64,
    pub entry_id: String,
    pub kind: String,
    pub class: String,
    pub hlc: String,
    pub from_device: String,
    pub body: Option<Vec<u8>>,
    pub blob: Option<String>,
}

/// One page of catch-up entries with `after < seq <= until`, oldest first.
pub async fn catch_up_page(
    pool: &SqlitePool,
    group_id: &str,
    after: i64,
    until: i64,
    items: bool,
    now: &str,
) -> Result<Vec<EntryRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT seq, entry_id, kind, class, hlc, from_device, body, blob FROM entries
         WHERE group_id = ? AND seq > ? AND seq <= ?
           AND (expires_at IS NULL OR expires_at > ?)
           AND (? OR kind NOT IN ('item', 'meta'))
         ORDER BY seq LIMIT 64",
    )
    .bind(group_id)
    .bind(after)
    .bind(until)
    .bind(now)
    .bind(items)
    .fetch_all(pool)
    .await
}

/// Deletes expired entries and returns their blob files.
pub async fn delete_expired(pool: &SqlitePool, now: &str) -> Result<Vec<String>, sqlx::Error> {
    let blobs: Vec<Option<String>> = sqlx::query_scalar(
        "DELETE FROM entries WHERE expires_at IS NOT NULL AND expires_at <= ? RETURNING blob",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    Ok(blobs.into_iter().flatten().collect())
}

/// Groups whose devices were all last seen before `cutoff`.
pub async fn idle_groups(pool: &SqlitePool, cutoff: &str) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT g.id FROM groups g
         WHERE COALESCE((SELECT MAX(last_seen_at) FROM devices d WHERE d.group_id = g.id), g.created_at) < ?",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
}

pub async fn blob_names(pool: &SqlitePool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT blob FROM entries WHERE blob IS NOT NULL")
        .fetch_all(pool)
        .await
}
