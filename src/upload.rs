//! `POST /v1/entries` (PROTOCOL.md §4.6): frames are read and handled one at a time,
//! so memory stays bounded by a single frame whatever the request size.

use crate::{
    blobs::INLINE_MAX,
    config::Images,
    db::{self, NewEntry, Stored},
    error::ApiError,
    extract::Member,
    frames, timestamp, App, AppState,
};
use axum::{body::Body, extract::State, Json};
use chrono::{TimeDelta, Utc};
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntryHeader {
    entry_id: String,
    kind: String,
    class: String,
    hlc: String,
    #[serde(default = "live_default")]
    live: bool,
    #[serde(default)]
    skip: Vec<String>,
}

fn live_default() -> bool {
    true
}

impl EntryHeader {
    fn valid(&self) -> bool {
        let short = |s: &str, max: usize| !s.is_empty() && s.len() <= max;
        short(&self.entry_id, 64)
            && short(&self.kind, 32)
            && short(&self.class, 32)
            && self.hlc.len() <= 128
            && frames::parse_hlc(&self.hlc).is_some()
    }

    /// Event-stream header for this entry; `seq` is `None` for images that are not stored.
    fn event(&self, seq: Option<i64>, from: &str, body: &[u8]) -> axum::body::Bytes {
        let header = json!({
            "type": "entry",
            "seq": seq,
            "entryId": self.entry_id,
            "kind": self.kind,
            "class": self.class,
            "hlc": self.hlc,
            "from": from,
            "live": self.live,
        });
        frames::encode(&header, body)
    }

    /// Everyone not to forward to: the sender plus the devices it already reached over LAN.
    fn skip<'a>(&'a self, sender: &'a str) -> Vec<&'a str> {
        let mut skip: Vec<&str> = self.skip.iter().map(String::as_str).collect();
        skip.push(sender);
        skip
    }
}

pub async fn upload(
    State(app): State<AppState>,
    member: Member,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    let mut reader = StreamReader::new(body.into_data_stream().map_err(std::io::Error::other));
    let policy = &app.config.policy;
    let mut total = 0u64;
    let mut results = Vec::new();

    while let Some((head, len)) = frames::read_head(&mut reader).await? {
        total += 8 + head.len() as u64 + len;
        if app
            .config
            .limits
            .max_request_bytes
            .is_some_and(|max| total > max)
        {
            return Err(ApiError::TooLarge);
        }
        app.limits.frame(&member.group, &member.device)?;
        let header: EntryHeader = serde_json::from_slice(&head)
            .map_err(|err| ApiError::bad(format!("invalid frame header: {err}")))?;

        let image = header.class == "image";
        let max = if image {
            policy.image_max_bytes
        } else {
            policy.max_entry_bytes
        };
        let rejection = if !header.valid() {
            Some("badRequest")
        } else if len > max {
            Some("tooLarge")
        } else if header.kind == "meta" && !policy.meta {
            Some("policy")
        } else {
            None
        };
        if let Some(error) = rejection {
            skip_body(&mut reader, len).await?;
            results
                .push(json!({ "entryId": header.entry_id, "status": "rejected", "error": error }));
            continue;
        }

        let mut body = vec![0; usize::try_from(len).map_err(|_| ApiError::TooLarge)?];
        reader
            .read_exact(&mut body)
            .await
            .map_err(|_| ApiError::bad("truncated frame"))?;
        let result = if image && policy.images == Images::Live {
            forward_image(&app, &member, &header, &body)
        } else {
            store(&app, &member, &header, &body).await?
        };
        results.push(result);
    }
    Ok(Json(json!({ "results": results })))
}

async fn skip_body<R: AsyncRead + Unpin>(reader: &mut R, len: u64) -> Result<(), ApiError> {
    let skipped = tokio::io::copy(&mut reader.take(len), &mut tokio::io::sink()).await;
    match skipped {
        Ok(n) if n == len => Ok(()),
        _ => Err(ApiError::bad("truncated frame")),
    }
}

/// `images = "live"`: forward to online devices within the daily quota, store nothing.
fn forward_image(app: &App, member: &Member, header: &EntryHeader, body: &[u8]) -> Value {
    let len = body.len() as u64;
    if let Err(retry_at) = app.limits.image_allowed(&member.group, len) {
        return json!({
            "entryId": header.entry_id,
            "status": "rejected",
            "error": "quota",
            "retryAt": timestamp(retry_at),
        });
    }
    let frame = header.event(None, &member.device, body);
    if app
        .hub
        .broadcast(&member.group, &frame, &header.skip(&member.device))
        > 0
    {
        app.limits.charge_image(&member.group, len);
    }
    json!({ "entryId": header.entry_id, "status": "forwarded" })
}

/// Last-writer-wins store, then forward with the new `seq`.
async fn store(
    app: &App,
    member: &Member,
    header: &EntryHeader,
    body: &[u8],
) -> Result<Value, ApiError> {
    let len = body.len() as u64;
    let quota = |retry_at| {
        json!({
            "entryId": header.entry_id,
            "status": "rejected",
            "error": "quota",
            "retryAt": timestamp(retry_at),
        })
    };
    let (group_used, global_used) =
        db::stored_bytes(&app.db, &member.group, &header.entry_id).await?;
    if !app.limits.storage_allowed(group_used, global_used, len) {
        return Ok(quota(Utc::now() + TimeDelta::hours(1)));
    }
    let forwards = header.class != "image";
    if forwards {
        if let Err(reset) = app.limits.forward_allowed(&member.group, len) {
            return Ok(quota(reset));
        }
    }

    let blob = if body.len() > INLINE_MAX {
        Some(app.blobs.write(body).await?)
    } else {
        None
    };
    let expires_at = match app.config.policy.text_retention_hours {
        Some(hours) if header.class == "text" => {
            Some(timestamp(Utc::now() + TimeDelta::hours(i64::from(hours))))
        }
        _ => None,
    };
    let entry = NewEntry {
        group_id: &member.group,
        entry_id: &header.entry_id,
        kind: &header.kind,
        class: &header.class,
        hlc: &header.hlc,
        from_device: &member.device,
        body: blob.is_none().then_some(body),
        blob: blob.as_deref(),
        expires_at: expires_at.as_deref(),
        size: len,
    };

    let lock = app.store_lock.lock().await;
    let stored = db::store_entry(&app.db, &entry).await;
    if let Ok(Stored::New { seq, .. }) = &stored {
        let frame = header.event(Some(*seq), &member.device, body);
        let delivered = app
            .hub
            .broadcast(&member.group, &frame, &header.skip(&member.device));
        if forwards {
            app.limits
                .charge_forward(&member.group, len * delivered as u64);
        }
    }
    drop(lock);

    match stored {
        Ok(Stored::New { seq, replaced_blob }) => {
            app.blobs.remove(replaced_blob).await;
            Ok(json!({ "entryId": header.entry_id, "status": "stored", "seq": seq }))
        }
        Ok(Stored::Stale) => {
            app.blobs.remove(blob).await;
            Ok(json!({ "entryId": header.entry_id, "status": "stale" }))
        }
        Err(err) => {
            app.blobs.remove(blob).await;
            Err(err)
        }
    }
}
