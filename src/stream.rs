//! `GET /v1/events` (PROTOCOL.md §4.7): catch-up, `synced`, then live frames and pings.

use crate::{
    db::{self, EntryRow},
    error::ApiError,
    extract::{ApiQuery, Member},
    frames, now, stats, App, AppState,
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};
use futures_util::stream;
use serde::Deserialize;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{collections::VecDeque, convert::Infallible, time::Duration};
use tokio::{
    sync::mpsc,
    time::{interval_at, Instant, Interval},
};

const PING: Duration = Duration::from_secs(240);

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    after: i64,
    items: Option<String>,
}

pub async fn events(
    State(app): State<AppState>,
    member: Member,
    ApiQuery(query): ApiQuery<EventsQuery>,
) -> Result<Response, ApiError> {
    // Register before reading the max seq: anything stored later arrives live, so
    // catch-up up to `until` plus the live queue leaves no gap.
    let (id, rx, queued) = app.hub.connect(&member.group, &member.device);
    app.stats.add("streams.opened", 1);
    let online = Online {
        app: app.clone(),
        group: member.group,
        device: member.device,
        id,
    };
    let until = db::max_seq(&app.db, &online.group).await?;
    let after = if (0..=until).contains(&query.after) {
        query.after
    } else {
        0
    };
    let events = Events {
        online,
        rx,
        queued,
        after,
        until,
        items: query.items.as_deref() != Some("none"),
        page: VecDeque::new(),
        catching_up: true,
        ping: interval_at(Instant::now() + PING, PING),
    };
    let body = Body::from_stream(stream::unfold(events, next));
    Ok(([(CONTENT_TYPE, frames::CONTENT_TYPE)], body).into_response())
}

/// Marks the device offline when its stream ends, unless a newer stream replaced it.
struct Online {
    app: AppState,
    group: String,
    device: String,
    id: u64,
}

impl Drop for Online {
    fn drop(&mut self) {
        self.app.hub.release(&self.group, &self.device, self.id);
    }
}

struct Events {
    online: Online,
    rx: mpsc::Receiver<Bytes>,
    queued: Arc<AtomicUsize>,
    after: i64,
    until: i64,
    items: bool,
    page: VecDeque<EntryRow>,
    catching_up: bool,
    ping: Interval,
}

/// The next frame, counted as outbound bytes.
async fn next(s: Events) -> Option<(Result<Bytes, Infallible>, Events)> {
    let (frame, s) = next_frame(s).await?;
    s.online.app.stats.bytes_out(frame.len() as u64);
    Some((Ok(frame), s))
}

async fn next_frame(mut s: Events) -> Option<(Bytes, Events)> {
    loop {
        if !s.catching_up {
            let frame = tokio::select! {
                frame = s.rx.recv() => {
                    let frame = frame?;
                    s.queued.fetch_sub(frame.len(), Ordering::Relaxed);
                    frame
                },
                _ = s.ping.tick() => frames::encode(&json!({ "type": "ping" }), &[]),
            };
            return Some((frame, s));
        }
        if let Some(row) = s.page.pop_front() {
            match catch_up_frame(&s.online.app, row).await {
                Some(frame) => return Some((frame, s)),
                None => continue,
            }
        }
        let app = &s.online.app;
        let page =
            db::catch_up_page(&app.db, &s.online.group, s.after, s.until, s.items, &now()).await;
        let rows = match page {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!("catch-up query failed: {err}");
                app.stats.add("errors.catchup", 1);
                return None;
            }
        };
        match rows.last() {
            Some(last) => {
                s.after = last.seq;
                s.page.extend(rows);
            }
            None => {
                s.catching_up = false;
                let synced = frames::encode(&json!({ "type": "synced", "seq": s.until }), &[]);
                return Some((synced, s));
            }
        }
    }
}

async fn catch_up_frame(app: &App, row: EntryRow) -> Option<Bytes> {
    let body = match (row.body, &row.blob) {
        (Some(body), _) => body,
        // A missing file means a newer version replaced this one; it arrives live.
        (None, Some(name)) => match app.blobs.read(name).await {
            Ok(body) => body,
            Err(err) => {
                tracing::warn!("skipping entry with unreadable blob {name}: {err}");
                return None;
            }
        },
        (None, None) => Vec::new(),
    };
    let bucket = stats::bucket(&row.kind, &row.class);
    app.stats.add(&format!("catchup.{bucket}.n"), 1);
    app.stats
        .add(&format!("catchup.{bucket}.bytes"), body.len() as u64);
    let header = json!({
        "type": "entry",
        "seq": row.seq,
        "entryId": row.entry_id,
        "kind": row.kind,
        "class": row.class,
        "hlc": row.hlc,
        "from": row.from_device,
        "live": false,
    });
    Some(frames::encode(&header, &body))
}
