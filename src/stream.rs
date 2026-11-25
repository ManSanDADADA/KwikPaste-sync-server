//! `GET /v1/events` (PROTOCOL.md §4.7): catch-up, `synced`, then live frames and pings.

use crate::{
    db::{self, EntryRow},
    error::ApiError,
    extract::{ApiQuery, Member},
    frames, now, App, AppState,
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
    let (id, rx) = app.hub.connect(&member.group, &member.device);
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
    after: i64,
    until: i64,
    items: bool,
    page: VecDeque<EntryRow>,
    catching_up: bool,
    ping: Interval,
}

async fn next(mut s: Events) -> Option<(Result<Bytes, Infallible>, Events)> {
    loop {
        if !s.catching_up {
            let frame = tokio::select! {
                frame = s.rx.recv() => frame?,
                _ = s.ping.tick() => frames::encode(&json!({ "type": "ping" }), &[]),
            };
            return Some((Ok(frame), s));
        }
        if let Some(row) = s.page.pop_front() {
            match catch_up_frame(&s.online.app, row).await {
                Some(frame) => return Some((Ok(frame), s)),
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
                return Some((Ok(synced), s));
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
