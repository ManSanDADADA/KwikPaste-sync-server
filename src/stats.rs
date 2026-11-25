//! Operator statistics (README「统计接口」). Counters are summed per UTC minute and kept 8 days,
//! rolled up per Asia/Shanghai day and kept for good; which devices were active on which day
//! is kept 32 days for the active counts. Nothing is recorded unless a stats token is set, and
//! the report holds only aggregates: no group or device ids, no IPs, no content.

use crate::App;
use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveTime, SecondsFormat, TimeDelta, Utc};
use serde_json::{json, Map, Value};
use sqlx::{Sqlite, SqliteConnection, SqlitePool, Transaction};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

const MINUTE_RETENTION_DAYS: i64 = 8;
const DEVICE_DAY_RETENTION_DAYS: i64 = 32;
/// Minutes in the report's series: the last 24 hours.
const SERIES_MINUTES: i64 = 24 * 60;
pub const MAX_DAYS: u32 = 400;

/// The traffic bucket an entry counts under.
pub fn bucket(kind: &str, class: &str) -> &'static str {
    match (kind, class) {
        ("settings", _) => "settings",
        (_, "image") => "image",
        ("item", "text") => "text",
        _ => "other",
    }
}

/// The version and platform from `X-KwikPaste-Client: KwikPaste/<version> (<windows|macos>)`;
/// anything else counts as unknown.
fn parse_client(header: &str) -> Option<(String, &'static str)> {
    let (version, platform) = header.strip_prefix("KwikPaste/")?.split_once(" (")?;
    let platform = match platform.strip_suffix(')')? {
        "windows" => "windows",
        "macos" => "macos",
        _ => return None,
    };
    let valid = !version.is_empty()
        && version.len() <= 32
        && version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b));
    valid.then(|| (version.to_owned(), platform))
}

type Minutes = HashMap<i64, HashMap<String, u64>>;
type Active = HashMap<(NaiveDate, String, String), Option<(String, &'static str)>>;

#[derive(Default)]
struct Pending {
    /// Unix minute → metric → value.
    minutes: Minutes,
    /// Devices seen since the last flush, per day, with the client they reported.
    active: Active,
    /// The second being measured for the bandwidth peaks, and its bytes so far.
    second: i64,
    second_in: u64,
    second_out: u64,
}

impl Pending {
    fn close_second(&mut self) {
        let minute = self.second.div_euclid(60);
        add_to(&mut self.minutes, minute, "bw.in.max", self.second_in);
        add_to(&mut self.minutes, minute, "bw.out.max", self.second_out);
        (self.second_in, self.second_out) = (0, 0);
    }
}

fn is_peak(metric: &str) -> bool {
    metric.ends_with(".max")
}

fn add_to(minutes: &mut Minutes, minute: i64, metric: &str, n: u64) {
    if n == 0 {
        return;
    }
    let metrics = minutes.entry(minute).or_default();
    match metrics.get_mut(metric) {
        Some(v) if is_peak(metric) => *v = (*v).max(n),
        Some(v) => *v += n,
        None => {
            metrics.insert(metric.to_owned(), n);
        }
    }
}

pub struct Stats {
    enabled: bool,
    pending: Mutex<Pending>,
}

impl Stats {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            pending: Mutex::default(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    fn lock(&self) -> MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Adds `n` to a count, or raises a peak (`*.max`) to `n`, in the current minute.
    pub fn add(&self, metric: &str, n: u64) {
        if self.enabled {
            let minute = Utc::now().timestamp().div_euclid(60);
            add_to(&mut self.lock().minutes, minute, metric, n);
        }
    }

    /// Request bytes received, for the bandwidth sums and per-second peaks.
    pub fn bytes_in(&self, n: u64) {
        self.bytes(n, true);
    }

    /// Bytes handed to event streams, for the bandwidth sums and per-second peaks.
    pub fn bytes_out(&self, n: u64) {
        self.bytes(n, false);
    }

    fn bytes(&self, n: u64, inbound: bool) {
        if !self.enabled || n == 0 {
            return;
        }
        let now = Utc::now().timestamp();
        let mut p = self.lock();
        if p.second != now {
            p.close_second();
            p.second = now;
        }
        let metric = if inbound {
            p.second_in += n;
            "bytes.in"
        } else {
            p.second_out += n;
            "bytes.out"
        };
        add_to(&mut p.minutes, now.div_euclid(60), metric, n);
    }

    /// Marks a device active today, with the `X-KwikPaste-Client` header it sent.
    pub fn active(&self, group: &str, device: &str, client: Option<&str>) {
        if !self.enabled {
            return;
        }
        let day = shanghai_date(Utc::now());
        let client = client.and_then(parse_client);
        let mut p = self.lock();
        let slot = p
            .active
            .entry((day, group.to_owned(), device.to_owned()))
            .or_default();
        if client.is_some() {
            *slot = client;
        }
    }

    /// Takes everything pending. The second in progress is closed early, so its peak can
    /// come out a little low.
    fn take(&self) -> (Minutes, Active) {
        let mut p = self.lock();
        p.close_second();
        (
            std::mem::take(&mut p.minutes),
            std::mem::take(&mut p.active),
        )
    }
}

fn shanghai_date(t: DateTime<Utc>) -> NaiveDate {
    (t + TimeDelta::hours(8)).date_naive()
}

fn minute_key(minute: i64) -> String {
    DateTime::from_timestamp(minute * 60, 0)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%MZ")
        .to_string()
}

/// The first UTC minute of a Shanghai day.
fn day_start(day: NaiveDate) -> i64 {
    (day.and_time(NaiveTime::MIN).and_utc() - TimeDelta::hours(8))
        .timestamp()
        .div_euclid(60)
}

/// Runs [`flush`] just after every minute boundary.
pub async fn flush_loop(app: crate::AppState) {
    loop {
        let into_minute = Utc::now().timestamp_millis().rem_euclid(60_000);
        tokio::time::sleep(Duration::from_millis((60_050 - into_minute) as u64)).await;
        if let Err(err) = flush(&app).await {
            tracing::warn!("stats flush failed: {err:#}");
        }
    }
}

/// Writes the pending counters, the online gauges and today's active devices, then rolls
/// today and yesterday up from their minutes.
pub async fn flush(app: &App) -> Result<()> {
    if !app.stats.enabled {
        return Ok(());
    }
    let now = Utc::now();
    let (minutes, active) = app.stats.take();
    let online = app.hub.online();
    let online_groups = online.iter().map(|(g, _)| g).collect::<HashSet<_>>().len();
    let today = shanghai_date(now);
    let current = now.timestamp().div_euclid(60);
    let dropped = app.hub.take_dropped();

    let mut tx = app.db.begin().await?;
    for (minute, metrics) in &minutes {
        let key = minute_key(*minute);
        for (metric, value) in metrics {
            upsert_minute(&mut tx, &key, metric, *value).await?;
        }
    }
    let key = minute_key(current);
    for (metric, value) in [
        ("online.devices.max", online.len() as u64),
        ("online.groups.max", online_groups as u64),
        ("streams.dropped", dropped),
    ] {
        upsert_minute(&mut tx, &key, metric, value).await?;
    }
    for ((day, group, device), client) in &active {
        let (version, platform) = client
            .as_ref()
            .map_or((None, None), |(v, p)| (Some(v.as_str()), Some(*p)));
        sqlx::query(
            "INSERT INTO stats_device_days (day, group_id, device_id, version, platform)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT (day, group_id, device_id) DO UPDATE SET
                 version = COALESCE(excluded.version, version),
                 platform = COALESCE(excluded.platform, platform)",
        )
        .bind(day.to_string())
        .bind(group)
        .bind(device)
        .bind(version)
        .bind(platform)
        .execute(&mut *tx)
        .await?;
    }
    // A stream left open over midnight makes no request, yet its device is active.
    for (group, device) in &online {
        sqlx::query(
            "INSERT OR IGNORE INTO stats_device_days (day, group_id, device_id) VALUES (?, ?, ?)",
        )
        .bind(today.to_string())
        .bind(group)
        .bind(device)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    rollup(&app.db, today, true).await?;
    if let Some(yesterday) = today.pred_opt() {
        rollup(&app.db, yesterday, false).await?;
    }
    Ok(())
}

async fn upsert_minute(
    tx: &mut Transaction<'_, Sqlite>,
    minute: &str,
    metric: &str,
    value: u64,
) -> Result<(), sqlx::Error> {
    if value == 0 {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO stats_minutes (minute, metric, value) VALUES (?, ?, ?)
         ON CONFLICT (minute, metric) DO UPDATE SET value =
             CASE WHEN metric LIKE '%.max' THEN MAX(value, excluded.value) ELSE value + excluded.value END",
    )
    .bind(minute)
    .bind(metric)
    .bind(i64::try_from(value).unwrap_or(i64::MAX))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_day(
    conn: &mut SqliteConnection,
    day: &str,
    metric: &str,
    value: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO stats_days (day, metric, value) VALUES (?, ?, ?)
         ON CONFLICT (day, metric) DO UPDATE SET value = excluded.value",
    )
    .bind(day)
    .bind(metric)
    .bind(value)
    .execute(conn)
    .await?;
    Ok(())
}

/// Recomputes a day's sums and peaks from its minutes plus its active counts; with
/// `totals`, also records the current group, device and storage totals as the day's.
async fn rollup(pool: &SqlitePool, day: NaiveDate, totals: bool) -> Result<()> {
    let key = day.to_string();
    let back = |days: i64| (day - TimeDelta::days(days)).to_string();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO stats_days (day, metric, value)
         SELECT ?, metric, CASE WHEN metric LIKE '%.max' THEN MAX(value) ELSE SUM(value) END
         FROM stats_minutes WHERE minute >= ? AND minute < ? GROUP BY metric
         ON CONFLICT (day, metric) DO UPDATE SET value = excluded.value",
    )
    .bind(&key)
    .bind(minute_key(day_start(day)))
    .bind(minute_key(day_start(day) + 24 * 60))
    .execute(&mut *tx)
    .await?;

    let active: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM stats_device_days WHERE day = ?1),
             (SELECT COUNT(DISTINCT group_id) FROM stats_device_days WHERE day = ?1),
             (SELECT COUNT(*) FROM (SELECT DISTINCT group_id, device_id FROM stats_device_days
                                    WHERE day > ?2 AND day <= ?1)),
             (SELECT COUNT(DISTINCT group_id) FROM stats_device_days WHERE day > ?2 AND day <= ?1),
             (SELECT COUNT(*) FROM (SELECT DISTINCT group_id, device_id FROM stats_device_days
                                    WHERE day > ?3 AND day <= ?1)),
             (SELECT COUNT(DISTINCT group_id) FROM stats_device_days WHERE day > ?3 AND day <= ?1)",
    )
    .bind(&key)
    .bind(back(7))
    .bind(back(30))
    .fetch_one(&mut *tx)
    .await?;
    let mut values = vec![
        ("active.devices", active.0),
        ("active.groups", active.1),
        ("active.devices.7d", active.2),
        ("active.groups.7d", active.3),
        ("active.devices.30d", active.4),
        ("active.groups.30d", active.5),
    ];
    if totals {
        let (groups, devices, entries, stored): (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM groups), (SELECT COUNT(*) FROM devices),
                    (SELECT COUNT(*) FROM entries), (SELECT COALESCE(SUM(size), 0) FROM entries)",
        )
        .fetch_one(&mut *tx)
        .await?;
        values.extend([
            ("total.groups", groups),
            ("total.devices", devices),
            ("total.entries", entries),
            ("total.storedBytes", stored),
        ]);
    }
    for (metric, value) in values {
        upsert_day(&mut tx, &key, metric, value).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Drops minutes and device days past their retention.
pub async fn purge(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let now = Utc::now();
    let minutes = now.timestamp().div_euclid(60) - MINUTE_RETENTION_DAYS * 24 * 60;
    sqlx::query("DELETE FROM stats_minutes WHERE minute < ?")
        .bind(minute_key(minutes))
        .execute(pool)
        .await?;
    let day = shanghai_date(now) - TimeDelta::days(DEVICE_DAY_RETENTION_DAYS);
    sqlx::query("DELETE FROM stats_device_days WHERE day < ?")
        .bind(day.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

/// `GET /v1/stats`: the live snapshot, the last 24 hours per minute and `days` days.
pub async fn report(app: &App, days: u32) -> Result<Value> {
    flush(app).await?;
    let now = Utc::now();
    let today = shanghai_date(now);
    let (online_devices, online_groups) = {
        let online = app.hub.online();
        let groups = online.iter().map(|(g, _)| g).collect::<HashSet<_>>().len();
        (online.len(), groups)
    };
    let (codes, joins) = app.pairing.counts();
    let (groups, devices, entries, stored): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM groups), (SELECT COUNT(*) FROM devices),
                (SELECT COUNT(*) FROM entries), (SELECT COALESCE(SUM(size), 0) FROM entries)",
    )
    .fetch_one(&app.db)
    .await?;
    let group_sizes: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT n, COUNT(*) FROM (SELECT COUNT(*) AS n FROM devices GROUP BY group_id)
         GROUP BY n ORDER BY n",
    )
    .fetch_all(&app.db)
    .await?;
    // Each device under the newest client it reported; devices that never sent one are unknown.
    let clients: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT COALESCE(v.version, ''), COALESCE(v.platform, ''), COUNT(*)
         FROM (SELECT DISTINCT group_id, device_id FROM stats_device_days WHERE day > ?) a
         LEFT JOIN stats_device_days v ON v.group_id = a.group_id AND v.device_id = a.device_id
             AND v.day = (SELECT MAX(day) FROM stats_device_days w
                          WHERE w.group_id = a.group_id AND w.device_id = a.device_id
                            AND w.version IS NOT NULL)
         GROUP BY 1, 2 ORDER BY 3 DESC",
    )
    .bind((today - TimeDelta::days(7)).to_string())
    .fetch_all(&app.db)
    .await?;

    let quota = app.config.limits.image_quota;
    let data_dir = app.config.data_dir.clone();
    let data_bytes = tokio::task::spawn_blocking(move || dir_bytes(&data_dir))
        .await
        .unwrap_or(0);

    Ok(json!({
        "serverVersion": env!("CARGO_PKG_VERSION"),
        "mode": app.config.mode,
        "now": now.to_rfc3339_opts(SecondsFormat::Secs, true),
        "startedAt": app.started_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        "online": {
            "devices": online_devices,
            "groups": online_groups,
            "pairingCodes": codes,
            "joinsWaiting": joins,
        },
        "totals": {
            "groups": groups,
            "devices": devices,
            "entries": entries,
            "storedBytes": stored,
        },
        "groupSizes": group_sizes
            .into_iter()
            .map(|(devices, groups)| json!({ "devices": devices, "groups": groups }))
            .collect::<Vec<_>>(),
        "clients": clients
            .into_iter()
            .map(|(version, platform, devices)| {
                json!({ "version": version, "platform": platform, "devices": devices })
            })
            .collect::<Vec<_>>(),
        "limits": {
            "storageBytes": app.config.limits.storage,
            "groupStorageBytes": app.config.limits.group_storage,
            "imagesPerDayBytes": quota.map(|q| q.global),
            "groupImagesPerDayBytes": quota.map(|q| q.per_group),
            "groupForwardPerDayBytes": app.config.limits.group_forward_per_day,
            "maxDevices": app.config.policy.max_devices,
        },
        "usage": {
            "imagesTodayBytes": app.limits.images_today(),
        },
        "process": {
            "rssBytes": proc_rss(),
            "memoryLimitBytes": cgroup_memory_limit(),
            "dataBytes": data_bytes,
        },
        "minutes": series(&app.db, now).await?,
        "days": daily(&app.db, today, days.clamp(1, MAX_DAYS)).await?,
    }))
}

/// The last 24 hours per minute as dense arrays, oldest first.
async fn series(pool: &SqlitePool, now: DateTime<Utc>) -> Result<Value> {
    let last = now.timestamp().div_euclid(60);
    let first = last - SERIES_MINUTES + 1;
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT minute, metric, value FROM stats_minutes WHERE minute >= ?
           AND (metric IN ('bytes.in', 'bytes.out', 'bw.in.max', 'bw.out.max', 'online.devices.max')
                OR metric LIKE 'up.%.n')",
    )
    .bind(minute_key(first))
    .fetch_all(pool)
    .await?;
    let index: HashMap<String, usize> = (first..=last)
        .enumerate()
        .map(|(i, minute)| (minute_key(minute), i))
        .collect();
    let len = index.len();
    let mut columns: HashMap<&str, Vec<i64>> = HashMap::new();
    for (minute, metric, value) in rows {
        let Some(&i) = index.get(&minute) else {
            continue;
        };
        let column = match metric.as_str() {
            "bytes.in" => "bytesIn",
            "bytes.out" => "bytesOut",
            "bw.in.max" => "peakIn",
            "bw.out.max" => "peakOut",
            "online.devices.max" => "online",
            _ => "uploads",
        };
        columns.entry(column).or_insert_with(|| vec![0; len])[i] += value;
    }
    let mut out = Map::new();
    out.insert("start".into(), json!(minute_key(first)));
    for column in [
        "bytesIn", "bytesOut", "peakIn", "peakOut", "online", "uploads",
    ] {
        let values = columns.remove(column).unwrap_or_else(|| vec![0; len]);
        out.insert(column.into(), json!(values));
    }
    Ok(Value::Object(out))
}

/// `days` days ending today, oldest first, each with every metric recorded that day.
async fn daily(pool: &SqlitePool, today: NaiveDate, days: u32) -> Result<Value> {
    let first = today - TimeDelta::days(i64::from(days) - 1);
    let rows: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT day, metric, value FROM stats_days WHERE day >= ? ORDER BY day")
            .bind(first.to_string())
            .fetch_all(pool)
            .await?;
    let mut by_day: HashMap<String, Map<String, Value>> = HashMap::new();
    for (day, metric, value) in rows {
        by_day.entry(day).or_default().insert(metric, json!(value));
    }
    Ok(Value::Array(
        first
            .iter_days()
            .take(days as usize)
            .map(|day| {
                let key = day.to_string();
                let metrics = by_day.remove(&key).unwrap_or_default();
                json!({ "day": key, "metrics": metrics })
            })
            .collect(),
    ))
}

/// Bytes in the data directory (database, WAL, blobs).
fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_bytes(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Resident memory of this process (Linux only).
fn proc_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

/// The container's memory limit (cgroup v2), when one is set.
fn cgroup_memory_limit() -> Option<u64> {
    std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_header() {
        assert_eq!(
            parse_client("KwikPaste/2.0.11 (windows)"),
            Some(("2.0.11".into(), "windows"))
        );
        assert_eq!(
            parse_client("KwikPaste/2.1.0-beta.1 (macos)"),
            Some(("2.1.0-beta.1".into(), "macos"))
        );
        assert_eq!(parse_client("KwikPaste/2.0.11 (linux)"), None);
        assert_eq!(parse_client("KwikPaste/<script> (windows)"), None);
        assert_eq!(parse_client("curl/8"), None);
    }

    #[test]
    fn shanghai_day_starts_at_16_utc() {
        let day = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        assert_eq!(minute_key(day_start(day)), "2026-11-25T16:00Z");
    }
}
