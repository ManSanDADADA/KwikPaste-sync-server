//! Official-server limits (PROTOCOL.md §8): per-IP request windows and daily image quotas.
//! Counters live in memory; a restart resets them.

use crate::{config, error::ApiError};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

/// Stored bytes, each without the version of the entry being replaced.
pub struct StorageUse {
    pub group: u64,
    /// The group's entries that never expire.
    pub persistent: u64,
    pub global: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Bucket {
    Groups,
    Join,
}

pub struct Limits {
    config: config::Limits,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    windows: HashMap<(Bucket, IpAddr), (Instant, u32)>,
    day: Option<NaiveDate>,
    group_images: HashMap<String, u64>,
    global_images: u64,
    group_forward: HashMap<String, u64>,
    join_failures: VecDeque<Instant>,
    device_frames: HashMap<(String, String), VecDeque<Instant>>,
}

const JOIN_FAILURE_WINDOW: Duration = Duration::from_secs(3_600);
const FRAME_WINDOW: Duration = Duration::from_secs(600);

/// Drops timestamps older than `window`.
fn expire(times: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while times
        .front()
        .is_some_and(|t| now.duration_since(*t) >= window)
    {
        times.pop_front();
    }
}

/// Rolling-window check: `Err` with the wait until the oldest timestamp leaves the window.
fn rolling_check(
    times: &mut VecDeque<Instant>,
    max: u32,
    window: Duration,
) -> Result<(), ApiError> {
    let now = Instant::now();
    expire(times, now, window);
    match times.front() {
        Some(oldest) if times.len() >= max as usize => Err(ApiError::RateLimited {
            retry_after: (window - now.duration_since(*oldest)).as_secs().max(1),
        }),
        _ => Ok(()),
    }
}

impl State {
    /// Resets the daily counters after midnight Shanghai; returns the next reset.
    fn roll_day(&mut self) -> DateTime<Utc> {
        let (today, reset) = shanghai_day();
        if self.day != Some(today) {
            self.day = Some(today);
            self.group_images.clear();
            self.global_images = 0;
            self.group_forward.clear();
        }
        reset
    }
}

impl Limits {
    pub fn new(config: config::Limits) -> Self {
        Self {
            config,
            state: Mutex::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn rule(&self, bucket: Bucket) -> Option<(u32, Duration)> {
        match bucket {
            Bucket::Groups => self
                .config
                .groups_per_ip_per_day
                .map(|n| (n, Duration::from_secs(86_400))),
            Bucket::Join => self
                .config
                .joins_per_ip_per_hour
                .map(|n| (n, Duration::from_secs(3_600))),
        }
    }

    /// Counts one request from `ip`; fixed window starting at its first request.
    pub fn hit(&self, bucket: Bucket, ip: IpAddr) -> Result<(), ApiError> {
        let Some((max, window)) = self.rule(bucket) else {
            return Ok(());
        };
        let now = Instant::now();
        let mut state = self.lock();
        let (start, count) = state.windows.entry((bucket, ip)).or_insert((now, 0));
        if now.duration_since(*start) >= window {
            (*start, *count) = (now, 0);
        }
        if *count >= max {
            let retry_after = (window - now.duration_since(*start)).as_secs().max(1);
            return Err(ApiError::RateLimited { retry_after });
        }
        *count += 1;
        Ok(())
    }

    /// Whether `len` more image bytes fit today's quotas; `Err(retryAt)` when not.
    pub fn image_allowed(&self, group: &str, len: u64) -> Result<(), DateTime<Utc>> {
        let Some(quota) = self.config.image_quota else {
            return Ok(());
        };
        let mut state = self.lock();
        let reset = state.roll_day();
        let group_used = state.group_images.get(group).copied().unwrap_or(0);
        if group_used + len > quota.per_group || state.global_images + len > quota.global {
            return Err(reset);
        }
        Ok(())
    }

    pub fn charge_image(&self, group: &str, len: u64) {
        if self.config.image_quota.is_none() {
            return;
        }
        let mut state = self.lock();
        *state.group_images.entry(group.to_owned()).or_insert(0) += len;
        state.global_images += len;
    }

    /// Whether `len` more non-image bytes fit the group's forward budget; `Err(retryAt)` when not.
    pub fn forward_allowed(&self, group: &str, len: u64) -> Result<(), DateTime<Utc>> {
        let Some(budget) = self.config.group_forward_per_day else {
            return Ok(());
        };
        let mut state = self.lock();
        let reset = state.roll_day();
        let used = state.group_forward.get(group).copied().unwrap_or(0);
        if used + len > budget {
            return Err(reset);
        }
        Ok(())
    }

    pub fn charge_forward(&self, group: &str, len: u64) {
        if self.config.group_forward_per_day.is_none() {
            return;
        }
        let mut state = self.lock();
        state.roll_day();
        *state.group_forward.entry(group.to_owned()).or_insert(0) += len;
    }

    /// Fails while the global `codeInvalid` count of the last hour is at its limit.
    pub fn join_allowed(&self) -> Result<(), ApiError> {
        let Some(max) = self.config.join_failures_per_hour else {
            return Ok(());
        };
        rolling_check(&mut self.lock().join_failures, max, JOIN_FAILURE_WINDOW)
    }

    pub fn join_failed(&self) {
        if self.config.join_failures_per_hour.is_some() {
            self.lock().join_failures.push_back(Instant::now());
        }
    }

    /// Counts one uploaded frame from the device; fails once it is over its 10-minute rate.
    pub fn frame(&self, group: &str, device: &str) -> Result<(), ApiError> {
        let Some(max) = self.config.frames_per_device_per_10min else {
            return Ok(());
        };
        let mut state = self.lock();
        let times = state
            .device_frames
            .entry((group.to_owned(), device.to_owned()))
            .or_default();
        rolling_check(times, max, FRAME_WINDOW)?;
        times.push_back(Instant::now());
        Ok(())
    }

    /// Whether any storage cap is set; without one the usage is not even looked up.
    pub fn caps_storage(&self) -> bool {
        let c = &self.config;
        c.group_storage.is_some() || c.group_persistent_storage.is_some() || c.storage.is_some()
    }

    /// Whether `len` more stored bytes fit the group, global and (for entries that never
    /// expire) the group's persistent cap.
    pub fn storage_allowed(&self, used: &StorageUse, len: u64, persistent: bool) -> bool {
        let fits = |used: u64, cap: Option<u64>| cap.is_none_or(|cap| used + len <= cap);
        fits(used.group, self.config.group_storage)
            && fits(used.global, self.config.storage)
            && (!persistent || fits(used.persistent, self.config.group_persistent_storage))
    }

    pub fn purge(&self) {
        let now = Instant::now();
        let day = Duration::from_secs(86_400);
        let mut state = self.lock();
        state
            .windows
            .retain(|_, (start, _)| now.duration_since(*start) < day);
        expire(&mut state.join_failures, now, JOIN_FAILURE_WINDOW);
        state.device_frames.retain(|_, times| {
            expire(times, now, FRAME_WINDOW);
            !times.is_empty()
        });
    }
}

/// Today's date in Asia/Shanghai (UTC+8, no DST) and the next midnight there.
fn shanghai_day() -> (NaiveDate, DateTime<Utc>) {
    let shift = TimeDelta::hours(8);
    let today = (Utc::now() + shift).date_naive();
    let tomorrow = today.succ_opt().unwrap_or(today);
    (today, tomorrow.and_time(NaiveTime::MIN).and_utc() - shift)
}
