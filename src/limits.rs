//! Official-server limits (PROTOCOL.md §8): per-IP request windows and daily image quotas.
//! Counters live in memory; a restart resets them.

use crate::{config, error::ApiError};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

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
        let (today, reset) = shanghai_day();
        if state.day != Some(today) {
            state.day = Some(today);
            state.group_images.clear();
            state.global_images = 0;
        }
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

    pub fn purge(&self) {
        let now = Instant::now();
        let day = Duration::from_secs(86_400);
        self.lock()
            .windows
            .retain(|_, (start, _)| now.duration_since(*start) < day);
    }
}

/// Today's date in Asia/Shanghai (UTC+8, no DST) and the next midnight there.
fn shanghai_day() -> (NaiveDate, DateTime<Utc>) {
    let shift = TimeDelta::hours(8);
    let today = (Utc::now() + shift).date_naive();
    let tomorrow = today.succ_opt().unwrap_or(today);
    (today, tomorrow.and_time(NaiveTime::MIN).and_utc() - shift)
}
