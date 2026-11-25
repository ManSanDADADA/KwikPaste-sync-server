use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::{env, fs, path::PathBuf};

const MIB: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Official,
    Selfhost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Images {
    /// Forwarded to online devices, never stored.
    Live,
    Stored,
}

/// The policy clients receive from `GET /v1/info` (PROTOCOL.md §4.1).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    pub history: bool,
    pub meta: bool,
    pub settings: bool,
    pub text_retention_hours: Option<u32>,
    pub images: Images,
    pub image_max_bytes: u64,
    pub image_compress: Option<ImageCompress>,
    pub max_entry_bytes: u64,
    pub max_devices: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageCompress {
    pub max_side: u32,
    pub quality: u8,
}

/// Server-side limits that are not part of the published policy (PROTOCOL.md §8).
#[derive(Clone, Debug, Default)]
pub struct Limits {
    pub max_request_bytes: Option<u64>,
    pub groups_per_ip_per_day: Option<u32>,
    pub joins_per_ip_per_hour: Option<u32>,
    pub image_quota: Option<ImageQuota>,
    pub idle_group_days: Option<i64>,
}

/// Daily image forwarding volume, reset at 00:00 Asia/Shanghai.
#[derive(Clone, Copy, Debug)]
pub struct ImageQuota {
    pub per_group: u64,
    pub global: u64,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub mode: Mode,
    pub data_dir: PathBuf,
    /// Required in every request on a self-hosted server.
    pub server_token: Option<String>,
    pub trust_proxy: bool,
    pub policy: Policy,
    pub limits: Limits,
}

impl Config {
    pub fn official(data_dir: PathBuf) -> Self {
        Self {
            mode: Mode::Official,
            data_dir,
            server_token: None,
            trust_proxy: false,
            policy: Policy {
                history: false,
                meta: false,
                settings: true,
                text_retention_hours: Some(24),
                images: Images::Live,
                image_max_bytes: 2 * MIB,
                image_compress: Some(ImageCompress {
                    max_side: 1920,
                    quality: 70,
                }),
                max_entry_bytes: MIB,
                max_devices: 10,
            },
            limits: Limits {
                max_request_bytes: Some(8 * MIB),
                groups_per_ip_per_day: Some(10),
                joins_per_ip_per_hour: Some(30),
                image_quota: Some(ImageQuota {
                    per_group: 30 * MIB,
                    global: 1024 * MIB,
                }),
                idle_group_days: Some(90),
            },
        }
    }

    pub fn selfhost(data_dir: PathBuf, server_token: String) -> Self {
        Self {
            mode: Mode::Selfhost,
            data_dir,
            server_token: Some(server_token),
            trust_proxy: false,
            policy: Policy {
                history: true,
                meta: true,
                settings: true,
                text_retention_hours: None,
                images: Images::Stored,
                image_max_bytes: 100 * MIB + 64,
                image_compress: None,
                max_entry_bytes: 16 * MIB,
                max_devices: 20,
            },
            limits: Limits::default(),
        }
    }
}

/// Where the self-host server token came from, so startup can tell the operator.
pub enum TokenSource {
    Env,
    File(PathBuf),
}

/// Reads the `KP_SYNC_*` environment variables. Returns the config and the listen address.
pub fn from_env() -> Result<(Config, String, Option<TokenSource>)> {
    let listen = env::var("KP_SYNC_LISTEN").unwrap_or_else(|_| "0.0.0.0:8686".into());
    let data_dir = PathBuf::from(env::var("KP_SYNC_DATA").unwrap_or_else(|_| "./data".into()));
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("cannot create data directory {}", data_dir.display()))?;

    let mode = env::var("KP_SYNC_MODE").unwrap_or_else(|_| "selfhost".into());
    let (mut config, source) = match mode.as_str() {
        "official" => (Config::official(data_dir), None),
        "selfhost" => {
            let (token, source) = server_token(&data_dir)?;
            let mut config = Config::selfhost(data_dir, token);
            if let Some(mb) = parse_env::<u64>("KP_SYNC_MAX_IMAGE_MB")? {
                config.policy.image_max_bytes = mb.saturating_mul(MIB).saturating_add(64);
            }
            if let Some(n) = parse_env::<u32>("KP_SYNC_MAX_DEVICES")? {
                config.policy.max_devices = n;
            }
            if let Some(h) = parse_env::<u32>("KP_SYNC_TEXT_RETENTION_HOURS")? {
                config.policy.text_retention_hours = Some(h);
            }
            (config, Some(source))
        }
        other => bail!("KP_SYNC_MODE must be `official` or `selfhost`, got `{other}`"),
    };
    config.trust_proxy = env::var("KP_SYNC_TRUST_PROXY").is_ok_and(|v| v == "1");
    Ok((config, listen, source))
}

fn parse_env<T: std::str::FromStr>(name: &str) -> Result<Option<T>> {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => match v.trim().parse() {
            Ok(n) => Ok(Some(n)),
            Err(_) => bail!("{name} must be a non-negative integer, got `{v}`"),
        },
        _ => Ok(None),
    }
}

/// `KP_SYNC_TOKEN`, or `<data>/server-token`, generated on first start.
fn server_token(data_dir: &std::path::Path) -> Result<(String, TokenSource)> {
    if let Ok(token) = env::var("KP_SYNC_TOKEN") {
        if !token.trim().is_empty() {
            return Ok((token.trim().to_owned(), TokenSource::Env));
        }
    }
    let path = data_dir.join("server-token");
    if let Ok(saved) = fs::read_to_string(&path) {
        if !saved.trim().is_empty() {
            return Ok((saved.trim().to_owned(), TokenSource::File(path)));
        }
    }
    let token = crate::random_token();
    fs::write(&path, format!("{token}\n"))
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok((token, TokenSource::File(path)))
}
