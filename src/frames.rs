//! Frame format (PROTOCOL.md §3): u32 BE header length, JSON header, u32 BE body length, body.

use crate::error::ApiError;
use axum::body::Bytes;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt};

pub const CONTENT_TYPE: &str = "application/x-kwikpaste-frames";
const MAX_HEADER_BYTES: usize = 64 * 1024;

pub fn encode(header: &Value, body: &[u8]) -> Bytes {
    let header = header.to_string();
    let mut out = Vec::with_capacity(8 + header.len() + body.len());
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    Bytes::from(out)
}

/// Reads the next frame's header and body length; `None` at a clean end of input.
pub async fn read_head<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<Option<(Vec<u8>, u64)>, ApiError> {
    let mut len = [0u8; 4];
    if r.read(&mut len[..1]).await.map_err(truncated)? == 0 {
        return Ok(None);
    }
    r.read_exact(&mut len[1..]).await.map_err(truncated)?;
    let header_len = u32::from_be_bytes(len) as usize;
    if header_len > MAX_HEADER_BYTES {
        return Err(ApiError::bad("frame header exceeds 64 KiB"));
    }
    let mut header = vec![0; header_len];
    r.read_exact(&mut header).await.map_err(truncated)?;
    let body_len = r.read_u32().await.map_err(truncated)?;
    Ok(Some((header, u64::from(body_len))))
}

fn truncated(_: std::io::Error) -> ApiError {
    ApiError::bad("truncated frame")
}

/// Parses `"<16 hex>-<device id>"` into a comparable key.
pub fn parse_hlc(hlc: &str) -> Option<(u64, &str)> {
    let (clock, device) = hlc.split_once('-')?;
    if clock.len() != 16 || !clock.bytes().all(|b| b.is_ascii_hexdigit()) || device.is_empty() {
        return None;
    }
    Some((u64::from_str_radix(clock, 16).ok()?, device))
}

/// Whether `new` wins over `old` under last-writer-wins: clock first, then device id.
pub fn hlc_newer(new: &str, old: &str) -> bool {
    match (parse_hlc(new), parse_hlc(old)) {
        (Some(new), Some(old)) => new > old,
        (Some(_), None) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_order() {
        assert!(hlc_newer("0000000000000002-a", "0000000000000001-z"));
        assert!(hlc_newer("0000000000000001-b", "0000000000000001-a"));
        assert!(!hlc_newer("0000000000000001-a", "0000000000000001-a"));
        assert!(!hlc_newer("000000000000000a-a", "00000000000000A0-a"));
        assert!(parse_hlc("+000000000000001-a").is_none());
        assert!(parse_hlc("0000000000000001-").is_none());
    }

    #[tokio::test]
    async fn frame_round_trip() {
        let frame = encode(&serde_json::json!({"type": "ping"}), b"xyz");
        let mut input = &frame[..];
        let (header, body_len) = read_head(&mut input).await.ok().flatten().unwrap();
        assert_eq!(header, br#"{"type":"ping"}"#);
        assert_eq!(body_len, 3);
        let mut empty: &[u8] = &[];
        assert!(read_head(&mut empty).await.unwrap().is_none());
        let mut short = &frame[..3];
        assert!(read_head(&mut short).await.is_err());
    }
}
