//! Drives a real server over HTTP: pairing, upload, live + catch-up delivery, LWW,
//! official policy, server token and revocation.

use kwikpaste_sync_server::config::{Config, ImageQuota};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};
use tokio::{net::TcpListener, time::timeout};

const A: &str = "aaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbb";
const SERVER_TOKEN: &str = "test-server-token";

struct Server {
    base: String,
    client: reqwest::Client,
    server_token: Option<&'static str>,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start(config: impl FnOnce(PathBuf) -> Config) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path().to_path_buf());
        let server_token = config.server_token.is_some().then_some(SERVER_TOKEN);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(kwikpaste_sync_server::serve(
            config,
            listener,
            std::future::pending(),
        ));
        Self {
            base,
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            server_token,
            _dir: dir,
        }
    }

    fn request(&self, method: Method, path: &str, device: Option<&str>) -> reqwest::RequestBuilder {
        let mut req = self.client.request(method, format!("{}{path}", self.base));
        if let Some(token) = self.server_token {
            req = req.header("X-KwikPaste-Server-Token", token);
        }
        if let Some(token) = device {
            req = req.bearer_auth(token);
        }
        req
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        device: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = self.request(method, path, device);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn create_group(&self, device_id: &str) -> String {
        let (status, body) = self
            .call(
                Method::POST,
                "/v1/groups",
                None,
                Some(json!({ "deviceId": device_id, "info": "info" })),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let token = body["deviceToken"].as_str().unwrap().to_owned();
        assert_eq!(token.len(), 43);
        token
    }

    async fn events(&self, device: &str, query: &str) -> Events {
        let resp = self
            .request(Method::GET, &format!("/v1/events?{query}"), Some(device))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()["content-type"],
            "application/x-kwikpaste-frames"
        );
        Events {
            resp,
            buf: Vec::new(),
        }
    }

    async fn upload(&self, device: &str, frames: &[(Value, Vec<u8>)]) -> Vec<Value> {
        let body: Vec<u8> = frames.iter().flat_map(|(h, b)| frame(h, b)).collect();
        let resp = self
            .request(Method::POST, "/v1/entries", Some(device))
            .header("content-type", "application/x-kwikpaste-frames")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        body["results"].as_array().unwrap().clone()
    }

    /// Runs the join flow with fake SPAKE values and returns the joiner's device token.
    async fn pair(&self, inviter: &str, inviter_events: &mut Events, joiner: &str) -> String {
        let (status, code) = self
            .call(Method::POST, "/v1/pairing", Some(inviter), None)
            .await;
        assert_eq!(status, StatusCode::OK);
        let code = code["code"].as_str().unwrap().to_owned();
        assert_eq!(code.len(), 6);

        let join = self
            .request(Method::POST, "/v1/join", None)
            .json(&json!({ "code": code, "deviceId": joiner, "spake": "spake-a" }))
            .send();
        let join = tokio::spawn(join);

        let (request, _) = inviter_events.next_skipping_devices().await;
        assert_eq!(request["type"], "pairRequest");
        assert_eq!(request["deviceId"], joiner);
        assert_eq!(request["spake"], "spake-a");
        let session = request["session"].as_str().unwrap().to_owned();

        let secret = [7u8; 32];
        let secret_hex: String = secret.iter().map(|b| format!("{b:02x}")).collect();
        let proof = blake3::hash(&secret).to_hex().to_string();
        let reply = json!({ "spake": "spake-b", "sealed": "sealed-box", "joinProof": proof });
        let (status, _) = self
            .call(
                Method::POST,
                &format!("/v1/pairing/{session}/reply"),
                Some(inviter),
                Some(reply),
            )
            .await;
        assert_eq!(status, StatusCode::OK);

        let joined = join.await.unwrap().unwrap();
        assert_eq!(joined.status(), StatusCode::OK);
        let joined: Value = joined.json().await.unwrap();
        assert_eq!(
            joined,
            json!({ "session": session, "spake": "spake-b", "sealed": "sealed-box" })
        );

        let (status, done) = self
            .call(
                Method::POST,
                &format!("/v1/join/{session}/complete"),
                None,
                Some(json!({ "joinSecret": secret_hex, "info": "info-joiner" })),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{done}");
        done["deviceToken"].as_str().unwrap().to_owned()
    }
}

struct Events {
    resp: reqwest::Response,
    buf: Vec<u8>,
}

impl Events {
    /// Next frame, or `None` when the server closed the stream.
    async fn read(&mut self) -> Option<(Value, Vec<u8>)> {
        loop {
            if let Some(frame) = parse_frame(&mut self.buf) {
                return Some(frame);
            }
            let chunk = self.resp.chunk().await.ok()??;
            self.buf.extend_from_slice(&chunk);
        }
    }

    async fn next(&mut self) -> (Value, Vec<u8>) {
        timeout(Duration::from_secs(5), self.read())
            .await
            .expect("no frame within 5 s")
            .expect("stream ended")
    }

    async fn next_skipping_devices(&mut self) -> (Value, Vec<u8>) {
        loop {
            let frame = self.next().await;
            if frame.0["type"] != "devices" {
                return frame;
            }
        }
    }

    async fn assert_quiet(&mut self) {
        let frame = timeout(Duration::from_millis(300), self.read()).await;
        assert!(
            frame.is_err(),
            "unexpected frame {:?}",
            frame.map(|f| f.map(|f| f.0))
        );
    }

    async fn assert_ended(&mut self) {
        let end = timeout(Duration::from_secs(5), async {
            while let Some((header, _)) = self.read().await {
                assert_eq!(header["type"], "devices", "unexpected frame before end");
            }
        });
        end.await.expect("stream did not end");
    }
}

fn frame(header: &Value, body: &[u8]) -> Vec<u8> {
    let header = header.to_string();
    let mut out = (header.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn parse_frame(buf: &mut Vec<u8>) -> Option<(Value, Vec<u8>)> {
    let len = |at: usize| {
        buf.get(at..at + 4)
            .map(|b| u32::from_be_bytes(b.try_into().unwrap()) as usize)
    };
    let h = len(0)?;
    let b = len(4 + h)?;
    let end = 8 + h + b;
    if buf.len() < end {
        return None;
    }
    let header = serde_json::from_slice(&buf[4..4 + h]).unwrap();
    let body = buf[8 + h..end].to_vec();
    buf.drain(..end);
    Some((header, body))
}

fn entry(id: u32, kind: &str, class: &str, clock: u64, device: &str) -> Value {
    json!({
        "entryId": format!("{id:032x}"),
        "kind": kind,
        "class": class,
        "hlc": format!("{clock:016x}-{device}"),
        "live": true,
        "skip": [],
    })
}

#[tokio::test]
async fn selfhost_pairing_sync_and_revoke() {
    let s = Server::start(|dir| Config::selfhost(dir, SERVER_TOKEN.into())).await;

    // The server token guards every request, /v1/info included.
    let resp = s
        .client
        .get(format!("{}/v1/info", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(resp.json::<Value>().await.unwrap()["error"], "serverToken");
    let (status, info) = s.call(Method::GET, "/v1/info", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["mode"], "selfhost");
    assert_eq!(info["protocol"], json!({ "min": 1, "max": 1 }));
    assert_eq!(info["policy"]["images"], "stored");
    assert_eq!(info["policy"]["textRetentionHours"], Value::Null);
    assert_eq!(info["policy"]["imageMaxBytes"], 100 * 1024 * 1024 + 64);

    let (status, err) = s
        .call(
            Method::POST,
            "/v1/groups",
            None,
            Some(json!({ "deviceId": "short", "info": "x" })),
        )
        .await;
    assert_eq!(
        (status, err["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("badRequest"))
    );

    let ta = s.create_group(A).await;
    let mut ea = s.events(&ta, "after=0").await;
    assert_eq!(ea.next().await.0, json!({ "type": "synced", "seq": 0 }));

    let tb = s.pair(&ta, &mut ea, B).await;
    assert_eq!(ea.next().await.0["type"], "devices");
    let mut eb = s.events(&tb, "after=0").await;
    assert_eq!(eb.next().await.0, json!({ "type": "synced", "seq": 0 }));
    assert_eq!(ea.next().await.0["type"], "devices", "B came online");

    // A text entry reaches B live.
    let results = s
        .upload(
            &ta,
            &[(entry(1, "item", "text", 5, A), b"text-v1".to_vec())],
        )
        .await;
    assert_eq!(
        results,
        vec![json!({ "entryId": format!("{:032x}", 1), "status": "stored", "seq": 1 })]
    );
    let (header, body) = eb.next().await;
    assert_eq!(
        header,
        json!({
            "type": "entry", "seq": 1, "entryId": format!("{:032x}", 1), "kind": "item",
            "class": "text", "hlc": format!("{:016x}-{A}", 5), "from": A, "live": true,
        })
    );
    assert_eq!(body, b"text-v1");

    // Last writer wins: older or equal HLCs are stale; a newer one replaces the entry.
    let results = s
        .upload(
            &ta,
            &[
                (entry(1, "item", "text", 4, A), b"older".to_vec()),
                (entry(1, "item", "text", 5, A), b"same".to_vec()),
            ],
        )
        .await;
    assert_eq!(results[0]["status"], "stale");
    assert_eq!(results[1]["status"], "stale");
    let results = s
        .upload(
            &tb,
            &[(entry(1, "item", "text", 5, B), b"text-v2".to_vec())],
        )
        .await;
    assert_eq!(results[0]["seq"], 2, "same clock, larger device id wins");
    let (header, body) = ea.next().await;
    assert_eq!(
        (header["seq"].as_i64(), header["from"].as_str()),
        (Some(2), Some(B))
    );
    assert_eq!(body, b"text-v2");

    // A large image is stored as a blob; a skipped device does not get the settings entry live.
    let image = vec![42u8; 100 * 1024];
    let mut settings = entry(3, "settings", "none", 9, A);
    settings["skip"] = json!([B]);
    let results = s
        .upload(
            &ta,
            &[
                (entry(2, "item", "image", 8, A), image.clone()),
                (settings, b"theme".to_vec()),
            ],
        )
        .await;
    assert_eq!(
        (results[0]["seq"].as_i64(), results[1]["seq"].as_i64()),
        (Some(3), Some(4))
    );
    let (header, body) = eb.next().await;
    assert_eq!(
        (header["seq"].as_i64(), header["class"].as_str()),
        (Some(3), Some("image"))
    );
    assert_eq!(body, image);
    eb.assert_quiet().await;

    // Catch-up: latest version per entry, in seq order; the new stream replaces the old one.
    let mut eb2 = s.events(&tb, "after=0").await;
    eb.assert_ended().await;
    let mut seen = Vec::new();
    loop {
        let (header, body) = eb2.next().await;
        if header["type"] == "synced" {
            assert_eq!(header["seq"], 4);
            break;
        }
        assert_eq!(header["live"], false);
        seen.push((header["seq"].as_i64().unwrap(), body));
    }
    assert_eq!(
        seen,
        vec![
            (2, b"text-v2".to_vec()),
            (3, image.clone()),
            (4, b"theme".to_vec())
        ]
    );

    // items=none sends settings only; an `after` beyond the server's seq counts as 0.
    let mut eb3 = s.events(&tb, "after=0&items=none").await;
    assert_eq!(eb3.next().await.0["seq"], 4);
    assert_eq!(eb3.next().await.0, json!({ "type": "synced", "seq": 4 }));
    let mut eb4 = s.events(&tb, "after=999").await;
    assert_eq!(eb4.next().await.0["seq"], 2);

    let (status, devices) = s.call(Method::GET, "/v1/devices", Some(&ta), None).await;
    assert_eq!(status, StatusCode::OK);
    let devices = devices.as_array().unwrap();
    assert_eq!(devices.len(), 2);
    assert!(devices
        .iter()
        .all(|d| d["online"] == true && d["lastSeenAt"].is_string()));
    assert_eq!(devices[1]["info"], "info-joiner");

    // Revoke B: its stream gets `revoked` and closes, its token stops working.
    let (status, _) = s
        .call(Method::DELETE, &format!("/v1/devices/{B}"), Some(&ta), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    loop {
        let (header, _) = eb4.next().await;
        if header["type"] == "revoked" {
            break;
        }
    }
    eb4.assert_ended().await;
    let (status, err) = s.call(Method::GET, "/v1/devices", Some(&tb), None).await;
    assert_eq!(
        (status, err["error"].as_str()),
        (StatusCode::UNAUTHORIZED, Some("unauthorized"))
    );

    // The last device leaving deletes the group.
    let (status, _) = s
        .call(Method::DELETE, &format!("/v1/devices/{A}"), Some(&ta), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.call(Method::GET, "/v1/devices", Some(&ta), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn official_policy() {
    let s = Server::start(|dir| {
        let mut config = Config::official(dir);
        // Expire text at once and shrink the group image quota so both are observable.
        config.policy.text_retention_hours = Some(0);
        config.limits.image_quota = Some(ImageQuota {
            per_group: 3000,
            global: 1 << 30,
        });
        config
    })
    .await;

    let (status, info) = s.call(Method::GET, "/v1/info", None, None).await;
    assert_eq!(status, StatusCode::OK, "no server token needed");
    assert_eq!(info["mode"], "official");
    assert_eq!(info["policy"]["images"], "live");
    assert_eq!(
        info["policy"]["imageCompress"],
        json!({ "maxSide": 1920, "quality": 70 })
    );
    assert_eq!(
        (
            info["policy"]["meta"].clone(),
            info["policy"]["maxDevices"].clone()
        ),
        (json!(false), json!(10))
    );

    let ta = s.create_group(A).await;

    // Wrong code, then a right code while the inviter has no event stream.
    let join = |code: &str| json!({ "code": code, "deviceId": B, "spake": "x" });
    let (status, err) = s
        .call(Method::POST, "/v1/join", None, Some(join("000000")))
        .await;
    assert_eq!(
        (status, err["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("codeInvalid"))
    );
    let (_, code) = s.call(Method::POST, "/v1/pairing", Some(&ta), None).await;
    let (status, err) = s
        .call(
            Method::POST,
            "/v1/join",
            None,
            Some(join(code["code"].as_str().unwrap())),
        )
        .await;
    assert_eq!(
        (status, err["error"].as_str()),
        (StatusCode::CONFLICT, Some("inviterOffline"))
    );

    let mut ea = s.events(&ta, "after=0").await;
    ea.next().await;
    let tb = s.pair(&ta, &mut ea, B).await;
    let mut eb = s.events(&tb, "after=0").await;
    assert_eq!(eb.next().await.0["type"], "synced");

    // meta is not allowed; oversize images are rejected.
    let results = s
        .upload(
            &ta,
            &[
                (entry(1, "meta", "none", 1, A), b"fav".to_vec()),
                (
                    entry(2, "item", "image", 1, A),
                    vec![0; 2 * 1024 * 1024 + 1],
                ),
            ],
        )
        .await;
    assert_eq!(
        (results[0]["status"].as_str(), results[0]["error"].as_str()),
        (Some("rejected"), Some("policy"))
    );
    assert_eq!(results[1]["error"], "tooLarge");

    // Images are forwarded live without a seq, until the group's daily quota runs out.
    let results = s
        .upload(&ta, &[(entry(3, "item", "image", 2, A), vec![1; 2000])])
        .await;
    assert_eq!(
        results,
        vec![json!({ "entryId": format!("{:032x}", 3), "status": "forwarded" })]
    );
    let (header, body) = eb.next().await;
    assert_eq!(
        (header["seq"].clone(), header["live"].clone()),
        (Value::Null, json!(true))
    );
    assert_eq!(body, vec![1; 2000]);
    let results = s
        .upload(&ta, &[(entry(4, "item", "image", 3, A), vec![2; 2000])])
        .await;
    assert_eq!(results[0]["error"], "quota");
    let retry_at =
        chrono::DateTime::parse_from_rfc3339(results[0]["retryAt"].as_str().unwrap()).unwrap();
    assert!(retry_at > chrono::Utc::now());

    // Text and settings are stored and forwarded; on catch-up the expired text and the
    // unstored image are gone, settings stay.
    let results = s
        .upload(
            &ta,
            &[
                (entry(5, "item", "text", 4, A), b"hi".to_vec()),
                (entry(6, "settings", "none", 5, A), b"s".to_vec()),
            ],
        )
        .await;
    assert_eq!(
        (results[0]["seq"].as_i64(), results[1]["seq"].as_i64()),
        (Some(1), Some(2))
    );
    assert_eq!(eb.next().await.0["seq"], 1);
    assert_eq!(eb.next().await.0["seq"], 2);

    let mut eb2 = s.events(&tb, "after=0").await;
    let (header, body) = eb2.next().await;
    assert_eq!(
        (header["seq"].as_i64(), header["kind"].as_str()),
        (Some(2), Some("settings"))
    );
    assert_eq!(body, b"s");
    assert_eq!(eb2.next().await.0, json!({ "type": "synced", "seq": 2 }));
}

#[tokio::test]
async fn shutdown_closes_open_streams() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(kwikpaste_sync_server::serve(
        Config::official(dir.path().to_path_buf()),
        listener,
        async move {
            let _ = stopped.await;
        },
    ));
    let s = Server {
        base,
        client: reqwest::Client::builder().no_proxy().build().unwrap(),
        server_token: None,
        _dir: dir,
    };
    let token = s.create_group(A).await;
    let mut events = s.events(&token, "after=0").await;
    events.next().await;

    stop.send(()).unwrap();
    events.assert_ended().await;
    let result = timeout(Duration::from_secs(5), server).await;
    assert!(matches!(result, Ok(Ok(Ok(())))), "server did not stop");
}
