//! Pairing codes and join sessions (PROTOCOL.md §4.3–4.4). The server only relays the
//! SPAKE2 messages; it checks `joinProof` so only the joiner that opened `sealed` gets a token.

use crate::error::ApiError;
use chrono::{DateTime, Utc};
use rand::Rng;
use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const CODE_TTL: Duration = Duration::from_secs(5 * 60);
const CODE_ATTEMPTS: u8 = 5;
/// From the join request until `complete` must have arrived.
const SESSION_TTL: Duration = Duration::from_secs(5 * 60);

struct Code {
    code: String,
    inviter: String,
    expires: Instant,
    attempts_left: u8,
}

struct Session {
    group: String,
    inviter: String,
    joiner: String,
    created: Instant,
    waiter: Option<oneshot::Sender<Reply>>,
    join_proof: Option<blake3::Hash>,
}

pub struct Reply {
    pub spake: String,
    pub sealed: String,
}

pub struct Join {
    pub session: String,
    pub group: String,
    pub inviter: String,
    pub reply: oneshot::Receiver<Reply>,
}

#[derive(Default)]
pub struct Pairing {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// One active code per group.
    codes: HashMap<String, Code>,
    sessions: HashMap<String, Session>,
}

impl Pairing {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Issues a new 6-digit code for the group, replacing the previous one.
    pub fn create_code(&self, group: &str, inviter: &str) -> (String, DateTime<Utc>) {
        let mut inner = self.lock();
        let now = Instant::now();
        let code = loop {
            let code = format!("{:06}", rand::thread_rng().gen_range(0..1_000_000));
            let taken = inner
                .codes
                .iter()
                .any(|(g, c)| g != group && c.code == code && c.expires > now);
            if !taken {
                break code;
            }
        };
        inner.codes.insert(
            group.to_owned(),
            Code {
                code: code.clone(),
                inviter: inviter.to_owned(),
                expires: now + CODE_TTL,
                attempts_left: CODE_ATTEMPTS,
            },
        );
        let expires_at = Utc::now() + CODE_TTL;
        (code, expires_at)
    }

    pub fn cancel_code(&self, group: &str) {
        self.lock().codes.remove(group);
    }

    /// Spends one attempt of `code` and opens a session waiting for the inviter's reply.
    pub fn start_join(&self, code: &str, joiner: &str) -> Result<Join, ApiError> {
        let mut inner = self.lock();
        let now = Instant::now();
        let (group, entry) = inner
            .codes
            .iter_mut()
            .find(|(_, c)| c.code == code && c.expires > now && c.attempts_left > 0)
            .ok_or(ApiError::CodeInvalid)?;
        entry.attempts_left -= 1;
        let (group, inviter) = (group.clone(), entry.inviter.clone());

        let session = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        inner.sessions.insert(
            session.clone(),
            Session {
                group: group.clone(),
                inviter: inviter.clone(),
                joiner: joiner.to_owned(),
                created: now,
                waiter: Some(tx),
                join_proof: None,
            },
        );
        Ok(Join {
            session,
            group,
            inviter,
            reply: rx,
        })
    }

    /// The inviter's answer; only the device that issued the code may reply, once.
    pub fn reply(
        &self,
        session: &str,
        group: &str,
        device: &str,
        reply: Reply,
        join_proof: blake3::Hash,
    ) -> Result<(), ApiError> {
        let mut inner = self.lock();
        let s = inner
            .sessions
            .get_mut(session)
            .filter(|s| s.group == group && s.inviter == device && s.waiter.is_some())
            .ok_or(ApiError::NotFound)?;
        let delivered = s.waiter.take().is_some_and(|tx| tx.send(reply).is_ok());
        if !delivered {
            inner.sessions.remove(session);
            return Err(ApiError::NotFound);
        }
        s.join_proof = Some(join_proof);
        Ok(())
    }

    /// Checks `BLAKE3(joinSecret) == joinProof` and closes the session. Returns (group, joiner).
    pub fn finish(
        &self,
        session: &str,
        join_secret: &[u8; 32],
    ) -> Result<(String, String), ApiError> {
        let s = self
            .lock()
            .sessions
            .remove(session)
            .ok_or(ApiError::NotFound)?;
        let proof = s
            .join_proof
            .filter(|_| s.created.elapsed() <= SESSION_TTL)
            .ok_or(ApiError::NotFound)?;
        // `blake3::Hash` equality is constant-time.
        if blake3::hash(join_secret) != proof {
            return Err(ApiError::CodeInvalid);
        }
        Ok((s.group, s.joiner))
    }

    pub fn drop_session(&self, session: &str) {
        self.lock().sessions.remove(session);
    }

    pub fn forget_group(&self, group: &str) {
        let mut inner = self.lock();
        inner.codes.remove(group);
        inner.sessions.retain(|_, s| s.group != group);
    }

    pub fn purge(&self) {
        let mut inner = self.lock();
        let now = Instant::now();
        inner.codes.retain(|_, c| c.expires > now);
        inner
            .sessions
            .retain(|_, s| s.created.elapsed() <= SESSION_TTL);
    }

    /// Wakes every waiting join so a graceful shutdown does not wait for them.
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.codes.clear();
        inner.sessions.clear();
    }
}
