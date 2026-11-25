//! Open event streams: one per device; a device is online while it has one.

use crate::frames;
use axum::body::Bytes;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard,
    },
};
use tokio::sync::mpsc;

/// Frames queued for a stream that is not reading; past this the stream is dropped
/// and the device catches up from its cursor when it reconnects.
const QUEUE: usize = 1024;

struct Conn {
    id: u64,
    tx: mpsc::Sender<Bytes>,
}

#[derive(Default)]
pub struct Hub {
    groups: Mutex<HashMap<String, HashMap<String, Conn>>>,
    next_id: AtomicU64,
}

impl Hub {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, HashMap<String, Conn>>> {
        self.groups.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Opens a stream for a device, replacing (and so closing) its previous one.
    pub fn connect(&self, group: &str, device: &str) -> (u64, mpsc::Receiver<Bytes>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(QUEUE);
        let mut groups = self.lock();
        let members = groups.entry(group.to_owned()).or_default();
        let was_online = members.insert(device.to_owned(), Conn { id, tx }).is_some();
        if !was_online {
            send_all(members, &devices_frame(), &[device]);
        }
        (id, rx)
    }

    /// Called when stream `id` ends; tells the group if the device went offline.
    pub fn release(&self, group: &str, device: &str, id: u64) {
        let mut groups = self.lock();
        let Some(members) = groups.get_mut(group) else {
            return;
        };
        match members.get(device) {
            Some(conn) if conn.id != id => return,
            Some(_) => {
                members.remove(device);
            }
            None => {}
        }
        send_all(members, &devices_frame(), &[]);
        if members.is_empty() {
            groups.remove(group);
        }
    }

    pub fn is_online(&self, group: &str, device: &str) -> bool {
        self.lock()
            .get(group)
            .is_some_and(|members| members.contains_key(device))
    }

    pub fn group_online(&self, group: &str) -> bool {
        self.lock()
            .get(group)
            .is_some_and(|members| !members.is_empty())
    }

    /// Queues a frame for one device; false if it is offline (or was too far behind).
    pub fn send(&self, group: &str, device: &str, frame: Bytes) -> bool {
        let mut groups = self.lock();
        let Some(members) = groups.get_mut(group) else {
            return false;
        };
        let Some(conn) = members.get(device) else {
            return false;
        };
        if conn.tx.try_send(frame).is_ok() {
            return true;
        }
        members.remove(device);
        false
    }

    /// Queues a frame for every online member except `skip`; returns how many got it.
    pub fn broadcast(&self, group: &str, frame: &Bytes, skip: &[&str]) -> usize {
        self.lock()
            .get_mut(group)
            .map_or(0, |members| send_all(members, frame, skip))
    }

    /// Sends `revoked` and closes the device's stream.
    pub fn revoke(&self, group: &str, device: &str) {
        let mut groups = self.lock();
        if let Some(conn) = groups.get_mut(group).and_then(|m| m.remove(device)) {
            let _ = conn
                .tx
                .try_send(frames::encode(&json!({ "type": "revoked" }), &[]));
        }
    }

    /// Ends every stream so a graceful shutdown does not wait for them.
    pub fn close_all(&self) {
        self.lock().clear();
    }
}

pub fn devices_frame() -> Bytes {
    frames::encode(&json!({ "type": "devices" }), &[])
}

/// Sends to every member not in `skip`; members whose queue is full are dropped.
fn send_all(members: &mut HashMap<String, Conn>, frame: &Bytes, skip: &[&str]) -> usize {
    let mut sent = 0;
    members.retain(|device, conn| {
        if skip.contains(&device.as_str()) {
            return true;
        }
        let ok = conn.tx.try_send(frame.clone()).is_ok();
        sent += usize::from(ok);
        ok
    });
    sent
}
