-- The schema from 1.0 to 1.2, created before migrations existed; IF NOT EXISTS lets
-- databases made by those versions adopt it unchanged.
CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS devices (
    group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    token_hash BLOB NOT NULL UNIQUE,
    info TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (group_id, device_id)
);
CREATE TABLE IF NOT EXISTS entries (
    group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    entry_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    class TEXT NOT NULL,
    hlc TEXT NOT NULL,
    from_device TEXT NOT NULL,
    size INTEGER NOT NULL,
    body BLOB,
    blob TEXT,
    expires_at TEXT,
    PRIMARY KEY (group_id, entry_id)
);
CREATE INDEX IF NOT EXISTS entries_by_size ON entries (group_id, size);
CREATE INDEX IF NOT EXISTS entries_by_seq ON entries (group_id, seq);
CREATE INDEX IF NOT EXISTS entries_by_expiry ON entries (expires_at) WHERE expires_at IS NOT NULL;
