-- Operator statistics (README「统计接口」). Metrics whose name ends in `.max` are peaks;
-- every other metric is a count or a byte sum.

-- Per UTC minute (`YYYY-MM-DDTHH:MMZ`), kept 8 days.
CREATE TABLE stats_minutes (
    minute TEXT NOT NULL,
    metric TEXT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (minute, metric)
) WITHOUT ROWID;

-- Per Asia/Shanghai day (`YYYY-MM-DD`), rolled up from the minutes, kept for good.
CREATE TABLE stats_days (
    day TEXT NOT NULL,
    metric TEXT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (day, metric)
) WITHOUT ROWID;

-- Devices active per Asia/Shanghai day with the client they reported, kept 32 days.
CREATE TABLE stats_device_days (
    day TEXT NOT NULL,
    group_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    version TEXT,
    platform TEXT,
    PRIMARY KEY (day, group_id, device_id)
) WITHOUT ROWID;
CREATE INDEX stats_device_days_by_device ON stats_device_days (group_id, device_id, day);
