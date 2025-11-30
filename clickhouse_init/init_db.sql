CREATE DATABASE IF NOT EXISTS events_db;

CREATE TABLE IF NOT EXISTS events_db.events (
    event_id UUID,
    event_type LowCardinality(String),
    user_id Nullable(String),
    timestamp DateTime64(3),
    payload String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_type, timestamp);

