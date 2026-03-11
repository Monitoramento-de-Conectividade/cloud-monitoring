CREATE TABLE IF NOT EXISTS expected_pivots_pending (
    pivot_id TEXT PRIMARY KEY,
    added_at_ts REAL,
    source TEXT NOT NULL DEFAULT 'ui',
    updated_at_ts REAL NOT NULL
);
