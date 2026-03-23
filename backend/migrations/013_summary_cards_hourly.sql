CREATE TABLE IF NOT EXISTS summary_cards_hourly (
    bucket_ts INTEGER PRIMARY KEY,
    total_count INTEGER NOT NULL,
    connected_count INTEGER NOT NULL,
    disconnected_count INTEGER NOT NULL,
    initial_count INTEGER NOT NULL,
    quality_green_count INTEGER NOT NULL,
    quality_calculating_count INTEGER NOT NULL,
    quality_yellow_count INTEGER NOT NULL,
    quality_critical_count INTEGER NOT NULL,
    created_at_ts REAL NOT NULL,
    updated_at_ts REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_summary_cards_hourly_bucket_desc
    ON summary_cards_hourly (bucket_ts DESC);
