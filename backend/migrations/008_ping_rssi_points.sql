CREATE TABLE IF NOT EXISTS ping_rssi_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pivot_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    ts REAL NOT NULL,
    rssi INTEGER NOT NULL CHECK (rssi >= 0 AND rssi <= 31),
    created_at_ts REAL NOT NULL,
    FOREIGN KEY (pivot_id) REFERENCES pivots(pivot_id) ON DELETE CASCADE,
    FOREIGN KEY (session_id) REFERENCES monitoring_sessions(session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ping_rssi_points_pivot_session_ts_id_asc
    ON ping_rssi_points (pivot_id, session_id, ts ASC, id ASC);
