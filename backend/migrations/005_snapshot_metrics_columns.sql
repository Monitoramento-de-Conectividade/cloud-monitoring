ALTER TABLE pivot_snapshots ADD COLUMN median_ready INTEGER;
ALTER TABLE pivot_snapshots ADD COLUMN median_sample_count INTEGER;
ALTER TABLE pivot_snapshots ADD COLUMN median_cloudv2_interval_sec REAL;
ALTER TABLE pivot_snapshots ADD COLUMN disconnect_threshold_sec REAL;
