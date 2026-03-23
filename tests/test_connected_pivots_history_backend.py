import os
import tempfile
import unittest
from unittest.mock import patch

import backend.cloudv2_telemetry as telemetry_mod
from backend.cloudv2_persistence import TelemetryPersistence
from backend.cloudv2_telemetry import TelemetryStore


class ConnectedPivotsHistoryPersistenceTests(unittest.TestCase):
    def test_hourly_history_keeps_latest_30_days(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path)
            persistence.start()
            try:
                base_ts = 1_700_000_000
                persistence.record_connected_pivots_hourly(base_ts, connected_count=10, total_count=20)
                persistence.record_connected_pivots_hourly(
                    base_ts + (31 * 24 * 3600),
                    connected_count=12,
                    total_count=20,
                )

                points = persistence.fetch_connected_pivots_hourly_history(
                    now=base_ts + (31 * 24 * 3600),
                )

                self.assertEqual(len(points), 1)
                self.assertEqual(points[0]["connected_count"], 12)
                self.assertEqual(points[0]["total_count"], 20)
            finally:
                persistence.stop()


class ConnectedPivotsHistoryMockSnapshotTests(unittest.TestCase):
    def _build_store(self, temp_dir):
        db_path = os.path.join(temp_dir, "telemetry.sqlite3")
        config = {
            "enable_background_worker": False,
            "require_apply_to_start": False,
            "continuous_monitoring_mode": True,
            "history_mode": "merge",
            "sqlite_db_path": db_path,
            "api_state_cache_ttl_sec": 0,
            "api_quality_cache_ttl_sec": 0,
        }
        ensure_dirs = lambda: os.makedirs(temp_dir, exist_ok=True)
        data_dir_patch = patch.object(telemetry_mod, "DATA_DIR", temp_dir)
        ensure_dirs_patch = patch.object(telemetry_mod, "ensure_dirs", ensure_dirs)
        data_dir_patch.start()
        ensure_dirs_patch.start()
        self.addCleanup(data_dir_patch.stop)
        self.addCleanup(ensure_dirs_patch.stop)
        store = TelemetryStore(config=config, log_dir=temp_dir)
        store.start()
        return store

    def test_dev_mode_returns_mock_series_when_history_is_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {"CLOUDV2_DEV_HOT_RELOAD": "1"}):
            store = self._build_store(temp_dir)
            try:
                payload = store.get_connected_pivots_history_snapshot(now=1_700_000_000.0)
                self.assertEqual(payload["source"], "mock")
                self.assertEqual(len(payload["points"]), 720)
                self.assertGreaterEqual(payload["latest"]["total_count"], payload["latest"]["connected_count"])
            finally:
                store.stop()
