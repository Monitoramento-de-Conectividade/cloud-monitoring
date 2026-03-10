import os
import tempfile
import unittest
from unittest.mock import patch

from backend.cloudv2_dashboard import _normalize_bulk_pivot_ids
import backend.cloudv2_telemetry as telemetry_mod
from backend.cloudv2_telemetry import TelemetryStore


class BulkPivotActionHelpersTests(unittest.TestCase):
    def test_normalize_bulk_pivot_ids_strips_and_dedupes(self):
        self.assertEqual(
            _normalize_bulk_pivot_ids([" PivotA ", "", "PivotB", "PivotA", None]),
            ["PivotA", "PivotB"],
        )

    def test_normalize_bulk_pivot_ids_requires_non_empty_list(self):
        with self.assertRaisesRegex(ValueError, "pivot_ids obrigatorio"):
            _normalize_bulk_pivot_ids([])

    def test_normalize_bulk_pivot_ids_enforces_limit(self):
        with self.assertRaisesRegex(ValueError, "maximo de 2 pivot_ids por requisicao"):
            _normalize_bulk_pivot_ids(["PivotA", "PivotB", "PivotC"], limit=2)


class ExpectedPivotDiscoveryTests(unittest.TestCase):
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

    def test_cloudv2_descarta_pivot_nao_autorizado(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            try:
                result = store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_700_000_000.0)

                self.assertFalse(result["accepted"])
                self.assertEqual(result["reason"], "pivot nao autorizado")
                snapshot = store.get_state_snapshot()
                self.assertEqual(snapshot["pivots"], [])
                self.assertEqual(snapshot["expected_pivots_pending"], [])
            finally:
                store.stop()

    def test_pivot_autorizado_entra_quando_cloudv2_chega_e_sai_da_fila(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            try:
                queued = store.queue_expected_pivots(["PivotA_1"], now=1_700_000_000.0, source="test")
                self.assertEqual(queued["added_count"], 1)
                self.assertEqual(len(queued["pending"]), 1)

                accepted = store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_700_000_010.0)
                self.assertTrue(accepted["accepted"])
                self.assertEqual(accepted["pivot_id"], "PivotA_1")

                snapshot = store.get_state_snapshot(now=1_700_000_020.0)
                self.assertEqual(snapshot["expected_pivots_pending"], [])
                self.assertEqual([item["pivot_id"] for item in snapshot["pivots"]], ["PivotA_1"])
            finally:
                store.stop()

    def test_queue_expected_pivots_avisa_quando_pivot_ja_existe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_700_000_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_700_000_010.0)

                result = store.queue_expected_pivots(["PivotA_1"], now=1_700_000_020.0, source="test")
                self.assertEqual(result["added_count"], 0)
                self.assertEqual(result["results"][0]["status"], "already_exists")
            finally:
                store.stop()


if __name__ == "__main__":
    unittest.main()
