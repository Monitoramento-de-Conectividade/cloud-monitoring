import os
import tempfile
import unittest

from backend.cloudv2_persistence import TelemetryPersistence


class PivotTablePayloadTests(unittest.TestCase):
    def test_run_state_payload_exposes_signal_technology_and_timeline_mini(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=5000)
            persistence.start()
            try:
                pivot_id = "PivotTable_1"
                base_ts = 1_700_000_000.0

                run = persistence.get_or_create_active_run(now_ts=base_ts, source="test")
                session = persistence.get_or_create_active_session(
                    pivot_id,
                    pivot_slug="pivottable-1",
                    now_ts=base_ts,
                    source="test",
                    run_id=run["run_id"],
                )
                session_id = session["session_id"]

                persistence.upsert_snapshot(
                    pivot_id,
                    session_id,
                    {
                        "pivot_id": pivot_id,
                        "session_id": session_id,
                        "run_id": run["run_id"],
                        "updated_at_ts": base_ts + 3600,
                        "summary": {
                            "status": {"code": "green", "label": "Online", "rank": 2},
                            "quality": {"code": "green", "label": "Saudavel", "rank": 2},
                            "signal_technology": "17 / LTE",
                            "disconnect_threshold_sec": 600,
                            "attention_window_sec": 3600,
                        },
                    },
                    updated_at_ts=base_ts + 3600,
                )

                for event_ts in (base_ts + 120, base_ts + 1250, base_ts + 3320):
                    persistence.insert_connectivity_event(
                        pivot_id,
                        session_id,
                        {
                            "ts": event_ts,
                            "topic": "cloudv2",
                            "type": "cloudv2",
                            "summary": "evento",
                        },
                    )

                payload = persistence.get_run_state_payload(run_id=run["run_id"])
                self.assertIsNotNone(payload)
                self.assertEqual(len(payload["pivots"]), 1)

                item = payload["pivots"][0]
                self.assertEqual(item["signal"], "17")
                self.assertEqual(item["technology"], "LTE")
                self.assertEqual(item["signal_technology"], "17 / LTE")

                timeline_mini = item.get("timeline_mini")
                self.assertIsInstance(timeline_mini, list)
                self.assertGreater(len(timeline_mini), 0)
                states = {str(segment.get("state")) for segment in timeline_mini}
                self.assertTrue(states.issubset({"online", "offline"}))
                total_ratio = sum(float(segment.get("ratio") or 0) for segment in timeline_mini)
                self.assertGreater(total_ratio, 0.99)
                self.assertLess(total_ratio, 1.01)
            finally:
                persistence.stop()


if __name__ == "__main__":
    unittest.main()
