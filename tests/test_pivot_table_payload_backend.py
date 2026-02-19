import os
import tempfile
import unittest

from backend.cloudv2_persistence import TelemetryPersistence


class PivotTablePayloadTests(unittest.TestCase):
    def test_coordinates_are_persisted_and_exposed_in_state_and_panel(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=5000)
            persistence.start()
            try:
                pivot_id = "PivotCoords_1"
                base_ts = 1_700_200_000.0

                run = persistence.get_or_create_active_run(now_ts=base_ts, source="test")
                session = persistence.get_or_create_active_session(
                    pivot_id,
                    pivot_slug="pivotcoords-1",
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
                        "updated_at_ts": base_ts + 1,
                        "summary": {
                            "status": {"code": "green"},
                            "quality": {"code": "green"},
                        },
                    },
                    updated_at_ts=base_ts + 1,
                )

                persistence.set_pivot_coordinates(
                    pivot_id,
                    latitude=-22.254261055861843,
                    longitude=-45.71657037699047,
                    pivot_slug="pivotcoords-1",
                    seen_ts=base_ts,
                )

                state_payload = persistence.get_run_state_payload(run_id=run["run_id"])
                self.assertIsNotNone(state_payload)
                self.assertEqual(len(state_payload["pivots"]), 1)
                state_item = state_payload["pivots"][0]
                self.assertAlmostEqual(float(state_item["latitude"]), -22.254261055861843)
                self.assertAlmostEqual(float(state_item["longitude"]), -45.71657037699047)

                panel_payload = persistence.get_panel_payload(
                    pivot_id,
                    session_id=session_id,
                    run_id=run["run_id"],
                )
                self.assertIsNotNone(panel_payload)
                self.assertAlmostEqual(float(panel_payload["summary"]["latitude"]), -22.254261055861843)
                self.assertAlmostEqual(float(panel_payload["summary"]["longitude"]), -45.71657037699047)
            finally:
                persistence.stop()

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

    def test_run_state_payload_timeline_mini_uses_threshold_fallback_from_settings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=5000)
            persistence.start()
            try:
                pivot_id = "PivotFallback_1"
                base_ts = 1_700_100_000.0

                run = persistence.get_or_create_active_run(now_ts=base_ts, source="test")
                session = persistence.get_or_create_active_session(
                    pivot_id,
                    pivot_slug="pivotfallback-1",
                    now_ts=base_ts,
                    source="test",
                    run_id=run["run_id"],
                )
                session_id = session["session_id"]

                # Snapshot sem disconnect_threshold para forcar fallback.
                persistence.upsert_snapshot(
                    pivot_id,
                    session_id,
                    {
                        "pivot_id": pivot_id,
                        "session_id": session_id,
                        "run_id": run["run_id"],
                        "updated_at_ts": base_ts + 600,
                        "summary": {
                            "status": {"code": "green", "label": "Online", "rank": 2},
                            "quality": {"code": "green", "label": "Saudavel", "rank": 2},
                        },
                    },
                    updated_at_ts=base_ts + 600,
                )

                for event_ts in (base_ts + 590, base_ts + 595, base_ts + 599):
                    persistence.insert_connectivity_event(
                        pivot_id,
                        session_id,
                        {
                            "ts": event_ts,
                            "topic": "cloudv2-ping",
                            "type": "ping",
                            "summary": "evento ping",
                        },
                    )

                payload = persistence.get_run_state_payload(
                    run_id=run["run_id"],
                    connectivity_settings={"ping_expected_sec": 300, "tolerance_factor": 1.5},
                )
                self.assertIsNotNone(payload)
                self.assertEqual(len(payload["pivots"]), 1)
                timeline_mini = payload["pivots"][0].get("timeline_mini") or []
                self.assertGreater(len(timeline_mini), 0)
                # Com fallback adequado, deve existir ao menos um trecho online.
                has_online = any(str(item.get("state")) == "online" and float(item.get("ratio") or 0) > 0 for item in timeline_mini)
                self.assertTrue(has_online)
            finally:
                persistence.stop()


if __name__ == "__main__":
    unittest.main()
