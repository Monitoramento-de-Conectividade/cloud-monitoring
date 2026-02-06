import os
import re
import time
import threading
from datetime import datetime

from cloudv2_dashboard import DATA_DIR, ensure_dirs, slugify, write_json_atomic


DEFAULT_MAX_EVENTS = 200
DEFAULT_GRACE_SEC = 30


def _ts_to_str(ts):
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _format_ago(seconds):
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{int(seconds)}s atras"
    if seconds < 3600:
        return f"{int(seconds / 60)}m atras"
    return f"{seconds / 3600:.1f}h atras"


class TelemetryStore:
    def __init__(self, config, pivot_ids, log_dir):
        self.pivot_ids = list(pivot_ids)
        self.pivot_slugs = {pivot_id: slugify(pivot_id) for pivot_id in self.pivot_ids}
        self.log_dir = log_dir
        self.cmd_topics = self._normalize_cmd_topics(config)
        self.ping_topic = config.get("ping_topic", "cloudv2-ping")
        self.cloud2_topic = "cloud2"
        self.expected_ping_sec = int(config.get("ping_interval_minutes", 3)) * 60
        self.refresh_sec = int(config.get("dashboard_refresh_sec", 5))
        self.history_mode = str(config.get("history_mode", "merge")).strip().lower()

        self.lock = threading.Lock()
        self.state = {}
        self._dirty = True
        self._stop = False
        self._writer = threading.Thread(target=self._writer_loop, daemon=True)

        for pivot_id in self.pivot_ids:
            self.state[pivot_id] = self._new_pivot_state(pivot_id)

    def start(self):
        ensure_dirs()
        if self.history_mode == "merge":
            self._load_existing_logs()
        else:
            self._dirty = True
            self.write()
        self._writer.start()

    def stop(self):
        self._stop = True

    def _normalize_cmd_topics(self, config):
        configured = config.get("cmd_topics")
        if isinstance(configured, list):
            topics = [str(item).strip() for item in configured if str(item).strip()]
            if topics:
                return topics
        legacy = str(config.get("cmd_topic", "")).strip()
        if legacy:
            return [legacy]
        return []

    def detect_pivots(self, payload):
        matches = []
        for pivot_id in self.pivot_ids:
            if pivot_id in payload:
                matches.append(pivot_id)
        return matches

    def record_message(self, topic, payload, ts=None, mark_dirty=True):
        ts = ts or time.time()
        matches = self.detect_pivots(payload)
        if not matches:
            return

        with self.lock:
            for pivot_id in matches:
                pivot = self._get_pivot(pivot_id)
                pivot["total_count"] += 1
                pivot["last_seen_ts"] = ts
                self._update_topic(pivot, topic, ts)
                if topic == self.cloud2_topic:
                    pivot["cloud2_count"] += 1
                if topic == self.ping_topic:
                    self._handle_ping(pivot, ts)
            if mark_dirty:
                self._dirty = True

    def record_ping_sent(self, pivot_id, ts=None, mark_dirty=True):
        ts = ts or time.time()
        with self.lock:
            pivot = self._get_pivot(pivot_id)
            pivot["sent_count"] += 1
            pivot["last_sent_ts"] = ts
            if mark_dirty:
                self._dirty = True

    def record_ping_result(self, pivot_id, ok, ts=None, source="info", mark_dirty=True):
        ts = ts or time.time()
        with self.lock:
            pivot = self._get_pivot(pivot_id)
            if ok:
                pivot["responses"]["success"] += 1
            else:
                pivot["responses"]["fail"] += 1

            event = {"ts": ts, "ok": bool(ok), "source": source}
            pivot["responses"]["events"].append(event)
            pivot["responses"]["events"] = pivot["responses"]["events"][-DEFAULT_MAX_EVENTS:]
            pivot["responses"]["last"] = event

            if mark_dirty:
                self._dirty = True

    def _new_pivot_state(self, pivot_id):
        return {
            "pivot_id": pivot_id,
            "total_count": 0,
            "cloud2_count": 0,
            "sent_count": 0,
            "last_sent_ts": None,
            "last_seen_ts": None,
            "topics": {},
            "responses": {"success": 0, "fail": 0, "events": [], "last": None},
            "ping": {
                "expected_interval_sec": self.expected_ping_sec,
                "last_ping_ts": None,
                "missing_events": [],
                "missing_count": 0,
                "missing_total_sec": 0,
            },
        }

    def _get_pivot(self, pivot_id):
        if pivot_id not in self.state:
            self.state[pivot_id] = self._new_pivot_state(pivot_id)
            self.pivot_slugs[pivot_id] = slugify(pivot_id)
        return self.state[pivot_id]

    def _update_topic(self, pivot, topic, ts):
        info = pivot["topics"].get(topic)
        if not info:
            info = {"count": 0, "last_ts": None, "last_at": "-"}
            pivot["topics"][topic] = info
        info["count"] += 1
        info["last_ts"] = ts
        info["last_at"] = _ts_to_str(ts)

    def _handle_ping(self, pivot, ts):
        last_ping = pivot["ping"]["last_ping_ts"]
        if last_ping:
            gap = ts - last_ping
            if gap > self.expected_ping_sec + DEFAULT_GRACE_SEC:
                missing_start = last_ping + self.expected_ping_sec
                event = {
                    "start_ts": missing_start,
                    "end_ts": ts,
                    "duration_sec": max(0, ts - missing_start),
                    "start_at": _ts_to_str(missing_start),
                    "end_at": _ts_to_str(ts),
                }
                pivot["ping"]["missing_events"].append(event)
                pivot["ping"]["missing_events"] = pivot["ping"]["missing_events"][-DEFAULT_MAX_EVENTS:]
                pivot["ping"]["missing_count"] += 1
                pivot["ping"]["missing_total_sec"] += event["duration_sec"]
        pivot["ping"]["last_ping_ts"] = ts

    def _writer_loop(self):
        while not self._stop:
            time.sleep(self.refresh_sec)
            if self._dirty:
                self.write()

    def write(self):
        with self.lock:
            now = time.time()
            exports = {pivot_id: self._export_pivot(pivot, now) for pivot_id, pivot in self.state.items()}
            self._dirty = False

        for pivot_id, data in exports.items():
            slug = self.pivot_slugs.get(pivot_id, slugify(pivot_id))
            write_json_atomic(os.path.join(DATA_DIR, f"pivot_{slug}.json"), data)

        write_json_atomic(os.path.join(DATA_DIR, "state.json"), {"updated_at": _ts_to_str(time.time())})

    def _export_pivot(self, pivot, now):
        last_seen_ts = pivot["last_seen_ts"]
        last_ping_ts = pivot["ping"]["last_ping_ts"]
        last_response = pivot["responses"]["last"]
        responses_ok = pivot["responses"]["success"]
        responses_fail = pivot["responses"]["fail"]
        responses_total = responses_ok + responses_fail
        responses_rate = int(round((responses_ok / responses_total) * 100)) if responses_total else 0

        overdue = False
        if last_ping_ts:
            overdue = (now - last_ping_ts) > (self.expected_ping_sec + DEFAULT_GRACE_SEC)

        summary = {
            "total_count": pivot["total_count"],
            "cloud2_count": pivot["cloud2_count"],
            "sent_count": pivot["sent_count"],
            "last_seen_ts": last_seen_ts,
            "last_seen_at": _ts_to_str(last_seen_ts),
            "last_seen_ago": _format_ago(now - last_seen_ts) if last_seen_ts else "-",
            "last_response_ok": last_response["ok"] if last_response else None,
            "last_response_at": _ts_to_str(last_response["ts"]) if last_response else "-",
        }

        ping = dict(pivot["ping"])
        ping["last_ping_at"] = _ts_to_str(last_ping_ts)
        ping["overdue"] = overdue

        responses = {
            "success": responses_ok,
            "fail": responses_fail,
            "rate": responses_rate,
            "events": list(pivot["responses"]["events"]),
            "last": last_response,
        }

        return {
            "pivot_id": pivot["pivot_id"],
            "pivot_slug": self.pivot_slugs.get(pivot["pivot_id"]),
            "updated_at": _ts_to_str(now),
            "summary": summary,
            "topics": pivot["topics"],
            "ping": ping,
            "responses": responses,
        }

    def _load_existing_logs(self):
        if not os.path.isdir(self.log_dir):
            return

        events = []
        for name in os.listdir(self.log_dir):
            path = os.path.join(self.log_dir, name)
            if not os.path.isfile(path):
                continue

            if name.startswith("envios_11_") and name.endswith(".txt"):
                events.extend(self._parse_envios_file(path))
                continue

            if name.startswith("cloudv2-info_respostas_") and name.endswith(".txt"):
                events.extend(self._parse_topic_file(path, "cloudv2-info"))
                continue

            if name.endswith(".txt"):
                topic = self._topic_from_filename(name)
                if topic:
                    events.extend(self._parse_topic_file(path, topic))

        events.sort(key=lambda item: item["ts"])
        for event in events:
            if event["type"] == "message":
                self.record_message(event["topic"], event["payload"], ts=event["ts"], mark_dirty=False)
            elif event["type"] == "ping_result":
                pivot_id = event.get("cmd_topic")
                if not pivot_id:
                    if self.cmd_topics:
                        pivot_id = self.cmd_topics[0]
                    elif self.pivot_ids:
                        pivot_id = self.pivot_ids[0]
                    else:
                        pivot_id = "pivot"
                self.record_ping_result(pivot_id, event["ok"], ts=event["ts"], source="log", mark_dirty=False)

        self._dirty = True
        self.write()

    def _topic_from_filename(self, name):
        if "_" not in name:
            return None
        base = name.rsplit("_", 1)[0]
        return base

    def _parse_topic_file(self, path, topic):
        events = []
        entries = self._read_log_entries(path)
        for ts, payload in entries:
            events.append({"type": "message", "topic": topic, "payload": payload, "ts": ts})
        return events

    def _parse_envios_file(self, path):
        events = []
        entries = self._read_log_entries(path)
        for ts, payload in entries:
            cmd_topic = None
            payload_text = payload.strip()
            match = re.search(r"#11\$\s*\[(.+?)\]\s*-\s*(SIM|NAO)", payload_text, flags=re.IGNORECASE)
            if match:
                cmd_topic = match.group(1).strip()
                result_text = match.group(2).upper()
            else:
                result_text = "SIM" if "SIM" in payload_text.upper() else "NAO"
            ok = "SIM" in payload_text.upper()
            if result_text == "NAO":
                ok = False
            events.append({"type": "ping_result", "ok": ok, "ts": ts, "cmd_topic": cmd_topic})
        return events

    def _read_log_entries(self, path):
        entries = []
        try:
            with open(path, "r", encoding="utf-8") as file:
                lines = [line.rstrip("\n") for line in file]
        except OSError:
            return entries

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("[") and line.endswith("]"):
                ts_text = line[1:-1]
                ts = self._parse_timestamp(ts_text)
                payload = ""
                if i + 1 < len(lines):
                    payload = lines[i + 1].strip()
                if ts:
                    entries.append((ts, payload))
                i += 2
                continue
            i += 1
        return entries

    def _parse_timestamp(self, value):
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return parsed.timestamp()
        except ValueError:
            return None
