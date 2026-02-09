import hashlib
import json
import logging
import os
import re
import statistics
import threading
import time
from datetime import datetime

from cloudv2_dashboard import DATA_DIR, ensure_dirs, slugify, write_json_atomic


TOPIC_CLOUDV2 = "cloudv2"
TOPIC_PING = "cloudv2-ping"
TOPIC_CLOUD2 = "cloud2"
TOPIC_NETWORK = "cloudv2-network"
TOPIC_INFO = "cloudv2-info"
MONITOR_TOPICS = (TOPIC_CLOUDV2, TOPIC_PING, TOPIC_CLOUD2, TOPIC_NETWORK, TOPIC_INFO)
PROBE_RESPONSE_TOPICS = {TOPIC_NETWORK, TOPIC_INFO}
CONNECTIVITY_TOPICS = (TOPIC_CLOUDV2, TOPIC_PING, TOPIC_INFO, TOPIC_NETWORK)

STATUS_LABELS = {
    "green": "Conectado",
    "yellow": "Atencao",
    "red": "Offline",
    "gray": "Inicial",
}

STATUS_RANK = {
    "red": 0,
    "yellow": 1,
    "gray": 2,
    "green": 3,
}


def _ts_to_str(ts):
    if ts is None:
        return "-"
    return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


def _format_ago(seconds):
    if seconds is None:
        return "-"
    if seconds < 0:
        return "0s atras"
    if seconds < 60:
        return f"{int(seconds)}s atras"
    if seconds < 3600:
        return f"{int(seconds / 60)}m atras"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h atras"
    return f"{seconds / 86400:.1f}d atras"


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_duration_seconds(value):
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    clock_match = re.match(r"^(\d+):(\d+)$", text)
    if clock_match:
        minutes = int(clock_match.group(1))
        seconds = int(clock_match.group(2))
        return (minutes * 60) + seconds

    unit_match = re.match(r"^(\d+(?:\.\d+)?)\s*(s|sec|secs|m|min|mins|h|hr|hrs)$", text)
    if unit_match:
        amount = float(unit_match.group(1))
        unit = unit_match.group(2)
        if unit.startswith("h"):
            return int(amount * 3600)
        if unit.startswith("m"):
            return int(amount * 60)
        return int(amount)

    return None


def parse_device_payload(payload):
    text = str(payload or "").strip()
    if not text:
        return None, "payload vazio"
    if not text.startswith("#"):
        return None, "payload sem prefixo #"
    if not text.endswith("$"):
        return None, "payload sem sufixo $"

    core = text[1:-1]
    if not core:
        return None, "payload sem conteudo interno"

    parts = [part.strip() for part in core.split("-")]
    if len(parts) < 2:
        return None, "payload sem campos suficientes"

    idp = parts[0]
    pivot_id = parts[1]
    if not idp:
        return None, "campo IDP vazio"
    if not pivot_id:
        return None, "campo pivot_id vazio"

    parsed = {
        "raw": text,
        "idp": idp,
        "pivot_id": pivot_id,
        "parts": parts,
    }
    return parsed, None


class TelemetryStore:
    def __init__(self, config, log_dir):
        self.log = logging.getLogger("cloudv2.telemetry")
        self.log_dir = log_dir

        self.monitor_topics = tuple(config.get("monitor_topics") or MONITOR_TOPICS)
        self.refresh_sec = max(1, int(config.get("dashboard_refresh_sec", 5)))
        self.history_mode = str(config.get("history_mode", "merge")).strip().lower()

        self.ping_expected_sec = max(1, int(config.get("ping_interval_minutes", 3)) * 60)
        self.tolerance_factor = max(1.0, float(config.get("tolerance_factor", 1.25)))
        self.attention_disconnected_pct_threshold = min(
            100.0,
            max(0.0, float(config.get("attention_disconnected_pct_threshold", 20.0))),
        )
        self.attention_disconnected_window_hours = max(
            1,
            int(config.get("attention_disconnected_window_hours", 24)),
        )
        self.attention_disconnected_window_sec = self.attention_disconnected_window_hours * 3600
        self.cloudv2_window = max(3, int(config.get("cloudv2_median_window", 20)))
        self.cloudv2_min_samples = max(2, int(config.get("cloudv2_min_samples", 3)))
        if self.cloudv2_min_samples > self.cloudv2_window:
            self.cloudv2_min_samples = self.cloudv2_window

        self.dedupe_window_sec = max(1, int(config.get("dedupe_window_sec", 8)))
        self.history_retention_hours = max(24, int(config.get("history_retention_hours", 24)))
        self.retention_sec = self.history_retention_hours * 3600
        self.max_events_per_pivot = max(100, int(config.get("max_events_per_pivot", 5000)))
        self.show_pending_ping_pivots = bool(config.get("show_pending_ping_pivots", False))

        self.probe_default_interval_sec = max(10, int(config.get("probe_default_interval_sec", 300)))
        self.probe_min_interval_sec = max(10, int(config.get("probe_min_interval_sec", 60)))
        if self.probe_default_interval_sec < self.probe_min_interval_sec:
            self.probe_default_interval_sec = self.probe_min_interval_sec
        self.probe_timeout_factor = max(1.0, float(config.get("probe_timeout_factor", 1.25)))
        self.probe_timeout_streak_alert = max(1, int(config.get("probe_timeout_streak_alert", 2)))

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._background_loop, name="cloudv2-telemetry", daemon=True)
        self._started = False
        self._dirty = True
        self._last_write_ts = 0.0
        self._event_seq = 0
        self._probe_sender = None

        self.pivots = {}
        self.pending_ping_unknown = {}
        self.malformed_messages = []
        self.duplicate_count = 0

        self._dedupe_cache = {}
        self._probe_settings = self._normalize_probe_settings(config.get("probe_settings", {}))

        self.runtime_path = os.path.join(DATA_DIR, "runtime_store.json")

    def start(self):
        ensure_dirs()
        os.makedirs(self.log_dir, exist_ok=True)

        if self.history_mode == "merge":
            self._load_runtime_state()
        else:
            self._clear_dashboard_data_files()

        self._dirty = True
        self.write()

        if not self._started:
            self._started = True
            self._worker.start()

    def stop(self):
        self._stop_event.set()
        if self._started:
            self._worker.join(timeout=2.5)
        self.write()

    def set_probe_sender(self, sender_fn):
        self._probe_sender = sender_fn

    def process_message(self, topic, payload, ts=None):
        ts = float(ts if ts is not None else time.time())
        topic = str(topic or "").strip()
        payload_text = str(payload or "").strip()

        if not topic:
            return {"accepted": False, "reason": "topic vazio"}

        with self._lock:
            if topic not in self.monitor_topics:
                self.log.warning("Mensagem em topico nao monitorado descartada: topic=%s", topic)
                return {"accepted": False, "reason": "topic nao monitorado"}

            if self._is_duplicate_locked(topic, payload_text, ts):
                self.duplicate_count += 1
                return {"accepted": False, "reason": "duplicada"}

            parsed, parse_error = parse_device_payload(payload_text)
            if parse_error:
                self._record_malformed_locked(topic, payload_text, parse_error, ts)
                return {"accepted": False, "reason": parse_error}

            pivot_id = parsed["pivot_id"]

            if topic == TOPIC_CLOUDV2:
                pivot = self._get_or_create_pivot_locked(pivot_id, ts)
                self._record_message_common_locked(pivot, topic, ts)
                self._record_cloudv2_locked(pivot, parsed, topic, ts)
                self._refresh_status_locked(pivot, ts)
                self._prune_pivot_locked(pivot, ts)
                self._dirty = True
                return {"accepted": True, "pivot_id": pivot_id, "event": "cloudv2"}

            pivot = self.pivots.get(pivot_id)
            if pivot is None:
                if topic == TOPIC_PING:
                    self._record_pending_ping_locked(pivot_id, ts, payload_text)
                    return {
                        "accepted": False,
                        "reason": "ping para pivot nao descoberto",
                        "pivot_id": pivot_id,
                    }

                self.log.info(
                    "Mensagem descartada para pivot nao descoberto: topic=%s pivot_id=%s",
                    topic,
                    pivot_id,
                )
                return {
                    "accepted": False,
                    "reason": "pivot nao descoberto",
                    "pivot_id": pivot_id,
                }

            self._record_message_common_locked(pivot, topic, ts)

            if topic == TOPIC_PING:
                self._record_ping_locked(pivot, parsed, topic, ts)
            elif topic == TOPIC_CLOUD2:
                self._record_cloud2_locked(pivot, parsed, topic, ts)
            elif topic in PROBE_RESPONSE_TOPICS:
                self._record_probe_response_locked(pivot, parsed, topic, ts)

            self._refresh_status_locked(pivot, ts)
            self._prune_pivot_locked(pivot, ts)
            self._dirty = True
            return {"accepted": True, "pivot_id": pivot_id, "event": topic}

    def tick(self, now=None):
        now = float(now if now is not None else time.time())
        send_candidates = []
        changed = False

        with self._lock:
            for pivot in self.pivots.values():
                timed_out = self._check_probe_timeout_locked(pivot, now)
                if timed_out:
                    changed = True
                if self._refresh_status_locked(pivot, now):
                    changed = True
                self._prune_pivot_locked(pivot, now)
                if (not timed_out) and self._probe_should_send_locked(pivot, now):
                    send_candidates.append(pivot["pivot_id"])

            self._cleanup_pending_ping_locked(now)
            self._cleanup_dedupe_locked(now)

        for pivot_id in send_candidates:
            if pivot_id in MONITOR_TOPICS:
                self.log.error(
                    "Bloqueio de seguranca: tentativa de publicar probe em topico fixo monitorado (%s)",
                    pivot_id,
                )
                continue

            sender = self._probe_sender
            if sender is None:
                continue

            ok = False
            try:
                ok = bool(sender(pivot_id, "#11$"))
            except Exception as exc:
                self.log.exception("Erro ao enviar probe #11$ para %s: %s", pivot_id, exc)

            if ok:
                with self._lock:
                    pivot = self.pivots.get(pivot_id)
                    if pivot is not None:
                        self._record_probe_sent_locked(pivot, now)
                        self._refresh_status_locked(pivot, now)
                        changed = True
            else:
                self.log.warning("Falha ao publicar probe #11$ para pivot %s", pivot_id)

        if changed:
            self._dirty = True
        return changed

    def write(self):
        now = time.time()
        with self._lock:
            state_payload = self._build_state_snapshot_locked(now)
            pivot_payloads = {
                pivot_id: self._build_pivot_snapshot_locked(pivot, now)
                for pivot_id, pivot in self.pivots.items()
            }
            mapping = [
                {
                    "pivot_id": pivot_id,
                    "slug": pivot["pivot_slug"],
                    "file": f"pivot_{pivot['pivot_slug']}.json",
                }
                for pivot_id, pivot in sorted(self.pivots.items(), key=lambda item: item[0].lower())
            ]
            runtime_payload = self._build_runtime_payload_locked(now)

            self._dirty = False
            self._last_write_ts = now

        write_json_atomic(os.path.join(DATA_DIR, "state.json"), state_payload)
        write_json_atomic(os.path.join(DATA_DIR, "pivots.json"), mapping)

        for pivot_id, payload in pivot_payloads.items():
            slug = slugify(pivot_id)
            write_json_atomic(os.path.join(DATA_DIR, f"pivot_{slug}.json"), payload)

        write_json_atomic(self.runtime_path, runtime_payload)

    def get_state_snapshot(self, now=None):
        now = float(now if now is not None else time.time())
        with self._lock:
            return self._build_state_snapshot_locked(now)

    def get_pivot_snapshot(self, pivot_id, now=None):
        now = float(now if now is not None else time.time())
        normalized = str(pivot_id or "").strip()
        if not normalized:
            return None
        with self._lock:
            pivot = self.pivots.get(normalized)
            if pivot is None:
                return None
            return self._build_pivot_snapshot_locked(pivot, now)

    def get_probe_config_snapshot(self):
        with self._lock:
            items = []
            for pivot_id, setting in sorted(self._probe_settings.items(), key=lambda item: item[0].lower()):
                items.append(
                    {
                        "pivot_id": pivot_id,
                        "enabled": bool(setting.get("enabled")),
                        "interval_sec": int(setting.get("interval_sec", self.probe_default_interval_sec)),
                    }
                )
            return {
                "default_interval_sec": self.probe_default_interval_sec,
                "min_interval_sec": self.probe_min_interval_sec,
                "timeout_factor": self.probe_timeout_factor,
                "items": items,
            }

    def update_probe_setting(self, pivot_id, enabled, interval_sec):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")

        normalized_enabled = bool(enabled)
        normalized_interval = _safe_int(interval_sec, self.probe_default_interval_sec)
        if normalized_interval is None:
            normalized_interval = self.probe_default_interval_sec
        if normalized_interval < self.probe_min_interval_sec:
            normalized_interval = self.probe_min_interval_sec

        with self._lock:
            self._probe_settings[normalized_pivot] = {
                "enabled": normalized_enabled,
                "interval_sec": normalized_interval,
            }

            pivot = self.pivots.get(normalized_pivot)
            if pivot is not None:
                probe = pivot["probe"]
                probe["enabled"] = normalized_enabled
                probe["interval_sec"] = normalized_interval
                if not normalized_enabled:
                    probe["pending_sent_ts"] = None
                    probe["pending_deadline_ts"] = None
                self._refresh_status_locked(pivot, time.time())

            self._dirty = True

        self.log.info(
            "Configuracao de probe atualizada: pivot_id=%s enabled=%s interval_sec=%s",
            normalized_pivot,
            normalized_enabled,
            normalized_interval,
        )

        return {
            "pivot_id": normalized_pivot,
            "enabled": normalized_enabled,
            "interval_sec": normalized_interval,
        }

    def _background_loop(self):
        while not self._stop_event.is_set():
            now = time.time()
            self.tick(now)

            if self._dirty and (now - self._last_write_ts) >= self.refresh_sec:
                self.write()

            self._stop_event.wait(1.0)

    def _is_duplicate_locked(self, topic, payload, ts):
        digest = hashlib.sha1(f"{topic}|{payload}".encode("utf-8", errors="ignore")).hexdigest()
        last_ts = self._dedupe_cache.get(digest)
        self._dedupe_cache[digest] = ts

        if last_ts is None:
            return False
        return (ts - last_ts) <= self.dedupe_window_sec

    def _cleanup_dedupe_locked(self, now):
        if len(self._dedupe_cache) < 5000:
            return
        threshold = now - (self.dedupe_window_sec * 4)
        keep = {}
        for digest, ts in self._dedupe_cache.items():
            if ts >= threshold:
                keep[digest] = ts
        self._dedupe_cache = keep

    def _normalize_probe_settings(self, probe_settings):
        normalized = {}
        if not isinstance(probe_settings, dict):
            return normalized

        for raw_pivot_id, raw_setting in probe_settings.items():
            pivot_id = str(raw_pivot_id or "").strip()
            if not pivot_id:
                continue

            if isinstance(raw_setting, dict):
                enabled = bool(raw_setting.get("enabled", False))
                interval_sec = _safe_int(raw_setting.get("interval_sec"), self.probe_default_interval_sec)
            else:
                enabled = bool(raw_setting)
                interval_sec = self.probe_default_interval_sec

            if interval_sec is None:
                interval_sec = self.probe_default_interval_sec
            if interval_sec < self.probe_min_interval_sec:
                interval_sec = self.probe_min_interval_sec

            normalized[pivot_id] = {
                "enabled": enabled,
                "interval_sec": interval_sec,
            }

        return normalized

    def _new_pivot_state(self, pivot_id, discovered_ts):
        probe_setting = self._probe_settings.get(pivot_id, {})
        probe_enabled = bool(probe_setting.get("enabled", False))
        probe_interval = _safe_int(probe_setting.get("interval_sec"), self.probe_default_interval_sec)
        if probe_interval is None:
            probe_interval = self.probe_default_interval_sec
        if probe_interval < self.probe_min_interval_sec:
            probe_interval = self.probe_min_interval_sec

        return {
            "pivot_id": pivot_id,
            "pivot_slug": slugify(pivot_id),
            "discovered_at_ts": discovered_ts,
            "last_seen_ts": discovered_ts,
            "last_ping_ts": None,
            "last_cloudv2_ts": None,
            "last_cloud2_ts": None,
            "topic_counters": {topic: 0 for topic in self.monitor_topics},
            "cloudv2_intervals_sec": [],
            "topic_last_ts": {topic: None for topic in CONNECTIVITY_TOPICS},
            "topic_intervals_sec": {topic: [] for topic in CONNECTIVITY_TOPICS},
            "last_cloud2": None,
            "cloud2_events": [],
            "drop_events": [],
            "timeline": [],
            "probe": {
                "enabled": probe_enabled,
                "interval_sec": probe_interval,
                "last_sent_ts": None,
                "last_response_ts": None,
                "pending_sent_ts": None,
                "pending_deadline_ts": None,
                "timeout_streak": 0,
                "last_result": None,
                "events": [],
            },
            "status_cache": {
                "code": "gray",
                "reason": "Aguardando amostras iniciais de cloudv2.",
                "changed_at_ts": discovered_ts,
            },
        }

    def _get_or_create_pivot_locked(self, pivot_id, ts):
        pivot = self.pivots.get(pivot_id)
        if pivot is not None:
            return pivot

        pivot = self._new_pivot_state(pivot_id, ts)
        self.pivots[pivot_id] = pivot

        self._record_timeline_locked(
            pivot,
            event_type="pivot_discovered",
            topic=TOPIC_CLOUDV2,
            ts=ts,
            summary="Pivot descoberto automaticamente via cloudv2.",
            details={"pivot_id": pivot_id},
        )
        self.log.info("Pivot descoberto via cloudv2: pivot_id=%s", pivot_id)
        return pivot

    def _record_message_common_locked(self, pivot, topic, ts):
        pivot["last_seen_ts"] = ts
        counters = pivot["topic_counters"]
        counters[topic] = counters.get(topic, 0) + 1

        if topic in CONNECTIVITY_TOPICS:
            topic_last = pivot.setdefault("topic_last_ts", {item: None for item in CONNECTIVITY_TOPICS})
            topic_intervals = pivot.setdefault("topic_intervals_sec", {item: [] for item in CONNECTIVITY_TOPICS})

            previous_ts = _safe_float(topic_last.get(topic), None)
            if previous_ts is not None and ts > previous_ts:
                intervals = topic_intervals.setdefault(topic, [])
                intervals.append(ts - previous_ts)
                if len(intervals) > self.cloudv2_window:
                    topic_intervals[topic] = intervals[-self.cloudv2_window :]
            topic_last[topic] = ts

    def _record_cloudv2_locked(self, pivot, parsed, topic, ts):
        intervals = (
            pivot.get("topic_intervals_sec", {}).get(TOPIC_CLOUDV2)
            or pivot.get("cloudv2_intervals_sec", [])
        )
        interval_sec = intervals[-1] if intervals else None
        if intervals:
            pivot["cloudv2_intervals_sec"] = intervals[-self.cloudv2_window :]
            median_value = statistics.median(pivot["cloudv2_intervals_sec"])
            self.log.debug(
                "Mediana cloudv2 atualizada: pivot_id=%s mediana=%.2fs amostras=%s",
                pivot["pivot_id"],
                median_value,
                len(pivot["cloudv2_intervals_sec"]),
            )

        pivot["last_cloudv2_ts"] = ts

        details = {
            "idp": parsed.get("idp"),
            "field_count": len(parsed.get("parts", [])),
        }
        if interval_sec is not None:
            details["interval_sec"] = interval_sec

        self._record_timeline_locked(
            pivot,
            event_type="cloudv2",
            topic=topic,
            ts=ts,
            summary="Pacote cloudv2 recebido.",
            details=details,
        )

    def _record_ping_locked(self, pivot, parsed, topic, ts):
        pivot["last_ping_ts"] = ts
        self._record_timeline_locked(
            pivot,
            event_type="ping",
            topic=topic,
            ts=ts,
            summary="Ping passivo recebido em cloudv2-ping.",
            details={"idp": parsed.get("idp")},
        )

    def _record_cloud2_locked(self, pivot, parsed, topic, ts):
        parts = parsed.get("parts", [])

        # cloud2 pode trazer RSSI negativo no formato "...-<pivot>--67-..."
        # e data final com hifens; por isso parseamos o "tail" com cuidado.
        rssi = None
        technology = None
        drop_duration_raw = None
        firmware = None
        event_date = None

        raw_payload = parsed.get("raw", "")
        core = raw_payload[1:-1] if raw_payload.startswith("#") and raw_payload.endswith("$") else ""
        core_parts = core.split("-", 2)
        tail = core_parts[2] if len(core_parts) >= 3 else ""
        tail_tokens = tail.split("-") if tail else []

        idx = 0
        if tail_tokens:
            first = tail_tokens[0]
            if first == "" and len(tail_tokens) > 1 and re.fullmatch(r"\d+", tail_tokens[1] or ""):
                rssi = f"-{tail_tokens[1]}"
                idx = 2
            elif re.fullmatch(r"-?\d+", first or ""):
                rssi = first
                idx = 1

        if len(tail_tokens) > idx:
            technology = tail_tokens[idx] or None
        if len(tail_tokens) > idx + 1:
            drop_duration_raw = tail_tokens[idx + 1] or None
        if len(tail_tokens) > idx + 2:
            firmware = tail_tokens[idx + 2] or None
        if len(tail_tokens) > idx + 3:
            event_date = "-".join(tail_tokens[idx + 3 :]) or None

        # Fallback para formatos legados/curtos.
        if rssi is None and len(parts) > 2:
            rssi = parts[2] or None
        if technology is None and len(parts) > 3:
            technology = parts[3] or None
        if drop_duration_raw is None and len(parts) > 4:
            drop_duration_raw = parts[4] or None
        if firmware is None and len(parts) > 5:
            firmware = parts[5] or None
        if event_date is None and len(parts) > 6:
            event_date = "-".join(parts[6:]) or None

        drop_duration_sec = _parse_duration_seconds(drop_duration_raw)

        cloud2_event = {
            "ts": ts,
            "at": _ts_to_str(ts),
            "idp": parsed.get("idp"),
            "rssi": rssi,
            "technology": technology,
            "drop_duration_raw": drop_duration_raw,
            "drop_duration_sec": drop_duration_sec,
            "firmware": firmware,
            "event_date": event_date,
            "raw": parsed.get("raw"),
        }

        pivot["last_cloud2_ts"] = ts
        pivot["last_cloud2"] = cloud2_event
        pivot["cloud2_events"].append(cloud2_event)
        if len(pivot["cloud2_events"]) > self.max_events_per_pivot:
            pivot["cloud2_events"] = pivot["cloud2_events"][-self.max_events_per_pivot :]

        if drop_duration_sec is not None and drop_duration_sec > 0:
            pivot["drop_events"].append(
                {
                    "ts": ts,
                    "at": _ts_to_str(ts),
                    "duration_sec": drop_duration_sec,
                    "technology": technology,
                    "rssi": rssi,
                }
            )
            if len(pivot["drop_events"]) > self.max_events_per_pivot:
                pivot["drop_events"] = pivot["drop_events"][-self.max_events_per_pivot :]

        self._record_timeline_locked(
            pivot,
            event_type="cloud2",
            topic=topic,
            ts=ts,
            summary="Evento cloud2 (conexao/reconexao) recebido.",
            details={
                "rssi": rssi,
                "technology": technology,
                "drop_duration_raw": drop_duration_raw,
                "drop_duration_sec": drop_duration_sec,
                "firmware": firmware,
                "event_date": event_date,
            },
        )

    def _record_probe_response_locked(self, pivot, parsed, topic, ts):
        probe = pivot["probe"]
        pending_sent_ts = probe.get("pending_sent_ts")
        pending_deadline_ts = probe.get("pending_deadline_ts")

        if (
            pending_sent_ts is not None
            and pending_deadline_ts is not None
            and pending_sent_ts <= ts <= pending_deadline_ts
        ):
            latency = ts - pending_sent_ts
            probe["pending_sent_ts"] = None
            probe["pending_deadline_ts"] = None
            probe["last_response_ts"] = ts
            probe["timeout_streak"] = 0
            probe["last_result"] = "response"
            probe["events"].append(
                {
                    "type": "response",
                    "ts": ts,
                    "at": _ts_to_str(ts),
                    "topic": topic,
                    "latency_sec": latency,
                }
            )
            if len(probe["events"]) > self.max_events_per_pivot:
                probe["events"] = probe["events"][-self.max_events_per_pivot :]

            self._record_timeline_locked(
                pivot,
                event_type="probe_response",
                topic=topic,
                ts=ts,
                summary="Resposta de probe #11$ recebida dentro da janela esperada.",
                details={
                    "latency_sec": latency,
                    "source_topic": topic,
                },
            )
            self.log.info(
                "Probe respondido: pivot_id=%s topic=%s latency=%.2fs",
                pivot["pivot_id"],
                topic,
                latency,
            )
            return

        self._record_timeline_locked(
            pivot,
            event_type="probe_response_unmatched",
            topic=topic,
            ts=ts,
            summary="Resposta recebida sem probe pendente na janela temporal.",
            details={"source_topic": topic},
        )

    def _probe_should_send_locked(self, pivot, now):
        probe = pivot["probe"]
        if not probe.get("enabled"):
            return False

        if probe.get("pending_sent_ts") is not None:
            return False

        interval_sec = _safe_int(probe.get("interval_sec"), self.probe_default_interval_sec)
        if interval_sec is None:
            interval_sec = self.probe_default_interval_sec
        if interval_sec < self.probe_min_interval_sec:
            interval_sec = self.probe_min_interval_sec
        probe["interval_sec"] = interval_sec

        last_sent_ts = probe.get("last_sent_ts")
        if last_sent_ts is None:
            return True

        return (now - last_sent_ts) >= interval_sec

    def _record_probe_sent_locked(self, pivot, ts):
        probe = pivot["probe"]
        interval_sec = _safe_int(probe.get("interval_sec"), self.probe_default_interval_sec)
        if interval_sec is None:
            interval_sec = self.probe_default_interval_sec
        if interval_sec < self.probe_min_interval_sec:
            interval_sec = self.probe_min_interval_sec

        deadline_ts = ts + (interval_sec * self.probe_timeout_factor)
        probe["last_sent_ts"] = ts
        probe["pending_sent_ts"] = ts
        probe["pending_deadline_ts"] = deadline_ts
        probe["last_result"] = "sent"

        probe["events"].append(
            {
                "type": "sent",
                "ts": ts,
                "at": _ts_to_str(ts),
                "topic": pivot["pivot_id"],
                "payload": "#11$",
                "deadline_ts": deadline_ts,
                "deadline_at": _ts_to_str(deadline_ts),
            }
        )
        if len(probe["events"]) > self.max_events_per_pivot:
            probe["events"] = probe["events"][-self.max_events_per_pivot :]

        self._record_timeline_locked(
            pivot,
            event_type="probe_sent",
            topic=pivot["pivot_id"],
            ts=ts,
            summary="Probe #11$ enviado no topico dinamico do pivot.",
            details={
                "payload": "#11$",
                "deadline_ts": deadline_ts,
                "deadline_at": _ts_to_str(deadline_ts),
            },
        )
        self.log.info("Probe #11$ enviado: pivot_id=%s", pivot["pivot_id"])

    def _check_probe_timeout_locked(self, pivot, now):
        probe = pivot["probe"]
        pending_sent_ts = probe.get("pending_sent_ts")
        pending_deadline_ts = probe.get("pending_deadline_ts")
        if pending_sent_ts is None or pending_deadline_ts is None:
            return False

        if now <= pending_deadline_ts:
            return False

        probe["pending_sent_ts"] = None
        probe["pending_deadline_ts"] = None
        probe["timeout_streak"] = int(probe.get("timeout_streak", 0)) + 1
        probe["last_result"] = "timeout"

        probe["events"].append(
            {
                "type": "timeout",
                "ts": now,
                "at": _ts_to_str(now),
                "sent_ts": pending_sent_ts,
                "sent_at": _ts_to_str(pending_sent_ts),
            }
        )
        if len(probe["events"]) > self.max_events_per_pivot:
            probe["events"] = probe["events"][-self.max_events_per_pivot :]

        self._record_timeline_locked(
            pivot,
            event_type="probe_timeout",
            topic=pivot["pivot_id"],
            ts=now,
            summary="Probe #11$ sem resposta dentro da janela esperada.",
            details={
                "sent_ts": pending_sent_ts,
                "deadline_ts": pending_deadline_ts,
            },
        )

        self.log.warning(
            "Probe com timeout: pivot_id=%s streak=%s",
            pivot["pivot_id"],
            probe["timeout_streak"],
        )
        return True

    def _record_timeline_locked(self, pivot, event_type, topic, ts, summary, details=None):
        self._event_seq += 1
        event = {
            "id": self._event_seq,
            "ts": ts,
            "at": _ts_to_str(ts),
            "type": event_type,
            "topic": topic,
            "summary": summary,
            "details": details or {},
        }
        pivot["timeline"].append(event)
        if len(pivot["timeline"]) > self.max_events_per_pivot:
            pivot["timeline"] = pivot["timeline"][-self.max_events_per_pivot :]

    def _record_malformed_locked(self, topic, payload, reason, ts):
        excerpt = payload[:240]
        info = {
            "ts": ts,
            "at": _ts_to_str(ts),
            "topic": topic,
            "reason": reason,
            "payload_excerpt": excerpt,
        }
        self.malformed_messages.append(info)
        if len(self.malformed_messages) > 500:
            self.malformed_messages = self.malformed_messages[-500:]

        self.log.warning(
            "Payload malformado descartado: topic=%s reason=%s payload=%s",
            topic,
            reason,
            excerpt,
        )

    def _record_pending_ping_locked(self, pivot_id, ts, payload):
        entry = self.pending_ping_unknown.get(pivot_id)
        if entry is None:
            entry = {
                "pivot_id": pivot_id,
                "first_seen_ts": ts,
                "last_seen_ts": ts,
                "count": 0,
                "last_payload_excerpt": "",
            }
            self.pending_ping_unknown[pivot_id] = entry

        entry["count"] += 1
        entry["last_seen_ts"] = ts
        entry["last_payload_excerpt"] = payload[:160]

        self.log.info(
            "Ping recebido para pivot ainda nao descoberto via cloudv2: pivot_id=%s count=%s",
            pivot_id,
            entry["count"],
        )

    def _cleanup_pending_ping_locked(self, now):
        cutoff = now - self.retention_sec
        keep = {}
        for pivot_id, entry in self.pending_ping_unknown.items():
            last_seen_ts = _safe_float(entry.get("last_seen_ts"), 0)
            if last_seen_ts >= cutoff:
                keep[pivot_id] = entry
        self.pending_ping_unknown = keep

    def _prune_pivot_locked(self, pivot, now):
        cutoff = now - self.retention_sec

        pivot["timeline"] = [event for event in pivot["timeline"] if _safe_float(event.get("ts"), 0) >= cutoff]
        pivot["cloud2_events"] = [
            event for event in pivot["cloud2_events"] if _safe_float(event.get("ts"), 0) >= cutoff
        ]
        pivot["drop_events"] = [event for event in pivot["drop_events"] if _safe_float(event.get("ts"), 0) >= cutoff]

        probe = pivot["probe"]
        probe["events"] = [event for event in probe["events"] if _safe_float(event.get("ts"), 0) >= cutoff]

    def _compute_disconnected_pct_locked(self, pivot, now, disconnect_threshold_sec):
        if disconnect_threshold_sec is None or disconnect_threshold_sec <= 0:
            return None

        window_sec = min(self.attention_disconnected_window_sec, self.retention_sec)
        if window_sec <= 0:
            return None

        start_ts = now - window_sec
        min_relevant_ts = start_ts - disconnect_threshold_sec

        message_ts = []
        for event in pivot.get("timeline", []):
            topic = str(event.get("topic") or "")
            if topic not in CONNECTIVITY_TOPICS:
                continue
            ts = _safe_float(event.get("ts"), None)
            if ts is None:
                continue
            if ts > now or ts < min_relevant_ts:
                continue
            message_ts.append(ts)

        if not message_ts:
            return 100.0

        message_ts = sorted(set(message_ts))

        connected_sec = 0.0
        current_start = None
        current_end = None

        for ts in message_ts:
            seg_start = max(start_ts, ts)
            seg_end = min(now, ts + disconnect_threshold_sec)
            if seg_end <= seg_start:
                continue

            if current_start is None:
                current_start = seg_start
                current_end = seg_end
                continue

            if seg_start <= current_end:
                if seg_end > current_end:
                    current_end = seg_end
                continue

            connected_sec += current_end - current_start
            current_start = seg_start
            current_end = seg_end

        if current_start is not None:
            connected_sec += current_end - current_start

        connected_sec = max(0.0, min(float(window_sec), connected_sec))
        disconnected_sec = max(0.0, float(window_sec) - connected_sec)
        return (disconnected_sec / float(window_sec)) * 100.0

    def _compute_status_locked(self, pivot, now):
        topic_last_ts = pivot.get("topic_last_ts") or {}
        topic_intervals = pivot.get("topic_intervals_sec") or {}

        last_ping_ts = _safe_float(pivot.get("last_ping_ts"), None)
        last_cloudv2_ts = _safe_float(pivot.get("last_cloudv2_ts"), None)

        ping_window = self.ping_expected_sec * self.tolerance_factor
        ping_ok = False
        ping_age_sec = None
        if last_ping_ts is not None:
            ping_age_sec = now - last_ping_ts
            ping_ok = ping_age_sec <= ping_window

        cloudv2_intervals = (
            topic_intervals.get(TOPIC_CLOUDV2)
            if isinstance(topic_intervals.get(TOPIC_CLOUDV2), list)
            else pivot.get("cloudv2_intervals_sec", [])
        ) or []
        sample_count = len(cloudv2_intervals)
        median_interval_sec = statistics.median(cloudv2_intervals) if cloudv2_intervals else None
        cloudv2_window = None
        cloudv2_ok = False
        cloudv2_age_sec = None
        if median_interval_sec is not None and last_cloudv2_ts is not None:
            cloudv2_window = median_interval_sec * self.tolerance_factor
            cloudv2_age_sec = now - last_cloudv2_ts
            cloudv2_ok = cloudv2_age_sec <= cloudv2_window

        median_ready = sample_count >= self.cloudv2_min_samples

        expected_by_topic = {}
        for topic in CONNECTIVITY_TOPICS:
            intervals = topic_intervals.get(topic)
            topic_expected = None
            if isinstance(intervals, list) and intervals:
                if len(intervals) >= self.cloudv2_min_samples or topic in (TOPIC_PING, TOPIC_CLOUDV2):
                    topic_expected = statistics.median(intervals)
            if topic_expected is None and topic == TOPIC_PING:
                topic_expected = float(self.ping_expected_sec)
            expected_by_topic[topic] = topic_expected

        expected_candidates = [value for value in expected_by_topic.values() if value is not None and value > 0]
        max_expected_interval_sec = max(expected_candidates) if expected_candidates else None
        disconnect_threshold_sec = (
            max_expected_interval_sec * self.tolerance_factor if max_expected_interval_sec is not None else None
        )

        monitored_last_values = [
            _safe_float(topic_last_ts.get(topic), None)
            for topic in CONNECTIVITY_TOPICS
        ]
        monitored_last_values = [value for value in monitored_last_values if value is not None]
        last_monitored_message_ts = max(monitored_last_values) if monitored_last_values else None
        monitored_age_sec = (
            now - last_monitored_message_ts if last_monitored_message_ts is not None else None
        )

        disconnected_by_inactivity = False
        if disconnect_threshold_sec is not None and monitored_age_sec is not None:
            disconnected_by_inactivity = monitored_age_sec > disconnect_threshold_sec

        attention_disconnected_pct = self._compute_disconnected_pct_locked(
            pivot,
            now,
            disconnect_threshold_sec,
        )
        attention_by_disconnected_pct = (
            attention_disconnected_pct is not None
            and attention_disconnected_pct > self.attention_disconnected_pct_threshold
        )

        if disconnected_by_inactivity:
            code = "red"
            reason = (
                "Sem mensagens recentes nos topicos monitorados "
                "(cloudv2/cloudv2-ping/cloudv2-info/cloudv2-network) "
                "alem da janela esperada."
            )
        elif not median_ready:
            code = "gray"
            reason = (
                "Aguardando amostras de cloudv2 para estimar mediana "
                f"({sample_count}/{self.cloudv2_min_samples})."
            )
        elif attention_by_disconnected_pct:
            code = "yellow"
            reason = (
                "Percentual desconectado na janela de monitoramento "
                f"({attention_disconnected_pct:.1f}%) acima do limite "
                f"({self.attention_disconnected_pct_threshold:.1f}%)."
            )
        elif ping_ok and not cloudv2_ok:
            code = "yellow"
            reason = "Ping dentro do esperado, mas cloudv2 fora da janela estimada."
        elif ping_ok and cloudv2_ok:
            code = "green"
            reason = "Mensagens monitoradas dentro da janela esperada."
        else:
            code = "green"
            reason = "Conectado por atividade recente em topicos monitorados."

        return {
            "code": code,
            "label": STATUS_LABELS[code],
            "rank": STATUS_RANK[code],
            "reason": reason,
            "ping_ok": ping_ok,
            "ping_age_sec": ping_age_sec,
            "ping_window_sec": ping_window,
            "cloudv2_ok": cloudv2_ok,
            "cloudv2_age_sec": cloudv2_age_sec,
            "cloudv2_window_sec": cloudv2_window,
            "median_ready": median_ready,
            "median_interval_sec": median_interval_sec,
            "sample_count": sample_count,
            "expected_by_topic_sec": expected_by_topic,
            "max_expected_interval_sec": max_expected_interval_sec,
            "disconnect_threshold_sec": disconnect_threshold_sec,
            "last_monitored_message_ts": last_monitored_message_ts,
            "last_monitored_message_age_sec": monitored_age_sec,
            "disconnected_by_inactivity": disconnected_by_inactivity,
            "attention_disconnected_pct": attention_disconnected_pct,
            "attention_disconnected_pct_threshold": self.attention_disconnected_pct_threshold,
            "attention_window_sec": min(self.attention_disconnected_window_sec, self.retention_sec),
            "attention_by_disconnected_pct": attention_by_disconnected_pct,
        }

    def _refresh_status_locked(self, pivot, now):
        computed = self._compute_status_locked(pivot, now)
        cached = pivot.get("status_cache", {})
        previous_code = cached.get("code")
        previous_reason = cached.get("reason")

        changed = (computed["code"] != previous_code) or (computed["reason"] != previous_reason)
        if not changed:
            return False

        pivot["status_cache"] = {
            "code": computed["code"],
            "reason": computed["reason"],
            "changed_at_ts": now,
        }

        if previous_code != computed["code"]:
            self.log.info(
                "Mudanca de status: pivot_id=%s %s -> %s (%s)",
                pivot["pivot_id"],
                previous_code or "-",
                computed["code"],
                computed["reason"],
            )

        return True

    def _build_state_snapshot_locked(self, now):
        pivots = [
            self._build_pivot_summary_locked(pivot, now)
            for _, pivot in sorted(self.pivots.items(), key=lambda item: item[0].lower())
        ]

        pending_ping = []
        for pivot_id, entry in sorted(self.pending_ping_unknown.items(), key=lambda item: item[0].lower()):
            pending_ping.append(
                {
                    "pivot_id": pivot_id,
                    "count": int(entry.get("count", 0)),
                    "first_seen_ts": entry.get("first_seen_ts"),
                    "first_seen_at": _ts_to_str(entry.get("first_seen_ts")),
                    "last_seen_ts": entry.get("last_seen_ts"),
                    "last_seen_at": _ts_to_str(entry.get("last_seen_ts")),
                    "last_payload_excerpt": entry.get("last_payload_excerpt", ""),
                }
            )

        malformed_recent = [
            {
                "ts": item.get("ts"),
                "at": item.get("at"),
                "topic": item.get("topic"),
                "reason": item.get("reason"),
                "payload_excerpt": item.get("payload_excerpt"),
            }
            for item in self.malformed_messages[-50:]
        ]

        return {
            "updated_at": _ts_to_str(now),
            "updated_at_ts": now,
            "settings": {
                "monitor_topics": list(self.monitor_topics),
                "connectivity_topics": list(CONNECTIVITY_TOPICS),
                "tolerance_factor": self.tolerance_factor,
                "ping_expected_sec": self.ping_expected_sec,
                "attention_disconnected_pct_threshold": self.attention_disconnected_pct_threshold,
                "attention_disconnected_window_hours": self.attention_disconnected_window_hours,
                "cloudv2_median_window": self.cloudv2_window,
                "cloudv2_min_samples": self.cloudv2_min_samples,
                "show_pending_ping_pivots": self.show_pending_ping_pivots,
                "probe_timeout_streak_alert": self.probe_timeout_streak_alert,
            },
            "counts": {
                "pivots": len(pivots),
                "pending_ping_unknown": len(pending_ping),
                "malformed_messages": len(self.malformed_messages),
                "duplicate_drops": self.duplicate_count,
            },
            "pivots": pivots,
            "pending_ping": pending_ping,
            "malformed_recent": malformed_recent,
        }

    def _summarize_probe_stats_locked(self, probe):
        events = probe.get("events")
        if not isinstance(events, list):
            events = []

        sent_count = 0
        response_count = 0
        timeout_count = 0
        latency_samples = []

        for event in events:
            if not isinstance(event, dict):
                continue

            event_type = str(event.get("type") or "").strip().lower()
            if event_type == "sent":
                sent_count += 1
                continue
            if event_type == "timeout":
                timeout_count += 1
                continue
            if event_type != "response":
                continue

            response_count += 1
            latency = _safe_float(event.get("latency_sec"), None)
            if latency is not None and latency >= 0:
                latency_samples.append(latency)

        return {
            "sent_count": sent_count,
            "response_count": response_count,
            "timeout_count": timeout_count,
            "response_ratio_pct": ((response_count / sent_count) * 100.0) if sent_count > 0 else None,
            "latency_sample_count": len(latency_samples),
            "latency_last_sec": latency_samples[-1] if latency_samples else None,
            "latency_avg_sec": (sum(latency_samples) / len(latency_samples)) if latency_samples else None,
            "latency_median_sec": statistics.median(latency_samples) if latency_samples else None,
            "latency_min_sec": min(latency_samples) if latency_samples else None,
            "latency_max_sec": max(latency_samples) if latency_samples else None,
        }

    def _build_pivot_summary_locked(self, pivot, now):
        status = self._compute_status_locked(pivot, now)
        probe = pivot["probe"]
        probe_stats = self._summarize_probe_stats_locked(probe)
        last_activity_ts = max(
            [
                _safe_float(pivot.get("last_seen_ts"), 0) or 0,
                _safe_float(pivot.get("last_ping_ts"), 0) or 0,
                _safe_float(pivot.get("last_cloudv2_ts"), 0) or 0,
                _safe_float(pivot.get("last_cloud2_ts"), 0) or 0,
                _safe_float(probe.get("last_sent_ts"), 0) or 0,
                _safe_float(probe.get("last_response_ts"), 0) or 0,
            ]
        )

        probe_alert = int(probe.get("timeout_streak", 0)) >= self.probe_timeout_streak_alert

        return {
            "pivot_id": pivot["pivot_id"],
            "pivot_slug": pivot["pivot_slug"],
            "status": {
                "code": status["code"],
                "label": status["label"],
                "rank": status["rank"],
                "reason": status["reason"],
            },
            "ping_ok": status["ping_ok"],
            "cloudv2_ok": status["cloudv2_ok"],
            "median_ready": status["median_ready"],
            "median_cloudv2_interval_sec": status["median_interval_sec"],
            "median_sample_count": status["sample_count"],
            "max_expected_interval_sec": status["max_expected_interval_sec"],
            "disconnect_threshold_sec": status["disconnect_threshold_sec"],
            "disconnected_by_inactivity": status["disconnected_by_inactivity"],
            "attention_disconnected_pct": status["attention_disconnected_pct"],
            "attention_disconnected_pct_threshold": status["attention_disconnected_pct_threshold"],
            "attention_window_sec": status["attention_window_sec"],
            "attention_by_disconnected_pct": status["attention_by_disconnected_pct"],
            "last_monitored_message_ts": status["last_monitored_message_ts"],
            "last_monitored_message_at": _ts_to_str(status["last_monitored_message_ts"]),
            "last_monitored_message_age_sec": status["last_monitored_message_age_sec"],
            "expected_by_topic_sec": status["expected_by_topic_sec"],
            "last_ping_ts": pivot.get("last_ping_ts"),
            "last_ping_at": _ts_to_str(pivot.get("last_ping_ts")),
            "last_cloudv2_ts": pivot.get("last_cloudv2_ts"),
            "last_cloudv2_at": _ts_to_str(pivot.get("last_cloudv2_ts")),
            "last_cloud2": pivot.get("last_cloud2"),
            "last_activity_ts": last_activity_ts if last_activity_ts > 0 else None,
            "last_activity_at": _ts_to_str(last_activity_ts) if last_activity_ts > 0 else "-",
            "last_activity_ago": _format_ago(now - last_activity_ts) if last_activity_ts > 0 else "-",
            "topic_counters": dict(pivot.get("topic_counters", {})),
            "probe": {
                "enabled": bool(probe.get("enabled")),
                "interval_sec": int(probe.get("interval_sec", self.probe_default_interval_sec)),
                "last_sent_ts": probe.get("last_sent_ts"),
                "last_sent_at": _ts_to_str(probe.get("last_sent_ts")),
                "last_response_ts": probe.get("last_response_ts"),
                "last_response_at": _ts_to_str(probe.get("last_response_ts")),
                "pending": probe.get("pending_sent_ts") is not None,
                "pending_deadline_ts": probe.get("pending_deadline_ts"),
                "pending_deadline_at": _ts_to_str(probe.get("pending_deadline_ts")),
                "timeout_streak": int(probe.get("timeout_streak", 0)),
                "last_result": probe.get("last_result"),
                "alert": probe_alert,
                "sent_count": int(probe_stats["sent_count"]),
                "response_count": int(probe_stats["response_count"]),
                "timeout_count": int(probe_stats["timeout_count"]),
                "response_ratio_pct": probe_stats["response_ratio_pct"],
                "latency_sample_count": int(probe_stats["latency_sample_count"]),
                "latency_last_sec": probe_stats["latency_last_sec"],
                "latency_avg_sec": probe_stats["latency_avg_sec"],
                "latency_median_sec": probe_stats["latency_median_sec"],
                "latency_min_sec": probe_stats["latency_min_sec"],
                "latency_max_sec": probe_stats["latency_max_sec"],
            },
        }

    def _build_pivot_snapshot_locked(self, pivot, now):
        summary = self._build_pivot_summary_locked(pivot, now)

        drop_events = list(pivot.get("drop_events", []))
        drops_24h = [item for item in drop_events if _safe_float(item.get("ts"), 0) >= (now - 86400)]
        drops_7d = [item for item in drop_events if _safe_float(item.get("ts"), 0) >= (now - 604800)]

        last_drop = drop_events[-1] if drop_events else None
        last_cloud2 = pivot.get("last_cloud2") or {}

        timeline = sorted(
            list(pivot.get("timeline", [])),
            key=lambda item: _safe_float(item.get("ts"), 0),
            reverse=True,
        )

        probe_events = sorted(
            list(pivot.get("probe", {}).get("events", [])),
            key=lambda item: _safe_float(item.get("ts"), 0),
            reverse=True,
        )

        return {
            "pivot_id": pivot["pivot_id"],
            "pivot_slug": pivot["pivot_slug"],
            "updated_at": _ts_to_str(now),
            "updated_at_ts": now,
            "summary": summary,
            "metrics": {
                "drops_24h": len(drops_24h),
                "drops_7d": len(drops_7d),
                "last_drop_duration_sec": (last_drop or {}).get("duration_sec"),
                "last_drop_at": (last_drop or {}).get("at", "-"),
                "last_rssi": last_cloud2.get("rssi"),
                "last_technology": last_cloud2.get("technology"),
                "last_firmware": last_cloud2.get("firmware"),
                "last_cloud2_event_date": last_cloud2.get("event_date"),
                "last_cloud2_at": last_cloud2.get("at", "-"),
            },
            "timeline": timeline,
            "probe_events": probe_events,
            "cloud2_events": sorted(
                list(pivot.get("cloud2_events", [])),
                key=lambda item: _safe_float(item.get("ts"), 0),
                reverse=True,
            ),
        }

    def _build_runtime_payload_locked(self, now):
        payload = {
            "version": 2,
            "updated_at": _ts_to_str(now),
            "updated_at_ts": now,
            "event_seq": self._event_seq,
            "probe_settings": self._probe_settings,
            "pending_ping_unknown": self.pending_ping_unknown,
            "malformed_messages": self.malformed_messages,
            "duplicate_count": self.duplicate_count,
            "pivots": self.pivots,
        }

        # Garante serializacao JSON sem referencias compartilhadas mutaveis.
        return json.loads(json.dumps(payload, ensure_ascii=False))

    def _load_runtime_state(self):
        if not os.path.exists(self.runtime_path):
            return

        try:
            with open(self.runtime_path, "r", encoding="utf-8") as file:
                loaded = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            self.log.warning("Nao foi possivel restaurar runtime_store.json: %s", exc)
            return

        if not isinstance(loaded, dict):
            return

        with self._lock:
            loaded_probe = loaded.get("probe_settings")
            if isinstance(loaded_probe, dict):
                self._probe_settings = self._normalize_probe_settings(loaded_probe)

            loaded_pivots = loaded.get("pivots", {})
            if isinstance(loaded_pivots, dict):
                restored = {}
                for pivot_id, raw_pivot in loaded_pivots.items():
                    normalized_pivot_id = str(pivot_id or "").strip()
                    if not normalized_pivot_id or not isinstance(raw_pivot, dict):
                        continue

                    discovered_ts = _safe_float(raw_pivot.get("discovered_at_ts"), time.time())
                    pivot = self._new_pivot_state(normalized_pivot_id, discovered_ts)

                    for field in (
                        "last_seen_ts",
                        "last_ping_ts",
                        "last_cloudv2_ts",
                        "last_cloud2_ts",
                        "discovered_at_ts",
                    ):
                        value = _safe_float(raw_pivot.get(field), None)
                        pivot[field] = value

                    topic_counters = raw_pivot.get("topic_counters")
                    if isinstance(topic_counters, dict):
                        for topic in self.monitor_topics:
                            pivot["topic_counters"][topic] = int(topic_counters.get(topic, 0))

                    raw_topic_last = raw_pivot.get("topic_last_ts")
                    if isinstance(raw_topic_last, dict):
                        for topic in CONNECTIVITY_TOPICS:
                            pivot["topic_last_ts"][topic] = _safe_float(raw_topic_last.get(topic), None)

                    raw_topic_intervals = raw_pivot.get("topic_intervals_sec")
                    if isinstance(raw_topic_intervals, dict):
                        for topic in CONNECTIVITY_TOPICS:
                            values = raw_topic_intervals.get(topic)
                            if not isinstance(values, list):
                                continue
                            cleaned = []
                            for item in values:
                                parsed_interval = _safe_float(item, None)
                                if parsed_interval is not None and parsed_interval > 0:
                                    cleaned.append(parsed_interval)
                            pivot["topic_intervals_sec"][topic] = cleaned[-self.cloudv2_window :]

                    intervals = raw_pivot.get("cloudv2_intervals_sec")
                    if isinstance(intervals, list):
                        cleaned_intervals = []
                        for item in intervals:
                            parsed = _safe_float(item, None)
                            if parsed is not None and parsed > 0:
                                cleaned_intervals.append(parsed)
                        pivot["cloudv2_intervals_sec"] = cleaned_intervals[-self.cloudv2_window :]

                    # Compatibilidade entre estado legado e estrutura por topico.
                    if not pivot["topic_intervals_sec"].get(TOPIC_CLOUDV2):
                        pivot["topic_intervals_sec"][TOPIC_CLOUDV2] = list(pivot["cloudv2_intervals_sec"])
                    else:
                        pivot["cloudv2_intervals_sec"] = list(pivot["topic_intervals_sec"][TOPIC_CLOUDV2])
                    if pivot.get("last_cloudv2_ts") is not None:
                        pivot["topic_last_ts"][TOPIC_CLOUDV2] = _safe_float(pivot.get("last_cloudv2_ts"), None)
                    if pivot.get("last_ping_ts") is not None:
                        pivot["topic_last_ts"][TOPIC_PING] = _safe_float(pivot.get("last_ping_ts"), None)

                    last_cloud2 = raw_pivot.get("last_cloud2")
                    if isinstance(last_cloud2, dict):
                        pivot["last_cloud2"] = last_cloud2

                    for list_field in ("cloud2_events", "drop_events", "timeline"):
                        raw_list = raw_pivot.get(list_field)
                        if isinstance(raw_list, list):
                            pivot[list_field] = raw_list[-self.max_events_per_pivot :]

                    raw_probe = raw_pivot.get("probe")
                    if isinstance(raw_probe, dict):
                        probe = pivot["probe"]
                        probe["enabled"] = bool(raw_probe.get("enabled", probe["enabled"]))
                        probe_interval = _safe_int(raw_probe.get("interval_sec"), probe["interval_sec"])
                        if probe_interval is not None and probe_interval >= self.probe_min_interval_sec:
                            probe["interval_sec"] = probe_interval
                        probe["last_sent_ts"] = _safe_float(raw_probe.get("last_sent_ts"), None)
                        probe["last_response_ts"] = _safe_float(raw_probe.get("last_response_ts"), None)
                        probe["pending_sent_ts"] = _safe_float(raw_probe.get("pending_sent_ts"), None)
                        probe["pending_deadline_ts"] = _safe_float(raw_probe.get("pending_deadline_ts"), None)
                        probe["timeout_streak"] = _safe_int(raw_probe.get("timeout_streak"), 0) or 0
                        probe["last_result"] = raw_probe.get("last_result")
                        if isinstance(raw_probe.get("events"), list):
                            probe["events"] = raw_probe.get("events")[-self.max_events_per_pivot :]

                    raw_status = raw_pivot.get("status_cache")
                    if isinstance(raw_status, dict):
                        pivot["status_cache"] = {
                            "code": str(raw_status.get("code") or "gray"),
                            "reason": str(raw_status.get("reason") or ""),
                            "changed_at_ts": _safe_float(raw_status.get("changed_at_ts"), discovered_ts),
                        }

                    restored[normalized_pivot_id] = pivot

                self.pivots = restored

            pending_ping = loaded.get("pending_ping_unknown")
            if isinstance(pending_ping, dict):
                self.pending_ping_unknown = pending_ping

            malformed_messages = loaded.get("malformed_messages")
            if isinstance(malformed_messages, list):
                self.malformed_messages = malformed_messages[-500:]

            self.duplicate_count = _safe_int(loaded.get("duplicate_count"), 0) or 0
            self._event_seq = _safe_int(loaded.get("event_seq"), 0) or 0

            now = time.time()
            for pivot in self.pivots.values():
                self._prune_pivot_locked(pivot, now)
                self._refresh_status_locked(pivot, now)
            self._cleanup_pending_ping_locked(now)

        self.log.info("Estado de runtime restaurado com %s pivots.", len(self.pivots))

    def _clear_dashboard_data_files(self):
        if not os.path.isdir(DATA_DIR):
            return

        for name in os.listdir(DATA_DIR):
            if not name.endswith(".json"):
                continue
            path = os.path.join(DATA_DIR, name)
            try:
                os.remove(path)
            except OSError:
                pass
