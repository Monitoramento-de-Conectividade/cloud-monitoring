import json
import os
import re
import sqlite3
import statistics
import threading
import time
import uuid
from datetime import datetime

from backend.cloudv2_paths import resolve_data_dir
from backend.cloudv2_dashboard import slugify


DEFAULT_DB_PATH = os.path.join(resolve_data_dir(), "telemetry.sqlite3")
DEFAULT_MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")
PROBE_STATS_WINDOW_SEC = 30 * 24 * 3600


def _ts_to_str(ts):
    if ts is None:
        return "-"
    return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


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


def _safe_bool(value, default=None):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "sim"}:
            return True
        if normalized in {"0", "false", "no", "n", "nao", "não"}:
            return False
    return default


class TelemetryPersistence:
    def __init__(self, db_path=None, migrations_dir=None, max_events_per_pivot=5000, log=None):
        self.db_path = str(db_path or DEFAULT_DB_PATH)
        self.migrations_dir = str(migrations_dir or DEFAULT_MIGRATIONS_DIR)
        self.max_events_per_pivot = max(100, int(max_events_per_pivot or 5000))
        self.log = log

        self._lock = threading.RLock()
        self._conn = None

    def start(self):
        with self._lock:
            if self._conn is not None:
                return

            directory = os.path.dirname(self.db_path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 3000")

            self._conn = conn
            self._ensure_migrations_table_locked()
            self._apply_migrations_locked()

    def stop(self):
        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _require_conn_locked(self):
        if self._conn is None:
            raise RuntimeError("Persistence not started")
        return self._conn

    def _ensure_migrations_table_locked(self):
        conn = self._require_conn_locked()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at_ts REAL NOT NULL
                )
                """
            )

    def _iter_migration_files_locked(self):
        if not os.path.isdir(self.migrations_dir):
            return []

        entries = []
        for filename in os.listdir(self.migrations_dir):
            if not filename.lower().endswith(".sql"):
                continue

            match = re.match(r"^(\d+)[_-](.+)\.sql$", filename)
            if not match:
                continue

            version = int(match.group(1))
            name = match.group(2).replace("_", " ")
            path = os.path.join(self.migrations_dir, filename)
            entries.append((version, name, path))

        entries.sort(key=lambda item: item[0])
        return entries

    def _apply_migrations_locked(self):
        conn = self._require_conn_locked()
        rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
        applied = {int(row["version"]) for row in rows}

        pending = [item for item in self._iter_migration_files_locked() if item[0] not in applied]
        if not pending:
            return

        for version, name, path in pending:
            with open(path, "r", encoding="utf-8") as file:
                script = file.read()
            with conn:
                conn.executescript(script)
                conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at_ts) VALUES (?, ?, ?)",
                    (version, name, time.time()),
                )
            if self.log is not None:
                self.log.info("Migration aplicada: v%s (%s)", version, name)

    def _upsert_pivot_locked(self, conn, pivot_id, pivot_slug, seen_ts=None):
        now_ts = time.time()
        seen_value = _safe_float(seen_ts, None)

        with conn:
            conn.execute(
                """
                INSERT INTO pivots (
                    pivot_id,
                    pivot_slug,
                    first_seen_ts,
                    last_seen_ts,
                    created_at_ts,
                    updated_at_ts
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(pivot_id) DO UPDATE SET
                    pivot_slug = excluded.pivot_slug,
                    first_seen_ts = CASE
                        WHEN pivots.first_seen_ts IS NULL THEN excluded.first_seen_ts
                        WHEN excluded.first_seen_ts IS NULL THEN pivots.first_seen_ts
                        WHEN excluded.first_seen_ts < pivots.first_seen_ts THEN excluded.first_seen_ts
                        ELSE pivots.first_seen_ts
                    END,
                    last_seen_ts = CASE
                        WHEN pivots.last_seen_ts IS NULL THEN excluded.last_seen_ts
                        WHEN excluded.last_seen_ts IS NULL THEN pivots.last_seen_ts
                        WHEN excluded.last_seen_ts > pivots.last_seen_ts THEN excluded.last_seen_ts
                        ELSE pivots.last_seen_ts
                    END,
                    updated_at_ts = excluded.updated_at_ts
                """,
                (
                    str(pivot_id),
                    str(pivot_slug),
                    seen_value,
                    seen_value,
                    now_ts,
                    now_ts,
                ),
            )

    def ensure_pivot(self, pivot_id, pivot_slug=None, seen_ts=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return
        normalized_slug = str(pivot_slug or slugify(normalized_id))
        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, seen_ts)

    def touch_pivot_seen(self, pivot_id, seen_ts):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return
        seen_value = _safe_float(seen_ts, None)
        if seen_value is None:
            seen_value = time.time()
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    UPDATE pivots
                    SET
                        last_seen_ts = CASE
                            WHEN last_seen_ts IS NULL THEN ?
                            WHEN ? > last_seen_ts THEN ?
                            ELSE last_seen_ts
                        END,
                        updated_at_ts = ?
                    WHERE pivot_id = ?
                    """,
                    (
                        seen_value,
                        seen_value,
                        seen_value,
                        time.time(),
                        normalized_id,
                    ),
                )

    def _query_run_row_locked(self, conn, run_id=None):
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            return conn.execute(
                """
                SELECT *
                FROM monitoring_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (normalized_run,),
            ).fetchone()

        active_row = conn.execute(
            """
            SELECT *
            FROM monitoring_runs
            WHERE is_active = 1
            ORDER BY updated_at_ts DESC, started_at_ts DESC
            LIMIT 1
            """
        ).fetchone()
        if active_row is not None:
            return active_row

        return conn.execute(
            """
            SELECT *
            FROM monitoring_runs
            ORDER BY started_at_ts DESC, updated_at_ts DESC
            LIMIT 1
            """
        ).fetchone()

    def _row_to_run_dict_locked(self, row, now_ts=None):
        if row is None:
            return None

        now_value = _safe_float(now_ts, None)
        if now_value is None:
            now_value = time.time()

        started_at_ts = _safe_float(row["started_at_ts"], None)
        ended_at_ts = _safe_float(row["ended_at_ts"], None)
        updated_at_ts = _safe_float(row["updated_at_ts"], None)
        duration_anchor = ended_at_ts if ended_at_ts is not None else now_value
        duration_sec = None
        if started_at_ts is not None:
            duration_sec = max(0.0, duration_anchor - started_at_ts)

        return {
            "run_id": str(row["run_id"]),
            "started_at_ts": started_at_ts,
            "started_at": _ts_to_str(started_at_ts),
            "ended_at_ts": ended_at_ts,
            "ended_at": _ts_to_str(ended_at_ts),
            "updated_at_ts": updated_at_ts,
            "updated_at": _ts_to_str(updated_at_ts),
            "is_active": bool(int(row["is_active"])),
            "source": str(row["source"] or ""),
            "label": str(row["label"] or ""),
            "duration_sec": duration_sec,
        }

    def resolve_run(self, run_id=None):
        with self._lock:
            conn = self._require_conn_locked()
            row = self._query_run_row_locked(conn, run_id=run_id)
            return self._row_to_run_dict_locked(row)

    def get_or_create_active_run(self, now_ts=None, source="runtime", label="", metadata=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            row = self._query_run_row_locked(conn, run_id=None)
            if row is not None and bool(int(row["is_active"])):
                return self._row_to_run_dict_locked(row, now_ts=current_ts)

            run_id = str(uuid.uuid4())
            metadata_payload = metadata if isinstance(metadata, dict) else {}
            with conn:
                conn.execute(
                    """
                    INSERT INTO monitoring_runs (
                        run_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        current_ts,
                        str(source or "runtime"),
                        str(label or ""),
                        json.dumps(metadata_payload, ensure_ascii=False),
                        current_ts,
                        current_ts,
                    ),
                )

            created = conn.execute(
                "SELECT * FROM monitoring_runs WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            return self._row_to_run_dict_locked(created, now_ts=current_ts)

    def create_new_run(self, now_ts=None, source="ui", label="", metadata=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            run_id = str(uuid.uuid4())
            metadata_payload = metadata if isinstance(metadata, dict) else {}
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                    """,
                    (current_ts, current_ts),
                )
                conn.execute(
                    """
                    INSERT INTO monitoring_runs (
                        run_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        current_ts,
                        str(source or "ui"),
                        str(label or ""),
                        json.dumps(metadata_payload, ensure_ascii=False),
                        current_ts,
                        current_ts,
                    ),
                )

            row = conn.execute(
                "SELECT * FROM monitoring_runs WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            return self._row_to_run_dict_locked(row, now_ts=current_ts)

    def activate_existing_run(self, run_id, now_ts=None):
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return None

        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=normalized_run_id)
            if run_row is None:
                return None

            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                        AND run_id <> ?
                    """,
                    (current_ts, current_ts, normalized_run_id),
                )
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 1,
                        ended_at_ts = NULL,
                        updated_at_ts = ?
                    WHERE run_id = ?
                    """,
                    (current_ts, normalized_run_id),
                )

            row = conn.execute(
                "SELECT * FROM monitoring_runs WHERE run_id = ? LIMIT 1",
                (normalized_run_id,),
            ).fetchone()
            return self._row_to_run_dict_locked(row, now_ts=current_ts)

    def activate_latest_sessions_for_run(self, run_id, now_ts=None):
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return {}

        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=normalized_run_id)
            if run_row is None:
                return {}

            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                    """,
                    (current_ts, current_ts),
                )

                latest_rows = conn.execute(
                    """
                    SELECT s.session_id
                    FROM monitoring_sessions AS s
                    WHERE s.run_id = ?
                        AND s.session_id = (
                            SELECT s2.session_id
                            FROM monitoring_sessions AS s2
                            WHERE s2.run_id = s.run_id
                                AND s2.pivot_id = s.pivot_id
                            ORDER BY s2.updated_at_ts DESC, s2.started_at_ts DESC
                            LIMIT 1
                        )
                    GROUP BY s.pivot_id
                    """,
                    (normalized_run_id,),
                ).fetchall()

                for row in latest_rows:
                    session_id = str(row["session_id"] or "").strip()
                    if not session_id:
                        continue
                    conn.execute(
                        """
                        UPDATE monitoring_sessions
                        SET
                            is_active = 1,
                            ended_at_ts = NULL,
                            updated_at_ts = ?
                        WHERE session_id = ?
                        """,
                        (current_ts, session_id),
                    )

            rows = conn.execute(
                """
                SELECT pivot_id, session_id
                FROM monitoring_sessions
                WHERE run_id = ?
                    AND is_active = 1
                ORDER BY updated_at_ts DESC, started_at_ts DESC
                """,
                (normalized_run_id,),
            ).fetchall()

            active_by_pivot = {}
            for row in rows:
                pivot_id = str(row["pivot_id"] or "").strip()
                session_id = str(row["session_id"] or "").strip()
                if not pivot_id or not session_id:
                    continue
                if pivot_id not in active_by_pivot:
                    active_by_pivot[pivot_id] = session_id
            return active_by_pivot

    def list_runs(self, limit=200):
        safe_limit = max(1, min(1000, int(limit or 200)))
        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT
                    runs.*,
                    COUNT(sessions.session_id) AS session_count,
                    COUNT(DISTINCT sessions.pivot_id) AS pivot_count
                FROM monitoring_runs AS runs
                LEFT JOIN monitoring_sessions AS sessions
                    ON sessions.run_id = runs.run_id
                GROUP BY runs.run_id
                ORDER BY runs.started_at_ts DESC, runs.updated_at_ts DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()

            items = []
            for row in rows:
                item = self._row_to_run_dict_locked(row)
                if item is None:
                    continue
                item["session_count"] = int(row["session_count"] or 0)
                item["pivot_count"] = int(row["pivot_count"] or 0)
                items.append(item)
            return items

    def _resolve_effective_run_id_locked(self, conn, run_id=None):
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            return normalized_run
        run_row = self._query_run_row_locked(conn, run_id=None)
        if run_row is None:
            return None
        resolved = str(run_row["run_id"] or "").strip()
        return resolved or None

    def _list_distinct_cloud2_column_locked(self, conn, column_name, run_id=None, limit=500):
        if column_name not in ("technology", "firmware"):
            return []

        safe_limit = max(1, min(2000, int(limit or 500)))
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            rows = conn.execute(
                f"""
                SELECT DISTINCT TRIM(cloud2.{column_name}) AS value
                FROM cloud2_events AS cloud2
                INNER JOIN monitoring_sessions AS sessions
                    ON sessions.session_id = cloud2.session_id
                WHERE sessions.run_id = ?
                    AND cloud2.{column_name} IS NOT NULL
                    AND TRIM(cloud2.{column_name}) <> ''
                ORDER BY value COLLATE NOCASE ASC
                LIMIT ?
                """,
                (normalized_run, safe_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT DISTINCT TRIM({column_name}) AS value
                FROM cloud2_events
                WHERE {column_name} IS NOT NULL
                    AND TRIM({column_name}) <> ''
                ORDER BY value COLLATE NOCASE ASC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()

        values = []
        for row in rows:
            value = str(row["value"] or "").strip()
            if value:
                values.append(value)
        return values

    def get_cloud2_filter_options(self, run_id=None, limit=500):
        with self._lock:
            conn = self._require_conn_locked()
            effective_run = self._resolve_effective_run_id_locked(conn, run_id=run_id)
            technologies = self._list_distinct_cloud2_column_locked(
                conn,
                "technology",
                run_id=effective_run,
                limit=limit,
            )
            firmwares = self._list_distinct_cloud2_column_locked(
                conn,
                "firmware",
                run_id=effective_run,
                limit=limit,
            )
            return {
                "run_id": effective_run,
                "technologies": technologies,
                "firmwares": firmwares,
            }

    def _query_session_row_locked(self, conn, pivot_id, session_id=None, run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        normalized_run_id = str(run_id or "").strip()
        if session_id:
            normalized_session = str(session_id).strip()
            if not normalized_session:
                return None
            query = """
                SELECT *
                FROM monitoring_sessions
                WHERE pivot_id = ? AND session_id = ?
            """
            params = [normalized_id, normalized_session]
            if normalized_run_id:
                query += " AND run_id = ?"
                params.append(normalized_run_id)
            query += " LIMIT 1"
            return conn.execute(query, tuple(params)).fetchone()

        active_query = """
            SELECT *
            FROM monitoring_sessions
            WHERE pivot_id = ? AND is_active = 1
        """
        active_params = [normalized_id]
        if normalized_run_id:
            active_query += " AND run_id = ?"
            active_params.append(normalized_run_id)
        active_query += " ORDER BY updated_at_ts DESC, started_at_ts DESC LIMIT 1"

        active_row = conn.execute(active_query, tuple(active_params)).fetchone()
        if active_row is not None:
            return active_row

        latest_query = """
            SELECT *
            FROM monitoring_sessions
            WHERE pivot_id = ?
        """
        latest_params = [normalized_id]
        if normalized_run_id:
            latest_query += " AND run_id = ?"
            latest_params.append(normalized_run_id)
        latest_query += " ORDER BY started_at_ts DESC, updated_at_ts DESC LIMIT 1"
        return conn.execute(latest_query, tuple(latest_params)).fetchone()

    def _row_to_session_dict_locked(self, row, now_ts=None):
        if row is None:
            return None

        now_value = _safe_float(now_ts, None)
        if now_value is None:
            now_value = time.time()

        started_at_ts = _safe_float(row["started_at_ts"], None)
        ended_at_ts = _safe_float(row["ended_at_ts"], None)
        updated_at_ts = _safe_float(row["updated_at_ts"], None)
        duration_anchor = ended_at_ts if ended_at_ts is not None else now_value
        duration_sec = None
        if started_at_ts is not None:
            duration_sec = max(0.0, duration_anchor - started_at_ts)

        return {
            "session_id": str(row["session_id"]),
            "pivot_id": str(row["pivot_id"]),
            "run_id": str(row["run_id"] or ""),
            "started_at_ts": started_at_ts,
            "started_at": _ts_to_str(started_at_ts),
            "ended_at_ts": ended_at_ts,
            "ended_at": _ts_to_str(ended_at_ts),
            "updated_at_ts": updated_at_ts,
            "updated_at": _ts_to_str(updated_at_ts),
            "is_active": bool(int(row["is_active"])),
            "source": str(row["source"] or ""),
            "label": str(row["label"] or ""),
            "duration_sec": duration_sec,
        }

    def resolve_session(self, pivot_id, session_id=None, run_id=None):
        with self._lock:
            conn = self._require_conn_locked()
            row = self._query_session_row_locked(conn, pivot_id, session_id=session_id, run_id=run_id)
            return self._row_to_session_dict_locked(row)

    def get_or_create_active_session(self, pivot_id, pivot_slug=None, now_ts=None, source="runtime", run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        normalized_slug = str(pivot_slug or slugify(normalized_id))
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, current_ts)
            normalized_run_id = str(run_id or "").strip()
            if normalized_run_id:
                resolved_run = self._query_run_row_locked(conn, run_id=normalized_run_id)
            else:
                resolved_run = self.get_or_create_active_run(now_ts=current_ts, source=source)
            if isinstance(resolved_run, sqlite3.Row):
                resolved_run_id = str(resolved_run["run_id"] or "")
            else:
                resolved_run_id = str((resolved_run or {}).get("run_id") or "")
            if not resolved_run_id:
                return None

            row = self._query_session_row_locked(conn, normalized_id, session_id=None, run_id=resolved_run_id)
            if row is not None and bool(int(row["is_active"])):
                return self._row_to_session_dict_locked(row, now_ts=current_ts)

            # Reaproveita a ultima sessao do pivô no run atual, evitando criar
            # uma nova sessao em cada restart quando ja existe historico valido.
            if row is not None:
                existing_session_id = str(row["session_id"] or "").strip()
                if existing_session_id:
                    with conn:
                        conn.execute(
                            """
                            UPDATE monitoring_sessions
                            SET
                                is_active = 0,
                                ended_at_ts = COALESCE(ended_at_ts, ?),
                                updated_at_ts = ?
                            WHERE pivot_id = ? AND is_active = 1 AND session_id <> ?
                            """,
                            (current_ts, current_ts, normalized_id, existing_session_id),
                        )
                        conn.execute(
                            """
                            UPDATE monitoring_sessions
                            SET
                                is_active = 1,
                                ended_at_ts = NULL,
                                updated_at_ts = ?
                            WHERE session_id = ?
                            """,
                            (current_ts, existing_session_id),
                        )
                    reused = conn.execute(
                        "SELECT * FROM monitoring_sessions WHERE session_id = ? LIMIT 1",
                        (existing_session_id,),
                    ).fetchone()
                    return self._row_to_session_dict_locked(reused, now_ts=current_ts)

            session_id = str(uuid.uuid4())
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE pivot_id = ? AND is_active = 1
                    """,
                    (current_ts, current_ts, normalized_id),
                )
                conn.execute(
                    """
                    INSERT INTO monitoring_sessions (
                        session_id,
                        run_id,
                        pivot_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, ?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        resolved_run_id,
                        normalized_id,
                        current_ts,
                        str(source or "runtime"),
                        "",
                        "{}",
                        current_ts,
                        current_ts,
                    ),
                )

            created = conn.execute(
                "SELECT * FROM monitoring_sessions WHERE session_id = ? LIMIT 1",
                (session_id,),
            ).fetchone()
            return self._row_to_session_dict_locked(created, now_ts=current_ts)

    def create_new_session(self, pivot_id, pivot_slug=None, now_ts=None, source="ui", run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        normalized_slug = str(pivot_slug or slugify(normalized_id))
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, current_ts)
            normalized_run_id = str(run_id or "").strip()
            if normalized_run_id:
                resolved_run = self._query_run_row_locked(conn, run_id=normalized_run_id)
            else:
                resolved_run = self.get_or_create_active_run(now_ts=current_ts, source=source)
            if isinstance(resolved_run, sqlite3.Row):
                resolved_run_id = str(resolved_run["run_id"] or "")
            else:
                resolved_run_id = str((resolved_run or {}).get("run_id") or "")
            if not resolved_run_id:
                return None

            session_id = str(uuid.uuid4())
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE pivot_id = ? AND is_active = 1
                    """,
                    (current_ts, current_ts, normalized_id),
                )
                conn.execute(
                    """
                    INSERT INTO monitoring_sessions (
                        session_id,
                        run_id,
                        pivot_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, ?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        resolved_run_id,
                        normalized_id,
                        current_ts,
                        str(source or "ui"),
                        "",
                        "{}",
                        current_ts,
                        current_ts,
                    ),
                )

            row = conn.execute(
                "SELECT * FROM monitoring_sessions WHERE session_id = ? LIMIT 1",
                (session_id,),
            ).fetchone()
            return self._row_to_session_dict_locked(row, now_ts=current_ts)

    def list_sessions(self, pivot_id, limit=200, run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return []
        safe_limit = max(1, min(1000, int(limit or 200)))
        normalized_run_id = str(run_id or "").strip()

        with self._lock:
            conn = self._require_conn_locked()
            query = """
                SELECT *
                FROM monitoring_sessions
                WHERE pivot_id = ?
            """
            params = [normalized_id]
            if normalized_run_id:
                query += " AND run_id = ?"
                params.append(normalized_run_id)
            query += " ORDER BY started_at_ts DESC, updated_at_ts DESC LIMIT ?"
            params.append(safe_limit)
            rows = conn.execute(query, tuple(params)).fetchall()
            return [self._row_to_session_dict_locked(row) for row in rows]

    def get_active_sessions_map(self, run_id=None):
        normalized_run_id = str(run_id or "").strip()
        with self._lock:
            conn = self._require_conn_locked()
            query = """
                SELECT pivot_id, session_id
                FROM monitoring_sessions
                WHERE is_active = 1
            """
            params = []
            if normalized_run_id:
                query += " AND run_id = ?"
                params.append(normalized_run_id)
            query += " ORDER BY updated_at_ts DESC, started_at_ts DESC"
            rows = conn.execute(query, tuple(params)).fetchall()
            result = {}
            for row in rows:
                pivot_id = str(row["pivot_id"] or "").strip()
                session_id = str(row["session_id"] or "").strip()
                if not pivot_id or not session_id:
                    continue
                if pivot_id not in result:
                    result[pivot_id] = session_id
            return result

    def deactivate_all_active_sessions(self, now_ts=None, run_id=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()
        normalized_run_id = str(run_id or "").strip()
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                query = """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                """
                params = [current_ts, current_ts]
                if normalized_run_id:
                    query += " AND run_id = ?"
                    params.append(normalized_run_id)
                conn.execute(query, tuple(params))

    def deactivate_all_active_runs(self, now_ts=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                    """,
                    (current_ts, current_ts),
                )

    def purge_all_data(self):
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute("DELETE FROM connectivity_events")
                conn.execute("DELETE FROM probe_events")
                conn.execute("DELETE FROM probe_delay_points")
                conn.execute("DELETE FROM cloud2_events")
                conn.execute("DELETE FROM drop_events")
                conn.execute("DELETE FROM pivot_snapshots")
                conn.execute("DELETE FROM monitoring_sessions")
                conn.execute("DELETE FROM monitoring_runs")
                conn.execute("DELETE FROM probe_settings")
                conn.execute("DELETE FROM pivots")
                conn.execute(
                    """
                    DELETE FROM sqlite_sequence
                    WHERE name IN (
                        'connectivity_events',
                        'probe_events',
                        'probe_delay_points',
                        'cloud2_events',
                        'drop_events'
                    )
                    """
                )

    def delete_pivot(self, pivot_id):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return False

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                row = conn.execute(
                    "SELECT 1 FROM pivots WHERE pivot_id = ? LIMIT 1",
                    (normalized_id,),
                ).fetchone()
                conn.execute(
                    "DELETE FROM probe_settings WHERE pivot_id = ?",
                    (normalized_id,),
                )
                conn.execute(
                    "DELETE FROM pivots WHERE pivot_id = ?",
                    (normalized_id,),
                )
            return row is not None

    def _json_dumps(self, value):
        return json.dumps(value, ensure_ascii=False)

    def _json_loads(self, value, fallback):
        if value is None:
            return fallback
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return fallback

    def upsert_snapshot(self, pivot_id, session_id, snapshot_payload, updated_at_ts=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        snapshot = snapshot_payload if isinstance(snapshot_payload, dict) else {}
        summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
        status = summary.get("status") if isinstance(summary.get("status"), dict) else {}
        quality = summary.get("quality") if isinstance(summary.get("quality"), dict) else {}

        status_code = str(status.get("code") or "")
        quality_code = str(quality.get("code") or "")
        last_activity_ts = _safe_float(summary.get("last_activity_ts"), None)
        last_seen_ts = _safe_float(summary.get("last_monitored_message_ts"), None)
        median_ready_value = _safe_bool(summary.get("median_ready"), None)
        median_ready = None if median_ready_value is None else int(median_ready_value)
        median_sample_count = _safe_int(summary.get("median_sample_count"), None)
        if median_sample_count is not None and median_sample_count < 0:
            median_sample_count = 0
        median_cloudv2_interval_sec = _safe_float(summary.get("median_cloudv2_interval_sec"), None)
        disconnect_threshold_sec = _safe_float(summary.get("disconnect_threshold_sec"), None)
        ts_value = _safe_float(updated_at_ts, None)
        if ts_value is None:
            ts_value = _safe_float(snapshot.get("updated_at_ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO pivot_snapshots (
                        pivot_id,
                        session_id,
                        updated_at_ts,
                        status_code,
                        quality_code,
                        last_activity_ts,
                        last_seen_ts,
                        median_ready,
                        median_sample_count,
                        median_cloudv2_interval_sec,
                        disconnect_threshold_sec,
                        snapshot_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pivot_id, session_id) DO UPDATE SET
                        updated_at_ts = excluded.updated_at_ts,
                        status_code = excluded.status_code,
                        quality_code = excluded.quality_code,
                        last_activity_ts = excluded.last_activity_ts,
                        last_seen_ts = excluded.last_seen_ts,
                        median_ready = COALESCE(excluded.median_ready, pivot_snapshots.median_ready),
                        median_sample_count = COALESCE(excluded.median_sample_count, pivot_snapshots.median_sample_count),
                        median_cloudv2_interval_sec = COALESCE(excluded.median_cloudv2_interval_sec, pivot_snapshots.median_cloudv2_interval_sec),
                        disconnect_threshold_sec = COALESCE(excluded.disconnect_threshold_sec, pivot_snapshots.disconnect_threshold_sec),
                        snapshot_json = excluded.snapshot_json
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        status_code,
                        quality_code,
                        last_activity_ts,
                        last_seen_ts,
                        median_ready,
                        median_sample_count,
                        median_cloudv2_interval_sec,
                        disconnect_threshold_sec,
                        self._json_dumps(snapshot),
                    ),
                )
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET updated_at_ts = ?
                    WHERE session_id = ?
                    """,
                    (ts_value, normalized_session),
                )
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET updated_at_ts = ?
                    WHERE run_id = (
                        SELECT run_id
                        FROM monitoring_sessions
                        WHERE session_id = ?
                        LIMIT 1
                    )
                    """,
                    (ts_value, normalized_session),
                )
                conn.execute(
                    """
                    UPDATE pivots
                    SET
                        updated_at_ts = ?,
                        last_seen_ts = CASE
                            WHEN ? IS NULL THEN last_seen_ts
                            WHEN last_seen_ts IS NULL THEN ?
                            WHEN ? > last_seen_ts THEN ?
                            ELSE last_seen_ts
                        END
                    WHERE pivot_id = ?
                    """,
                    (
                        ts_value,
                        last_seen_ts,
                        last_seen_ts,
                        last_seen_ts,
                        last_seen_ts,
                        normalized_id,
                    ),
                )

    def has_snapshot(self, pivot_id, session_id):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return False
        with self._lock:
            conn = self._require_conn_locked()
            row = conn.execute(
                """
                SELECT 1
                FROM pivot_snapshots
                WHERE pivot_id = ? AND session_id = ?
                LIMIT 1
                """,
                (normalized_id, normalized_session),
            ).fetchone()
            return row is not None

    def session_has_events(self, pivot_id, session_id):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return False
        with self._lock:
            conn = self._require_conn_locked()
            checks = (
                "SELECT 1 FROM connectivity_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM probe_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM cloud2_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM drop_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
            )
            for query in checks:
                if conn.execute(query, (normalized_id, normalized_session)).fetchone() is not None:
                    return True
            return False

    def insert_connectivity_event(
        self,
        pivot_id,
        session_id,
        event,
        source_topic=None,
        raw_payload=None,
        parsed_payload=None,
    ):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        details = event_payload.get("details") if isinstance(event_payload.get("details"), dict) else {}

        ts_value = _safe_float(event_payload.get("ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO connectivity_events (
                        pivot_id,
                        session_id,
                        ts,
                        topic,
                        event_type,
                        summary,
                        details_json,
                        source_topic,
                        raw_payload,
                        parsed_payload_json,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        str(event_payload.get("topic") or ""),
                        str(event_payload.get("type") or ""),
                        str(event_payload.get("summary") or ""),
                        self._json_dumps(details),
                        str(source_topic or ""),
                        None if raw_payload is None else str(raw_payload),
                        self._json_dumps(parsed_payload if isinstance(parsed_payload, dict) else {}),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def insert_probe_event(self, pivot_id, session_id, event):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        ts_value = _safe_float(event_payload.get("ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO probe_events (
                        pivot_id,
                        session_id,
                        ts,
                        event_type,
                        topic,
                        latency_sec,
                        deadline_ts,
                        sent_ts,
                        payload,
                        details_json,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        str(event_payload.get("type") or ""),
                        str(event_payload.get("topic") or ""),
                        _safe_float(event_payload.get("latency_sec"), None),
                        _safe_float(event_payload.get("deadline_ts"), None),
                        _safe_float(event_payload.get("sent_ts"), None),
                        str(event_payload.get("payload") or ""),
                        self._json_dumps(event_payload.get("details") or {}),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def insert_probe_delay_point(
        self,
        pivot_id,
        session_id,
        ts,
        latency_sec,
        avg_latency_sec,
        median_latency_sec,
        sample_count,
    ):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        ts_value = _safe_float(ts, None)
        latency_value = _safe_float(latency_sec, None)
        avg_value = _safe_float(avg_latency_sec, None)
        sample_value = int(sample_count or 0)

        if ts_value is None or latency_value is None or avg_value is None or sample_value <= 0:
            return

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO probe_delay_points (
                        pivot_id,
                        session_id,
                        ts,
                        latency_sec,
                        avg_latency_sec,
                        median_latency_sec,
                        sample_count,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        latency_value,
                        avg_value,
                        _safe_float(median_latency_sec, None),
                        sample_value,
                        time.time(),
                    ),
                )

    def insert_cloud2_event(self, pivot_id, session_id, event):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        ts_value = _safe_float(event_payload.get("ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO cloud2_events (
                        pivot_id,
                        session_id,
                        ts,
                        rssi,
                        technology,
                        drop_duration_raw,
                        drop_duration_sec,
                        firmware,
                        event_date,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        None if event_payload.get("rssi") is None else str(event_payload.get("rssi")),
                        None if event_payload.get("technology") is None else str(event_payload.get("technology")),
                        None
                        if event_payload.get("drop_duration_raw") is None
                        else str(event_payload.get("drop_duration_raw")),
                        _safe_float(event_payload.get("drop_duration_sec"), None),
                        None if event_payload.get("firmware") is None else str(event_payload.get("firmware")),
                        None if event_payload.get("event_date") is None else str(event_payload.get("event_date")),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def insert_drop_event(self, pivot_id, session_id, event):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        ts_value = _safe_float(event_payload.get("ts"), None)
        duration_sec = _safe_float(event_payload.get("duration_sec"), None)
        if ts_value is None or duration_sec is None:
            return

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO drop_events (
                        pivot_id,
                        session_id,
                        ts,
                        duration_sec,
                        technology,
                        rssi,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        duration_sec,
                        None if event_payload.get("technology") is None else str(event_payload.get("technology")),
                        None if event_payload.get("rssi") is None else str(event_payload.get("rssi")),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def upsert_probe_setting(self, pivot_id, enabled, interval_sec):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return
        interval_value = int(interval_sec or 0)
        if interval_value < 1:
            interval_value = 1
        enabled_value = 1 if bool(enabled) else 0
        ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, slugify(normalized_id), None)
            with conn:
                conn.execute(
                    """
                    INSERT INTO probe_settings (pivot_id, enabled, interval_sec, updated_at_ts)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(pivot_id) DO UPDATE SET
                        enabled = excluded.enabled,
                        interval_sec = excluded.interval_sec,
                        updated_at_ts = excluded.updated_at_ts
                    """,
                    (
                        normalized_id,
                        enabled_value,
                        interval_value,
                        ts_value,
                    ),
                )

    def load_probe_settings(self):
        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT pivot_id, enabled, interval_sec
                FROM probe_settings
                ORDER BY pivot_id COLLATE NOCASE ASC
                """
            ).fetchall()
            settings = {}
            for row in rows:
                pivot_id = str(row["pivot_id"] or "").strip()
                if not pivot_id:
                    continue
                settings[pivot_id] = {
                    "enabled": bool(int(row["enabled"])),
                    "interval_sec": int(row["interval_sec"]),
                }
            return settings

    def _parse_event_row(self, row, fallback_type, fallback_topic):
        event = self._json_loads(row["event_json"], {})
        if not isinstance(event, dict):
            event = {}
        ts_value = _safe_float(event.get("ts"), _safe_float(row["ts"], None))
        event["id"] = int(row["id"])
        event["ts"] = ts_value
        event["at"] = _ts_to_str(ts_value)
        event["type"] = str(event.get("type") or fallback_type)
        event["topic"] = str(event.get("topic") or fallback_topic)
        return event

    def fetch_timeline_events(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM connectivity_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            event = self._parse_event_row(
                row=row,
                fallback_type=str(row["event_type"] or ""),
                fallback_topic=str(row["topic"] or ""),
            )
            details = event.get("details")
            if not isinstance(details, dict):
                details = self._json_loads(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
            source_topic = str(row["source_topic"] or "").strip()
            if source_topic and "source_topic" not in details:
                details["source_topic"] = source_topic

            raw_payload = row["raw_payload"]
            if raw_payload not in (None, "") and "raw_payload" not in details:
                details["raw_payload"] = str(raw_payload)

            parsed_payload = self._json_loads(row["parsed_payload_json"], {})
            if isinstance(parsed_payload, dict) and parsed_payload and "parsed_payload" not in details:
                details["parsed_payload"] = parsed_payload

            event["summary"] = str(event.get("summary") or row["summary"] or "")
            event["details"] = details
            events.append(event)
        return events

    def fetch_probe_events(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM probe_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            event = self._parse_event_row(
                row=row,
                fallback_type=str(row["event_type"] or ""),
                fallback_topic=str(row["topic"] or ""),
            )
            details = event.get("details")
            if not isinstance(details, dict):
                details = self._json_loads(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
            event["details"] = details
            events.append(event)
        return events

    def fetch_probe_delay_points(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM probe_delay_points
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts ASC, id ASC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        points = []
        for row in rows:
            ts_value = _safe_float(row["ts"], None)
            points.append(
                {
                    "id": int(row["id"]),
                    "ts": ts_value,
                    "at": _ts_to_str(ts_value),
                    "latency_sec": _safe_float(row["latency_sec"], None),
                    "avg_latency_sec": _safe_float(row["avg_latency_sec"], None),
                    "median_latency_sec": _safe_float(row["median_latency_sec"], None),
                    "sample_count": int(row["sample_count"]),
                }
            )
        return points

    def summarize_probe_stats_for_pivot(self, pivot_id, window_sec=None, now_ts=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return {
                "sent_count": 0,
                "response_count": 0,
                "timeout_count": 0,
                "response_ratio_pct": None,
                "latency_sample_count": 0,
                "latency_last_sec": None,
                "latency_avg_sec": None,
                "latency_median_sec": None,
                "latency_min_sec": None,
                "latency_max_sec": None,
            }

        safe_window = _safe_float(window_sec, None)
        if safe_window is None or safe_window <= 0:
            safe_window = float(PROBE_STATS_WINDOW_SEC)

        reference_ts = _safe_float(now_ts, None)
        if reference_ts is None:
            reference_ts = time.time()
        cutoff_ts = reference_ts - safe_window

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT event_type, latency_sec, ts
                FROM probe_events
                WHERE pivot_id = ?
                    AND ts >= ?
                ORDER BY ts ASC, id ASC
                """,
                (normalized_id, cutoff_ts),
            ).fetchall()

        sent_count = 0
        response_count = 0
        timeout_count = 0
        latency_samples = []

        for row in rows:
            event_type = str(row["event_type"] or "").strip().lower()
            if event_type == "sent":
                sent_count += 1
                continue
            if event_type == "timeout":
                timeout_count += 1
                continue
            if event_type != "response":
                continue

            response_count += 1
            latency = _safe_float(row["latency_sec"], None)
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

    def fetch_cloud2_events(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM cloud2_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            event = self._json_loads(row["event_json"], {})
            if not isinstance(event, dict):
                event = {}
            ts_value = _safe_float(event.get("ts"), _safe_float(row["ts"], None))
            event["id"] = int(row["id"])
            event["ts"] = ts_value
            event["at"] = _ts_to_str(ts_value)
            if "rssi" not in event:
                event["rssi"] = row["rssi"]
            if "technology" not in event:
                event["technology"] = row["technology"]
            if "drop_duration_raw" not in event:
                event["drop_duration_raw"] = row["drop_duration_raw"]
            if "drop_duration_sec" not in event:
                event["drop_duration_sec"] = _safe_float(row["drop_duration_sec"], None)
            if "firmware" not in event:
                event["firmware"] = row["firmware"]
            if "event_date" not in event:
                event["event_date"] = row["event_date"]
            events.append(event)
        return events

    def _fallback_state_pivot_summary(self, pivot_id, pivot_slug, session_id, run_id):
        return {
            "pivot_id": str(pivot_id),
            "pivot_slug": str(pivot_slug or slugify(pivot_id)),
            "session_id": str(session_id or ""),
            "run_id": str(run_id or ""),
            "status": {
                "code": "gray",
                "label": "Inicial",
                "rank": 1,
                "reason": "Sem snapshot persistido.",
            },
            "quality": {
                "code": "green",
                "label": "Saudavel",
                "rank": 3,
                "reason": "Sem snapshot persistido.",
            },
            "last_ping_ts": None,
            "last_ping_at": "-",
            "last_cloudv2_ts": None,
            "last_cloudv2_at": "-",
            "last_cloud2": {},
            "last_activity_ts": None,
            "last_activity_at": "-",
            "median_ready": False,
            "median_cloudv2_interval_sec": None,
            "median_sample_count": 0,
            "probe": {
                "enabled": False,
                "interval_sec": 300,
                "last_sent_ts": None,
                "last_sent_at": "-",
                "last_response_ts": None,
                "last_response_at": "-",
                "pending": False,
                "pending_deadline_ts": None,
                "pending_deadline_at": "-",
                "timeout_streak": 0,
                "last_result": None,
                "alert": False,
                "sent_count": 0,
                "response_count": 0,
                "timeout_count": 0,
                "response_ratio_pct": None,
                "latency_sample_count": 0,
                "latency_last_sec": None,
                "latency_avg_sec": None,
                "latency_median_sec": None,
                "latency_min_sec": None,
                "latency_max_sec": None,
            },
        }

    def _build_state_summary_from_snapshot_row(self, row, run_id):
        pivot_id = str(row["pivot_id"] or "").strip()
        session_id = str(row["session_id"] or "").strip()
        snapshot_payload = self._json_loads(row["snapshot_json"], {})
        if not isinstance(snapshot_payload, dict):
            snapshot_payload = {}

        pivot_slug = str(snapshot_payload.get("pivot_slug") or slugify(pivot_id))
        summary = snapshot_payload.get("summary")
        if isinstance(summary, dict):
            item = dict(summary)
        else:
            item = self._fallback_state_pivot_summary(
                pivot_id=pivot_id,
                pivot_slug=pivot_slug,
                session_id=session_id,
                run_id=run_id,
            )
        item["pivot_id"] = str(item.get("pivot_id") or pivot_id)
        item["pivot_slug"] = str(item.get("pivot_slug") or pivot_slug)
        item["session_id"] = str(item.get("session_id") or session_id)
        item["run_id"] = str(item.get("run_id") or run_id or "")

        parsed = _safe_bool(item.get("median_ready"), None)
        if parsed is None:
            item.pop("median_ready", None)
        else:
            item["median_ready"] = parsed
        parsed = _safe_int(item.get("median_sample_count"), None)
        if parsed is None:
            item.pop("median_sample_count", None)
        else:
            item["median_sample_count"] = max(0, parsed)
        parsed = _safe_float(item.get("median_cloudv2_interval_sec"), None)
        if parsed is None:
            item.pop("median_cloudv2_interval_sec", None)
        else:
            item["median_cloudv2_interval_sec"] = parsed
        parsed = _safe_float(item.get("disconnect_threshold_sec"), None)
        if parsed is None:
            item.pop("disconnect_threshold_sec", None)
        else:
            item["disconnect_threshold_sec"] = parsed

        row_keys = set(row.keys()) if hasattr(row, "keys") else set()
        if "snapshot_median_ready" in row_keys:
            parsed = _safe_bool(row["snapshot_median_ready"], None)
            if parsed is not None:
                item["median_ready"] = parsed
        if "snapshot_median_sample_count" in row_keys:
            parsed = _safe_int(row["snapshot_median_sample_count"], None)
            if parsed is not None:
                item["median_sample_count"] = max(0, parsed)
        if "snapshot_median_cloudv2_interval_sec" in row_keys:
            parsed = _safe_float(row["snapshot_median_cloudv2_interval_sec"], None)
            if parsed is not None:
                item["median_cloudv2_interval_sec"] = parsed
        if "snapshot_disconnect_threshold_sec" in row_keys:
            parsed = _safe_float(row["snapshot_disconnect_threshold_sec"], None)
            if parsed is not None:
                item["disconnect_threshold_sec"] = parsed

        if "median_ready" not in item:
            item["median_ready"] = False
        if "median_sample_count" not in item:
            item["median_sample_count"] = 0

        if not isinstance(item.get("status"), dict):
            item["status"] = {
                "code": "gray",
                "label": "Inicial",
                "rank": 1,
                "reason": "Sem status persistido.",
            }
        if not isinstance(item.get("quality"), dict):
            item["quality"] = {
                "code": "green",
                "label": "Saudavel",
                "rank": 3,
                "reason": "Sem qualidade persistida.",
            }
        if not isinstance(item.get("probe"), dict):
            item["probe"] = self._fallback_state_pivot_summary(
                pivot_id=pivot_id,
                pivot_slug=pivot_slug,
                session_id=session_id,
                run_id=run_id,
            )["probe"]
        if not isinstance(item.get("last_cloud2"), dict):
            item["last_cloud2"] = {}
        return item

    def get_run_state_payload(self, run_id=None):
        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=run_id)
            if run_row is None:
                return None

            resolved_run_id = str(run_row["run_id"] or "").strip()
            if not resolved_run_id:
                return None

            rows = conn.execute(
                """
                SELECT
                    sessions.pivot_id,
                    sessions.session_id,
                    sessions.updated_at_ts AS session_updated_at_ts,
                    snapshots.snapshot_json,
                    snapshots.median_ready AS snapshot_median_ready,
                    snapshots.median_sample_count AS snapshot_median_sample_count,
                    snapshots.median_cloudv2_interval_sec AS snapshot_median_cloudv2_interval_sec,
                    snapshots.disconnect_threshold_sec AS snapshot_disconnect_threshold_sec,
                    snapshots.updated_at_ts AS snapshot_updated_at_ts
                FROM monitoring_sessions AS sessions
                LEFT JOIN pivot_snapshots AS snapshots
                    ON snapshots.pivot_id = sessions.pivot_id
                    AND snapshots.session_id = sessions.session_id
                WHERE sessions.run_id = ?
                    AND sessions.session_id = (
                        SELECT sessions_inner.session_id
                        FROM monitoring_sessions AS sessions_inner
                        WHERE sessions_inner.run_id = sessions.run_id
                            AND sessions_inner.pivot_id = sessions.pivot_id
                        ORDER BY sessions_inner.updated_at_ts DESC, sessions_inner.started_at_ts DESC
                        LIMIT 1
                    )
                ORDER BY sessions.pivot_id COLLATE NOCASE ASC
                """,
                (resolved_run_id,),
            ).fetchall()

        pivots = []
        last_updated_ts = _safe_float(run_row["updated_at_ts"], None)
        for row in rows:
            pivots.append(self._build_state_summary_from_snapshot_row(row, resolved_run_id))
            row_updated = _safe_float(row["snapshot_updated_at_ts"], _safe_float(row["session_updated_at_ts"], None))
            if row_updated is None:
                continue
            if last_updated_ts is None or row_updated > last_updated_ts:
                last_updated_ts = row_updated

        if last_updated_ts is None:
            last_updated_ts = time.time()

        return {
            "run_id": resolved_run_id,
            "run": self._row_to_run_dict_locked(run_row, now_ts=last_updated_ts),
            "updated_at_ts": last_updated_ts,
            "updated_at": _ts_to_str(last_updated_ts),
            "pivots": pivots,
        }

    def get_panel_payload(self, pivot_id, session_id=None, run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        with self._lock:
            conn = self._require_conn_locked()
            session_row = self._query_session_row_locked(
                conn,
                normalized_id,
                session_id=session_id,
                run_id=run_id,
            )
            if session_row is None:
                return None

            resolved_session_id = str(session_row["session_id"])
            resolved_run_id = str(session_row["run_id"] or "").strip()
            run_row = self._query_run_row_locked(conn, run_id=resolved_run_id) if resolved_run_id else None
            snapshot_row = conn.execute(
                """
                SELECT snapshot_json
                FROM pivot_snapshots
                WHERE pivot_id = ? AND session_id = ?
                LIMIT 1
                """,
                (normalized_id, resolved_session_id),
            ).fetchone()

        payload = {}
        if snapshot_row is not None:
            payload = self._json_loads(snapshot_row["snapshot_json"], {})
            if not isinstance(payload, dict):
                payload = {}

        if not payload:
            payload = {
                "pivot_id": normalized_id,
                "pivot_slug": slugify(normalized_id),
                "updated_at": _ts_to_str(time.time()),
                "updated_at_ts": time.time(),
                "summary": {},
                "metrics": {},
                "timeline": [],
                "probe_events": [],
                "cloud2_events": [],
                "probe_delay_points": [],
            }

        timeline = self.fetch_timeline_events(normalized_id, resolved_session_id, limit=self.max_events_per_pivot)
        probe_events = self.fetch_probe_events(normalized_id, resolved_session_id, limit=self.max_events_per_pivot)
        cloud2_events = self.fetch_cloud2_events(normalized_id, resolved_session_id, limit=self.max_events_per_pivot)
        probe_delay_points = self.fetch_probe_delay_points(
            normalized_id,
            resolved_session_id,
            limit=self.max_events_per_pivot,
        )

        payload["pivot_id"] = normalized_id
        payload["pivot_slug"] = str(payload.get("pivot_slug") or slugify(normalized_id))
        payload["timeline"] = timeline
        payload["probe_events"] = probe_events
        payload["cloud2_events"] = cloud2_events
        payload["probe_delay_points"] = probe_delay_points
        payload["session_id"] = resolved_session_id
        payload["run_id"] = resolved_run_id
        payload["session"] = self._row_to_session_dict_locked(session_row)
        payload["run"] = self._row_to_run_dict_locked(run_row)

        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        probe_summary = summary.get("probe") if isinstance(summary.get("probe"), dict) else {}
        probe_stats = self.summarize_probe_stats_for_pivot(
            normalized_id,
            window_sec=PROBE_STATS_WINDOW_SEC,
            now_ts=payload.get("updated_at_ts"),
        )
        probe_summary.update(
            {
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
            }
        )
        summary["probe"] = probe_summary
        payload["summary"] = summary
        return payload
