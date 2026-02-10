import json
import os
import re
import shutil
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from backend.cloudv2_paths import DATA_SUBDIR, LEGACY_WEB_DIRS, resolve_data_dir, resolve_web_dir


DASHBOARD_DIR = resolve_web_dir()
DATA_DIR = resolve_data_dir(DASHBOARD_DIR)


def _data_dir_has_files(path):
    if not os.path.isdir(path):
        return False
    try:
        for name in os.listdir(path):
            if name in (".gitkeep",):
                continue
            if os.path.isfile(os.path.join(path, name)):
                return True
    except OSError:
        return False
    return False


def _seed_data_dir_from_legacy():
    normalized_current = os.path.normpath(DASHBOARD_DIR)

    for legacy_dir in LEGACY_WEB_DIRS:
        normalized_legacy = os.path.normpath(legacy_dir)
        if normalized_current == normalized_legacy:
            continue

        legacy_data_dir = os.path.join(legacy_dir, DATA_SUBDIR)
        if not os.path.isdir(legacy_data_dir):
            continue
        if _data_dir_has_files(DATA_DIR):
            return

        try:
            names = os.listdir(legacy_data_dir)
        except OSError:
            continue

        for name in names:
            source_path = os.path.join(legacy_data_dir, name)
            target_path = os.path.join(DATA_DIR, name)
            if not os.path.isfile(source_path):
                continue
            try:
                shutil.copy2(source_path, target_path)
            except OSError:
                continue


def ensure_dirs():
    os.makedirs(DASHBOARD_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    _seed_data_dir_from_legacy()


def slugify(value):
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value)).strip("_")
    return slug or "pivot"


def write_text_atomic(path, content):
    temp = f"{path}.tmp"
    with open(temp, "w", encoding="utf-8") as file:
        file.write(content)
    os.replace(temp, path)


def write_json_atomic(path, data):
    temp = f"{path}.tmp"
    with open(temp, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    os.replace(temp, path)


def _default_index_html():
    return """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>CloudV2 Monitor</title>
  <link rel="stylesheet" href="dashboard.css"/>
</head>
<body>
  <div id="app"></div>
  <script src="dashboard.js"></script>
</body>
</html>
"""


def _default_css():
    return """body{margin:0;font-family:Arial,sans-serif;background:#f6faf7;color:#153425}#app{padding:20px}"""


def _default_js():
    return """document.getElementById('app').textContent='Dashboard carregado.';"""


def generate_dashboard_assets(refresh_sec):
    ensure_dirs()
    index_path = os.path.join(DASHBOARD_DIR, "index.html")
    css_path = os.path.join(DASHBOARD_DIR, "dashboard.css")
    js_path = os.path.join(DASHBOARD_DIR, "dashboard.js")

    if not os.path.exists(index_path):
        write_text_atomic(index_path, _default_index_html())
    if not os.path.exists(css_path):
        write_text_atomic(css_path, _default_css())
    if not os.path.exists(js_path):
        write_text_atomic(js_path, _default_js())

    write_json_atomic(
        os.path.join(DATA_DIR, "ui_config.json"),
        {"refresh_sec": max(1, int(refresh_sec))},
    )


def _build_handler(telemetry_store, reload_token_getter=None):
    class DashboardHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

        def _write_json(self, status_code, payload):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self):
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0:
                return {}
            raw = self.rfile.read(content_length)
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query or "")

            if path == "/api/health":
                self._write_json(200, {"ok": True})
                return

            if path == "/api/state":
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                payload = telemetry_store.get_state_snapshot(run_id=run_id)
                self._write_json(200, payload)
                return

            if path == "/api/monitoring/runs":
                limit_raw = (query.get("limit") or [None])[0]
                try:
                    limit = int(limit_raw) if limit_raw is not None else 200
                except (TypeError, ValueError):
                    limit = 200
                runs = telemetry_store.list_monitoring_runs(limit=limit)
                self._write_json(
                    200,
                    {
                        "runs": runs,
                    },
                )
                return

            if path.startswith("/api/pivot/") and path.endswith("/sessions"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/sessions")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                limit_raw = (query.get("limit") or [None])[0]
                try:
                    limit = int(limit_raw) if limit_raw is not None else 200
                except (TypeError, ValueError):
                    limit = 200
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                sessions = telemetry_store.list_monitoring_sessions(pivot_id, limit=limit, run_id=run_id)
                self._write_json(
                    200,
                    {
                        "pivot_id": pivot_id,
                        "run_id": run_id,
                        "sessions": sessions,
                    },
                )
                return

            if path.startswith("/api/pivot/") and path.endswith("/panel"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/panel")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                session_id = (query.get("session_id") or [None])[0]
                if isinstance(session_id, str):
                    session_id = session_id.strip() or None
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                payload = telemetry_store.get_complete_panel(pivot_id, session_id=session_id, run_id=run_id)
                if payload is None:
                    self._write_json(404, {"error": "pivot nao encontrado"})
                    return
                self._write_json(200, payload)
                return

            if path.startswith("/api/pivot/"):
                pivot_id = unquote(path[len("/api/pivot/") :]).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                session_id = (query.get("session_id") or [None])[0]
                if isinstance(session_id, str):
                    session_id = session_id.strip() or None
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                payload = telemetry_store.get_pivot_snapshot(pivot_id, session_id=session_id, run_id=run_id)
                if payload is None:
                    self._write_json(404, {"error": "pivot nao encontrado"})
                    return
                self._write_json(200, payload)
                return

            if path == "/api/probe-config":
                payload = telemetry_store.get_probe_config_snapshot()
                self._write_json(200, payload)
                return

            if path == "/api/dev/reload-token":
                token = ""
                if callable(reload_token_getter):
                    token = str(reload_token_getter() or "")
                self._write_json(200, {"token": token})
                return

            super().do_GET()

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/api/admin/purge-database":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                password = body.get("password")
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    result = telemetry_store.purge_database_records(password=password, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "result": result})
                return

            if path == "/api/monitoring/runs":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    created = telemetry_store.start_new_monitoring_run(source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "created": created})
                return

            if path == "/api/monitoring/history":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                run_id = str(body.get("run_id") or "").strip()
                if not run_id:
                    self._write_json(400, {"error": "run_id obrigatorio"})
                    return
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    activated = telemetry_store.activate_history_run(run_id=run_id, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "activated": activated})
                return

            if path.startswith("/api/pivot/") and path.endswith("/sessions"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/sessions")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    created = telemetry_store.start_new_monitoring_session(pivot_id, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "created": created})
                return

            if path != "/api/probe-config":
                self._write_json(404, {"error": "rota nao encontrada"})
                return

            try:
                body = self._read_json_body()
            except json.JSONDecodeError:
                self._write_json(400, {"error": "json invalido"})
                return

            pivot_id = str(body.get("pivot_id", "")).strip()
            if not pivot_id:
                self._write_json(400, {"error": "pivot_id obrigatorio"})
                return

            enabled = bool(body.get("enabled", False))
            interval_sec = body.get("interval_sec")
            if interval_sec is None:
                interval_sec = telemetry_store.probe_default_interval_sec

            try:
                updated = telemetry_store.update_probe_setting(pivot_id, enabled, interval_sec)
            except ValueError as exc:
                self._write_json(400, {"error": str(exc)})
                return

            self._write_json(200, {"ok": True, "updated": updated})

        def log_message(self, format_text, *args):
            return

    return DashboardHandler


def start_dashboard_server(port, telemetry_store, reload_token_getter=None, host="127.0.0.1"):
    ensure_dirs()
    handler = _build_handler(telemetry_store, reload_token_getter=reload_token_getter)
    server = ThreadingHTTPServer((str(host), int(port)), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
