import json
import os
import re
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse


DASHBOARD_DIR = "dashboards"
DATA_DIR = os.path.join(DASHBOARD_DIR, "data")


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)


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

            if path == "/api/health":
                self._write_json(200, {"ok": True})
                return

            if path == "/api/state":
                payload = telemetry_store.get_state_snapshot()
                self._write_json(200, payload)
                return

            if path.startswith("/api/pivot/"):
                pivot_id = unquote(path[len("/api/pivot/") :]).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                payload = telemetry_store.get_pivot_snapshot(pivot_id)
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


def start_dashboard_server(port, telemetry_store, reload_token_getter=None):
    ensure_dirs()
    handler = _build_handler(telemetry_store, reload_token_getter=reload_token_getter)
    server = ThreadingHTTPServer(("127.0.0.1", int(port)), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
