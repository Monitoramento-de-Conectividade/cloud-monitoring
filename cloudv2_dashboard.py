import json
import os
import re
import threading
from datetime import datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


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


def _render_index_html(pivot_entries):
    cards = []
    for pivot_id, file_name in pivot_entries:
        cards.append(
            f"""
            <a class="pivot-card" href="{file_name}">
                <div class="pivot-id">{pivot_id}</div>
                <div class="pivot-sub">Abrir painel</div>
            </a>
            """
        )

    cards_html = "\n".join(cards) if cards else "<div class=\"empty\">Nenhum pivot configurado.</div>"

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>CloudV2 - Painel</title>
    <link rel="stylesheet" href="dashboard.css"/>
</head>
<body class="index">
    <div class="page">
        <header class="hero">
            <div>
                <h1>CloudV2 - Painel de Monitoramento</h1>
                <p>Selecione um pivot para visualizar respostas, falhas de ping e atividade por topico.</p>
            </div>
        </header>
        <section class="grid">
            {cards_html}
        </section>
    </div>
</body>
</html>
"""


def _render_pivot_html(pivot_id, pivot_file, refresh_sec):
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>CloudV2 - {pivot_id}</title>
    <link rel="stylesheet" href="dashboard.css"/>
</head>
<body data-pivot-id="{pivot_id}" data-pivot-file="{pivot_file}" data-refresh-sec="{refresh_sec}">
    <div class="page">
        <header class="hero">
            <div>
                <a class="back" href="index.html">Voltar</a>
                <h1 id="pivotTitle">{pivot_id}</h1>
                <p id="pivotSubtitle">Atualizando automaticamente.</p>
            </div>
            <div class="updated">
                <span>Ultima atualizacao</span>
                <strong id="lastUpdated">-</strong>
            </div>
        </header>

        <section class="stats">
            <div class="card">
                <div class="label">Total de mensagens</div>
                <div class="value" id="totalCount">0</div>
                <div class="hint">Em todos os topicos monitorados</div>
            </div>
            <div class="card">
                <div class="label">Cloud2</div>
                <div class="value" id="cloud2Count">0</div>
                <div class="hint">Mensagens no topico cloud2</div>
            </div>
            <div class="card">
                <div class="label">Ultima atividade</div>
                <div class="value" id="lastSeen">-</div>
                <div class="hint" id="lastSeenAgo">-</div>
            </div>
            <div class="card">
                <div class="label">Ping cloudv2-ping</div>
                <div class="value" id="lastPing">-</div>
                <div class="hint" id="pingStatus">-</div>
            </div>
            <div class="card">
                <div class="label">Resposta #11$</div>
                <div class="value" id="responseRate">0%</div>
                <div class="hint" id="responseTotals">0 OK | 0 NAO</div>
            </div>
            <div class="card">
                <div class="label">Envios #11$</div>
                <div class="value" id="sentCount">0</div>
                <div class="hint" id="lastResponseAt">Ultima resposta: -</div>
            </div>
        </section>

        <section class="charts">
            <div class="card">
                <div class="card-header">
                    <h2>Resposta #11$</h2>
                    <span class="badge">SIM vs NAO</span>
                </div>
                <canvas id="responseChart"></canvas>
                <div class="legend">
                    <span class="dot ok"></span> Respondeu
                    <span class="dot fail"></span> Nao respondeu
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    <h2>Falhas de ping cloudv2-ping</h2>
                    <span class="badge" id="missingCount">0 falhas</span>
                </div>
                <canvas id="pingChart"></canvas>
                <div class="legend">
                    <span class="dot miss"></span> Intervalo sem ping
                </div>
            </div>
        </section>

        <section class="details">
            <div class="card">
                <div class="card-header">
                    <h2>Topicos</h2>
                    <span class="badge" id="topicCount">0 topicos</span>
                </div>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Topico</th>
                                <th>Qtde</th>
                                <th>Ultima vez</th>
                            </tr>
                        </thead>
                        <tbody id="topicsBody"></tbody>
                    </table>
                </div>
            </div>
            <div class="card">
                <div class="card-header">
                    <h2>Falhas recentes</h2>
                    <span class="badge">Ping ausente</span>
                </div>
                <div class="list" id="missingList"></div>
            </div>
        </section>
    </div>
    <script src="dashboard.js"></script>
</body>
</html>
"""


def _dashboard_css():
    return """
:root {
  --bg: #e6f5ea;
  --panel: #ffffff;
  --accent: #2e7d32;
  --accent-dark: #1b5e20;
  --accent-soft: #cde9d4;
  --text: #12341f;
  --muted: #4e6f5b;
  --ok: #2e7d32;
  --fail: #9b8d1f;
  --miss: #688b4f;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: "Trebuchet MS", "Verdana", "Tahoma", sans-serif;
  color: var(--text);
  background: radial-gradient(circle at top, #f4fbf6 0%, #d8eedf 55%, #c7e3d0 100%);
  min-height: 100vh;
}

h1, h2 {
  font-family: "Georgia", "Palatino Linotype", serif;
  margin: 0;
}

a {
  color: inherit;
  text-decoration: none;
}

.page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 28px 24px 40px;
}

.hero {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  gap: 20px;
  padding: 20px 24px;
  border-radius: 18px;
  background: linear-gradient(135deg, #2e7d32, #1b5e20);
  color: #f2fff4;
  box-shadow: 0 12px 30px rgba(27, 94, 32, 0.25);
}

.hero p {
  margin: 8px 0 0;
  color: #d9f5df;
}

.hero .back {
  display: inline-block;
  font-size: 12px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: #d9f5df;
  margin-bottom: 10px;
}

.updated {
  text-align: right;
  font-size: 13px;
  color: #d9f5df;
}

.updated strong {
  display: block;
  font-size: 16px;
  color: #ffffff;
  margin-top: 4px;
}

.stats {
  margin-top: 24px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 16px;
}

.card {
  background: var(--panel);
  border-radius: 16px;
  padding: 16px 18px;
  box-shadow: 0 10px 20px rgba(18, 52, 31, 0.1);
  border: 1px solid rgba(46, 125, 50, 0.15);
}

.label {
  font-size: 12px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
}

.value {
  font-size: 28px;
  font-weight: 700;
  margin-top: 6px;
}

.hint {
  font-size: 12px;
  color: var(--muted);
  margin-top: 6px;
}

.charts {
  margin-top: 22px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 18px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.badge {
  background: var(--accent-soft);
  color: var(--accent-dark);
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

canvas {
  width: 100%;
  height: 180px;
  border-radius: 12px;
  background: #f6fbf7;
  border: 1px solid rgba(46, 125, 50, 0.12);
}

.legend {
  margin-top: 8px;
  font-size: 12px;
  color: var(--muted);
  display: flex;
  gap: 16px;
  align-items: center;
}

.dot {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  margin-right: 6px;
}

.dot.ok { background: var(--ok); }
.dot.fail { background: var(--fail); }
.dot.miss { background: var(--miss); }

.details {
  margin-top: 24px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 18px;
}

.table-wrap {
  max-height: 260px;
  overflow: auto;
  margin-top: 8px;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

th, td {
  text-align: left;
  padding: 8px 6px;
  border-bottom: 1px solid #e1efe5;
}

th {
  text-transform: uppercase;
  font-size: 11px;
  color: var(--muted);
  letter-spacing: 0.06em;
}

.list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 10px;
  max-height: 260px;
  overflow: auto;
}

.list-item {
  padding: 10px 12px;
  border-radius: 12px;
  background: #f0f8f2;
  border: 1px solid rgba(46, 125, 50, 0.12);
  font-size: 12px;
}

.index .grid {
  margin-top: 22px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 14px;
}

.pivot-card {
  background: var(--panel);
  border-radius: 16px;
  padding: 18px;
  border: 1px solid rgba(46, 125, 50, 0.2);
  box-shadow: 0 12px 20px rgba(18, 52, 31, 0.08);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}

.pivot-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 14px 26px rgba(18, 52, 31, 0.16);
}

.pivot-id {
  font-size: 18px;
  font-weight: 700;
}

.pivot-sub {
  margin-top: 6px;
  font-size: 12px;
  color: var(--muted);
}

.empty {
  font-size: 14px;
  color: var(--muted);
}
"""


def _dashboard_js():
    return """
const pivotId = document.body.dataset.pivotId;
const pivotFile = document.body.dataset.pivotFile;
const refreshSec = parseInt(document.body.dataset.refreshSec || "5", 10);

let lastData = null;

function toLocal(ts) {
  if (!ts) return "-";
  const date = new Date(ts * 1000);
  return date.toLocaleString();
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) node.textContent = value;
}

function resizeCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.floor(rect.width * ratio);
  canvas.height = Math.floor(rect.height * ratio);
  const ctx = canvas.getContext("2d");
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  return { width: rect.width, height: rect.height, ctx };
}

function drawResponseChart(events) {
  const canvas = document.getElementById("responseChart");
  if (!canvas) return;
  const { width, height, ctx } = resizeCanvas(canvas);
  ctx.clearRect(0, 0, width, height);

  if (!events || events.length === 0) {
    ctx.fillStyle = "#6a8672";
    ctx.font = "12px Trebuchet MS";
    ctx.fillText("Sem dados de resposta", 12, height / 2);
    return;
  }

  const maxEvents = Math.min(events.length, 160);
  const start = events.length - maxEvents;
  const spacing = (width - 24) / Math.max(1, maxEvents - 1);
  const top = 12;
  const bottom = height - 12;

  for (let i = 0; i < maxEvents; i += 1) {
    const evt = events[start + i];
    const x = 12 + i * spacing;
    ctx.strokeStyle = evt.ok ? "#2e7d32" : "#9b8d1f";
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(x, bottom);
    ctx.lineTo(x, top);
    ctx.stroke();
  }
}

function drawMissingChart(missing, expectedSec) {
  const canvas = document.getElementById("pingChart");
  if (!canvas) return;
  const { width, height, ctx } = resizeCanvas(canvas);
  ctx.clearRect(0, 0, width, height);

  if (!missing || missing.length === 0) {
    ctx.fillStyle = "#6a8672";
    ctx.font = "12px Trebuchet MS";
    ctx.fillText("Sem falhas de ping", 12, height / 2);
    return;
  }

  const maxEvents = Math.min(missing.length, 24);
  const slice = missing.slice(-maxEvents);
  const maxDur = Math.max(expectedSec || 1, ...slice.map(item => item.duration_sec || 0));
  const barWidth = Math.max(8, (width - 24) / maxEvents - 6);
  const base = height - 18;

  slice.forEach((item, index) => {
    const heightRatio = (item.duration_sec || 0) / maxDur;
    const barHeight = Math.max(6, (height - 36) * heightRatio);
    const x = 12 + index * (barWidth + 6);
    ctx.fillStyle = "#688b4f";
    ctx.fillRect(x, base - barHeight, barWidth, barHeight);
  });
}

function updateTopics(topics) {
  const body = document.getElementById("topicsBody");
  if (!body) return;
  body.innerHTML = "";
  const entries = Object.entries(topics || {});
  entries.sort((a, b) => (b[1].count || 0) - (a[1].count || 0));

  if (entries.length === 0) {
    const row = document.createElement("tr");
    row.innerHTML = "<td colspan='3'>Sem mensagens registradas</td>";
    body.appendChild(row);
    setText("topicCount", "0 topicos");
    return;
  }

  entries.forEach(([topic, info]) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${topic}</td>
      <td>${info.count || 0}</td>
      <td>${info.last_at || "-"}</td>
    `;
    body.appendChild(row);
  });

  setText("topicCount", `${entries.length} topicos`);
}

function updateMissingList(missing) {
  const list = document.getElementById("missingList");
  if (!list) return;
  list.innerHTML = "";

  if (!missing || missing.length === 0) {
    list.innerHTML = "<div class='list-item'>Nenhuma falha de ping registrada.</div>";
    return;
  }

  const slice = missing.slice(-8).reverse();
  slice.forEach(item => {
    const node = document.createElement("div");
    node.className = "list-item";
    node.textContent = `${item.start_at} ate ${item.end_at} (${formatDuration(item.duration_sec)})`;
    list.appendChild(node);
  });
}

function updateDashboard(data) {
  if (!data) return;
  lastData = data;

  setText("pivotTitle", data.pivot_id || pivotId || "-");
  setText("lastUpdated", data.updated_at || "-");

  const summary = data.summary || {};
  setText("totalCount", summary.total_count || 0);
  setText("cloud2Count", summary.cloud2_count || 0);
  setText("lastSeen", summary.last_seen_at || "-");
  setText("lastSeenAgo", summary.last_seen_ago || "-");

  const ping = data.ping || {};
  setText("lastPing", ping.last_ping_at || "-");
  const missingTotal = ping.missing_total_sec || 0;
  const pingNote = missingTotal ? ` | ${formatDuration(missingTotal)} sem ping` : "";
  setText("pingStatus", (ping.overdue ? "Ping atrasado" : "Ping dentro do esperado") + pingNote);
  setText("missingCount", `${ping.missing_count || 0} falhas`);

  const responses = data.responses || {};
  setText("responseRate", `${responses.rate || 0}%`);
  setText("responseTotals", `${responses.success || 0} OK | ${responses.fail || 0} NAO`);
  setText("sentCount", summary.sent_count || 0);
  setText("lastResponseAt", summary.last_response_at ? `Ultima resposta: ${summary.last_response_at}` : "Ultima resposta: -");

  drawResponseChart(responses.events || []);
  drawMissingChart(ping.missing_events || [], ping.expected_interval_sec || 0);
  updateTopics(data.topics || {});
  updateMissingList(ping.missing_events || []);
}

async function loadData() {
  try {
    const response = await fetch(`${pivotFile}?t=${Date.now()}`);
    if (!response.ok) return;
    const data = await response.json();
    updateDashboard(data);
  } catch (err) {
    setText("pivotSubtitle", "Falha ao carregar dados. Tentando novamente.");
  }
}

window.addEventListener("resize", () => {
  if (lastData) updateDashboard(lastData);
});

loadData();
setInterval(loadData, refreshSec * 1000);
"""


def generate_dashboard_assets(pivot_ids, refresh_sec):
    ensure_dirs()
    write_text_atomic(os.path.join(DASHBOARD_DIR, "dashboard.css"), _dashboard_css())
    write_text_atomic(os.path.join(DASHBOARD_DIR, "dashboard.js"), _dashboard_js())

    pivot_entries = []
    for pivot_id in pivot_ids:
        slug = slugify(pivot_id)
        file_name = f"pivot_{slug}.html"
        pivot_entries.append((pivot_id, file_name))

        html = _render_pivot_html(pivot_id, f"data/pivot_{slug}.json", refresh_sec)
        write_text_atomic(os.path.join(DASHBOARD_DIR, file_name), html)

    index_html = _render_index_html(pivot_entries)
    write_text_atomic(os.path.join(DASHBOARD_DIR, "index.html"), index_html)

    mapping = [{"pivot_id": pivot_id, "file": file_name} for pivot_id, file_name in pivot_entries]
    write_json_atomic(os.path.join(DATA_DIR, "pivots.json"), mapping)


def start_dashboard_server(port):
    ensure_dirs()
    handler = partial(SimpleHTTPRequestHandler, directory=DASHBOARD_DIR)
    server = ThreadingHTTPServer(("127.0.0.1", int(port)), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
