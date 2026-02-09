const ui = {
  updatedAt: document.getElementById("updatedAt"),
  countsMeta: document.getElementById("countsMeta"),
  searchInput: document.getElementById("searchInput"),
  sortSelect: document.getElementById("sortSelect"),
  statusFilters: document.getElementById("statusFilters"),
  statusSummary: document.getElementById("statusSummary"),
  pendingPanel: document.getElementById("pendingPanel"),
  pendingList: document.getElementById("pendingList"),
  cardsGrid: document.getElementById("cardsGrid"),
  cardsPrev: document.getElementById("cardsPrev"),
  cardsNext: document.getElementById("cardsNext"),
  cardsPageInfo: document.getElementById("cardsPageInfo"),
  pivotView: document.getElementById("pivotView"),
  closePivot: document.getElementById("closePivot"),
  pivotTitle: document.getElementById("pivotTitle"),
  pivotStatus: document.getElementById("pivotStatus"),
  pivotMetrics: document.getElementById("pivotMetrics"),
  connPreset: document.getElementById("connPreset"),
  connFromWrap: document.getElementById("connFromWrap"),
  connToWrap: document.getElementById("connToWrap"),
  connFrom: document.getElementById("connFrom"),
  connTo: document.getElementById("connTo"),
  connApply: document.getElementById("connApply"),
  connSummary: document.getElementById("connSummary"),
  connTrack: document.getElementById("connTrack"),
  connSegmentCard: document.getElementById("connSegmentCard"),
  connStartLabel: document.getElementById("connStartLabel"),
  connEndLabel: document.getElementById("connEndLabel"),
  probeEnabled: document.getElementById("probeEnabled"),
  probeInterval: document.getElementById("probeInterval"),
  saveProbe: document.getElementById("saveProbe"),
  probeHint: document.getElementById("probeHint"),
  probeDelayPreset: document.getElementById("probeDelayPreset"),
  probeDelayFromWrap: document.getElementById("probeDelayFromWrap"),
  probeDelayToWrap: document.getElementById("probeDelayToWrap"),
  probeDelayFrom: document.getElementById("probeDelayFrom"),
  probeDelayTo: document.getElementById("probeDelayTo"),
  probeDelayApply: document.getElementById("probeDelayApply"),
  probeDelayHint: document.getElementById("probeDelayHint"),
  probeDelayChart: document.getElementById("probeDelayChart"),
  probeDelayStartLabel: document.getElementById("probeDelayStartLabel"),
  probeDelayEndLabel: document.getElementById("probeDelayEndLabel"),
  timelineList: document.getElementById("timelineList"),
  timelinePrev: document.getElementById("timelinePrev"),
  timelineNext: document.getElementById("timelineNext"),
  timelinePageInfo: document.getElementById("timelinePageInfo"),
  cloud2Table: document.getElementById("cloud2Table"),
};

const state = {
  rawState: null,
  pivots: [],
  statusFilter: "all",
  search: "",
  sort: "critical",
  cardsPage: 1,
  cardsPageSize: 18,
  selectedPivot: null,
  pivotData: null,
  connPreset: "30d",
  connCustomFrom: "",
  connCustomTo: "",
  connSelectedSegmentKey: null,
  probeDelayPreset: "30d",
  probeDelayCustomFrom: "",
  probeDelayCustomTo: "",
  timelinePage: 1,
  timelinePageSize: 25,
  refreshMs: 5000,
  devReloadToken: null,
};

const STATUS_META = {
  all: { label: "Todos", css: "gray" },
  green: { label: "Conectado", css: "green" },
  yellow: { label: "Atencao", css: "yellow" },
  red: { label: "Offline", css: "red" },
  gray: { label: "Inicial", css: "gray" },
};

const EVENT_LABEL = {
  pivot_discovered: "Descoberta",
  cloudv2: "Pacote cloudv2",
  ping: "Ping cloudv2-ping",
  cloud2: "Evento cloud2",
  probe_sent: "Probe enviado",
  probe_response: "Probe respondido",
  probe_timeout: "Probe timeout",
  probe_response_unmatched: "Resposta sem correlacao",
};

function parseHashPivot() {
  const raw = String(location.hash || "");
  const match = raw.match(/pivot=([^&]+)/i);
  if (!match) return null;
  try {
    return decodeURIComponent(match[1]);
  } catch (err) {
    return null;
  }
}

function setHashPivot(pivotId) {
  const id = String(pivotId || "").trim();
  if (!id) {
    history.replaceState(null, "", location.pathname + location.search);
    return;
  }
  const nextHash = `#pivot=${encodeURIComponent(id)}`;
  if (location.hash !== nextHash) {
    history.replaceState(null, "", `${location.pathname}${location.search}${nextHash}`);
  }
}

function text(value, fallback = "-") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function fmtDuration(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  return `${(s / 3600).toFixed(1)}h`;
}

function fmtSecondsPrecise(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s >= 10) return `${s.toFixed(1)}s`;
  if (s >= 1) return `${s.toFixed(2)}s`;
  return `${s.toFixed(3)}s`;
}

function fmtPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "-";
  return `${n.toFixed(1)}%`;
}

function agoFromTs(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const delta = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (delta < 60) return `${delta}s atras`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m atras`;
  if (delta < 86400) return `${(delta / 3600).toFixed(1)}h atras`;
  return `${(delta / 86400).toFixed(1)}d atras`;
}

function toDateTimeLocal(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "";
  const date = new Date(ts * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(
    date.getMinutes()
  )}`;
}

function parseDateTimeLocal(value) {
  const textValue = String(value || "").trim();
  if (!textValue) return null;
  const parsed = new Date(textValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return Math.floor(parsed.getTime() / 1000);
}

function formatShortDateTime(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const date = new Date(ts * 1000);
  return date.toLocaleString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function getJson(url) {
  const response = await fetch(`${url}${url.includes("?") ? "&" : "?"}t=${Date.now()}`);
  if (!response.ok) throw new Error(`${response.status}`);
  return response.json();
}

function isLocalhostRuntime() {
  const host = String(location.hostname || "").trim().toLowerCase();
  return host === "localhost" || host === "127.0.0.1";
}

async function checkDevReloadToken() {
  const payload = await getJson("/api/dev/reload-token");
  return String(payload.token || "");
}

function startDevAutoReload() {
  if (!isLocalhostRuntime()) return;

  const pollReloadToken = async () => {
    try {
      const token = await checkDevReloadToken();
      if (!token) return;

      if (!state.devReloadToken) {
        state.devReloadToken = token;
        return;
      }

      if (state.devReloadToken !== token) {
        location.reload();
      }
    } catch (err) {
      return;
    }
  };

  pollReloadToken();
  setInterval(pollReloadToken, 1500);
}

function applyFilterSort() {
  const list = [...state.pivots];
  const needle = state.search.trim().toLowerCase();

  let filtered = list.filter((item) => {
    if (state.statusFilter !== "all") {
      if ((item.status || {}).code !== state.statusFilter) return false;
    }
    if (needle && !String(item.pivot_id || "").toLowerCase().includes(needle)) {
      return false;
    }
    return true;
  });

  filtered.sort((a, b) => {
    const ap = String(a.pivot_id || "");
    const bp = String(b.pivot_id || "");
    if (state.sort === "pivot_asc") return ap.localeCompare(bp);

    const aActivity = Number(a.last_activity_ts || 0);
    const bActivity = Number(b.last_activity_ts || 0);

    if (state.sort === "activity_desc") return bActivity - aActivity || ap.localeCompare(bp);
    if (state.sort === "activity_asc") return aActivity - bActivity || ap.localeCompare(bp);

    const aRank = Number((a.status || {}).rank ?? 99);
    const bRank = Number((b.status || {}).rank ?? 99);
    if (aRank !== bRank) return aRank - bRank;

    const aAlert = (a.probe || {}).alert ? 1 : 0;
    const bAlert = (b.probe || {}).alert ? 1 : 0;
    if (aAlert !== bAlert) return bAlert - aAlert;

    if (aActivity !== bActivity) return aActivity - bActivity;
    return ap.localeCompare(bp);
  });

  return filtered;
}

function renderHeader() {
  const raw = state.rawState || {};
  const updatedAt = text(raw.updated_at, "-");
  const counts = raw.counts || {};
  ui.updatedAt.textContent = `Atualizado: ${updatedAt}`;
  ui.countsMeta.textContent = `${counts.pivots || 0} pivots | ${counts.duplicate_drops || 0} duplicadas`;
}

function renderStatusSummary() {
  const counts = { all: 0, green: 0, yellow: 0, red: 0, gray: 0 };
  for (const pivot of state.pivots) {
    counts.all += 1;
    const code = (pivot.status || {}).code || "gray";
    if (counts[code] === undefined) counts[code] = 0;
    counts[code] += 1;
  }

  ui.statusSummary.innerHTML = `
    <div class="summary-pill"><span>Total</span><strong>${counts.all}</strong></div>
    <div class="summary-pill"><span>Conectado</span><strong>${counts.green}</strong></div>
    <div class="summary-pill"><span>Atencao</span><strong>${counts.yellow}</strong></div>
    <div class="summary-pill"><span>Offline</span><strong>${counts.red}</strong></div>
    <div class="summary-pill"><span>Inicial</span><strong>${counts.gray}</strong></div>
  `;
}

function renderPending() {
  const raw = state.rawState || {};
  const enabled = !!(((raw.settings || {}).show_pending_ping_pivots));
  const pending = raw.pending_ping || [];

  if (!enabled) {
    ui.pendingPanel.hidden = true;
    return;
  }

  ui.pendingPanel.hidden = false;
  if (!pending.length) {
    ui.pendingList.innerHTML = `<div class="empty">Nenhum pendente no momento.</div>`;
    return;
  }

  ui.pendingList.innerHTML = pending
    .slice(0, 40)
    .map((item) => {
      const id = escapeHtml(text(item.pivot_id));
      const count = Number(item.count || 0);
      const last = escapeHtml(text(item.last_seen_at));
      return `<div class="list-item"><strong>${id}</strong> visto ${count}x (ultimo em ${last})</div>`;
    })
    .join("");
}

function renderCards() {
  const filtered = applyFilterSort();
  const totalPages = Math.max(1, Math.ceil(filtered.length / state.cardsPageSize));
  if (state.cardsPage > totalPages) state.cardsPage = totalPages;
  const start = (state.cardsPage - 1) * state.cardsPageSize;
  const pageItems = filtered.slice(start, start + state.cardsPageSize);

  if (!pageItems.length) {
    ui.cardsGrid.innerHTML = `<div class="empty">Nenhum pivot encontrado com os filtros atuais.</div>`;
  } else {
    ui.cardsGrid.innerHTML = pageItems
      .map((pivot) => {
        const status = pivot.status || {};
        const statusCode = text(status.code, "gray");
        const label = escapeHtml(text(status.label, "Inicial"));
        const pivotId = escapeHtml(text(pivot.pivot_id, "pivot"));
        const lastPing = escapeHtml(text(pivot.last_ping_at));
        const lastCloudv2 = escapeHtml(text(pivot.last_cloudv2_at));
        const cloud2 = pivot.last_cloud2 || {};
        const medianReady = !!pivot.median_ready;
        const samples = Number(pivot.median_sample_count || 0);
        const medianText = medianReady
          ? `${fmtDuration(pivot.median_cloudv2_interval_sec)} (${samples} amostras)`
          : `${samples} amostras (inicial)`;

        const rssi = escapeHtml(text(cloud2.rssi));
        const technology = escapeHtml(text(cloud2.technology));
        const firmware = escapeHtml(text(cloud2.firmware));

        return `
          <article class="pivot-card">
            <div class="pivot-head">
              <div class="pivot-id">${pivotId}</div>
              <span class="badge ${statusCode}">${label}</span>
            </div>
            <div class="kv-grid">
              <div class="k">Ultimo ping</div><div>${lastPing}</div>
              <div class="k">Ultimo cloudv2</div><div>${lastCloudv2}</div>
              <div class="k">Mediana cloudv2</div><div>${escapeHtml(medianText)}</div>
              <div class="k">Ultima atividade</div><div>${escapeHtml(text(pivot.last_activity_at))}</div>
              <div class="k">cloud2 RSSI/Tec</div><div>${rssi} / ${technology}</div>
              <div class="k">Firmware</div><div>${firmware}</div>
            </div>
            <div class="card-actions">
              <button class="ghost open-pivot" data-pivot="${pivotId}">Abrir visao</button>
            </div>
          </article>
        `;
      })
      .join("");
  }

  ui.cardsPageInfo.textContent = `Pagina ${state.cardsPage}/${totalPages}`;
  ui.cardsPrev.disabled = state.cardsPage <= 1;
  ui.cardsNext.disabled = state.cardsPage >= totalPages;

  for (const button of ui.cardsGrid.querySelectorAll(".open-pivot")) {
    button.addEventListener("click", () => openPivot(button.dataset.pivot || ""));
  }
}

function renderPivotMetrics(pivot) {
  const summary = pivot.summary || {};
  const metrics = pivot.metrics || {};
  const status = summary.status || {};
  const probe = summary.probe || {};
  const sentCount = Number(probe.sent_count || 0);
  const responseCount = Number(probe.response_count || 0);
  const timeoutCount = Number(probe.timeout_count || 0);
  const latencySampleCount = Number(probe.latency_sample_count || 0);
  const responseCoverageText =
    sentCount > 0
      ? `${responseCount}/${sentCount} (${fmtPercent(probe.response_ratio_pct)})`
      : `${responseCount}/${sentCount}`;

  const cards = [
    { label: "Status", value: text(status.label) },
    { label: "Razao", value: text(status.reason) },
    { label: "Ultimo ping", value: text(summary.last_ping_at) },
    { label: "Ultimo cloudv2", value: text(summary.last_cloudv2_at) },
    {
      label: "Mediana cloudv2",
      value: summary.median_ready
        ? `${fmtDuration(summary.median_cloudv2_interval_sec)} (${summary.median_sample_count} amostras)`
        : `${summary.median_sample_count} amostras`,
    },
    { label: "Quedas 24h", value: text(metrics.drops_24h, "0") },
    { label: "Quedas 7d", value: text(metrics.drops_7d, "0") },
    { label: "Ultima duracao queda", value: fmtDuration(metrics.last_drop_duration_sec) },
    { label: "Ultimo RSSI", value: text(metrics.last_rssi) },
    { label: "Ultima tecnologia", value: text(metrics.last_technology) },
    { label: "Firmware", value: text(metrics.last_firmware) },
    { label: "Probe timeout streak", value: text(probe.timeout_streak, "0") },
    { label: "Probe respostas/envios", value: responseCoverageText },
    { label: "Probe timeouts", value: text(timeoutCount, "0") },
    { label: "Delay ultima resposta", value: fmtSecondsPrecise(probe.latency_last_sec) },
    { label: "Delay medio resposta", value: fmtSecondsPrecise(probe.latency_avg_sec) },
    { label: "Delay mediano resposta", value: fmtSecondsPrecise(probe.latency_median_sec) },
    {
      label: "Delay min/max resposta",
      value: `${fmtSecondsPrecise(probe.latency_min_sec)} / ${fmtSecondsPrecise(probe.latency_max_sec)}`,
    },
    { label: "Amostras de delay", value: text(latencySampleCount, "0") },
  ];

  ui.pivotMetrics.innerHTML = cards
    .map((item) => {
      return `
      <div class="metric">
        <div class="label">${escapeHtml(item.label)}</div>
        <div class="value">${escapeHtml(item.value)}</div>
      </div>`;
    })
    .join("");
}

function connectivityRangeSeconds(presetValue) {
  if (presetValue === "24h") return 24 * 3600;
  if (presetValue === "48h") return 48 * 3600;
  if (presetValue === "7d") return 7 * 86400;
  if (presetValue === "15d") return 15 * 86400;
  if (presetValue === "30d") return 30 * 86400;
  return null;
}

function normalizeRange(pivot) {
  const nowTs = Number(pivot.updated_at_ts || Math.floor(Date.now() / 1000));
  const timeline = pivot.timeline || [];
  let minTs = nowTs;
  for (const event of timeline) {
    const ts = Number(event.ts || 0);
    if (Number.isFinite(ts) && ts > 0 && ts < minTs) minTs = ts;
  }
  if (!timeline.length) minTs = nowTs - 24 * 3600;

  let startTs = minTs;
  let endTs = nowTs;

  const preset = state.connPreset;
  if (preset === "custom") {
    const parsedFrom = parseDateTimeLocal(state.connCustomFrom);
    const parsedTo = parseDateTimeLocal(state.connCustomTo);
    if (parsedFrom !== null) startTs = parsedFrom;
    if (parsedTo !== null) endTs = parsedTo;
  } else if (preset !== "total") {
    const windowSec = connectivityRangeSeconds(preset);
    if (windowSec) startTs = nowTs - windowSec;
  }

  if (startTs < minTs) startTs = minTs;
  if (endTs > nowTs) endTs = nowTs;
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs >= endTs) {
    startTs = Math.max(minTs, nowTs - 24 * 3600);
    endTs = nowTs;
  }

  return { startTs, endTs, minTs, nowTs };
}

function normalizeProbeDelayRange(pivot) {
  const nowTs = Number(pivot.updated_at_ts || Math.floor(Date.now() / 1000));
  const probeEvents = pivot.probe_events || [];
  let minTs = nowTs;

  for (const event of probeEvents) {
    if (String(event.type || "").toLowerCase() !== "response") continue;
    const ts = Number(event.ts || 0);
    if (Number.isFinite(ts) && ts > 0 && ts < minTs) minTs = ts;
  }

  if (minTs === nowTs) minTs = Math.max(0, nowTs - 24 * 3600);

  let startTs = minTs;
  let endTs = nowTs;

  const preset = state.probeDelayPreset;
  if (preset === "custom") {
    const parsedFrom = parseDateTimeLocal(state.probeDelayCustomFrom);
    const parsedTo = parseDateTimeLocal(state.probeDelayCustomTo);
    if (parsedFrom !== null) startTs = parsedFrom;
    if (parsedTo !== null) endTs = parsedTo;
  } else if (preset !== "total") {
    const windowSec = connectivityRangeSeconds(preset);
    if (windowSec) startTs = nowTs - windowSec;
  }

  if (startTs < minTs) startTs = minTs;
  if (endTs > nowTs) endTs = nowTs;
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs >= endTs) {
    startTs = Math.max(minTs, nowTs - 24 * 3600);
    endTs = nowTs;
  }

  return { startTs, endTs, minTs, nowTs };
}

function buildProbeDelaySeries(pivot, startTs, endTs) {
  const responses = (pivot.probe_events || [])
    .filter((event) => String(event.type || "").toLowerCase() === "response")
    .map((event) => ({
      ts: Number(event.ts || 0),
      latencySec: Number(event.latency_sec),
    }))
    .filter(
      (event) =>
        Number.isFinite(event.ts) &&
        event.ts >= startTs &&
        event.ts <= endTs &&
        Number.isFinite(event.latencySec) &&
        event.latencySec >= 0
    )
    .sort((a, b) => a.ts - b.ts);

  const points = [];
  let sum = 0;
  let count = 0;
  for (const response of responses) {
    count += 1;
    sum += response.latencySec;
    points.push({
      ts: response.ts,
      latencySec: response.latencySec,
      sampleCount: count,
      avgLatencySec: sum / count,
    });
  }

  return {
    points,
    responseCount: responses.length,
  };
}

function buildConnectivitySegments(pivot, startTs, endTs) {
  const summary = pivot.summary || {};
  const settings = (state.rawState || {}).settings || {};
  const tolerance = Number(settings.tolerance_factor || 1.25) || 1.25;
  const monitoredTopics = ["cloudv2", "cloudv2-ping", "cloudv2-info", "cloudv2-network"];
  const expectedByTopic = summary.expected_by_topic_sec || {};

  const candidates = [];
  for (const topic of monitoredTopics) {
    const value = Number(expectedByTopic[topic]);
    if (Number.isFinite(value) && value > 0) candidates.push(value);
  }
  const maxExpectedFromSummary = Number(summary.max_expected_interval_sec || 0);
  const maxExpectedIntervalSec =
    Number.isFinite(maxExpectedFromSummary) && maxExpectedFromSummary > 0
      ? maxExpectedFromSummary
      : candidates.length
      ? Math.max(...candidates)
      : Number(settings.ping_expected_sec || 180);
  const thresholdFromSummary = Number(summary.disconnect_threshold_sec || 0);
  const disconnectThresholdSec =
    Number.isFinite(thresholdFromSummary) && thresholdFromSummary > 0
      ? thresholdFromSummary
      : Math.max(30, maxExpectedIntervalSec * tolerance);

  const events = (pivot.timeline || [])
    .filter((event) => monitoredTopics.includes(String(event.topic || "")))
    .map((event) => ({ topic: String(event.topic || ""), ts: Number(event.ts || 0) }))
    .filter((event) => Number.isFinite(event.ts) && event.ts > 0 && event.ts <= endTs)
    .sort((a, b) => a.ts - b.ts);

  let lastMessageTs = null;
  let idx = 0;

  while (idx < events.length && events[idx].ts <= startTs) {
    lastMessageTs = events[idx].ts;
    idx += 1;
  }

  const totalRange = endTs - startTs;
  let stepSec = 60;
  if (totalRange > 7 * 86400) stepSec = 300;
  if (totalRange > 20 * 86400) stepSec = 900;

  const segments = [];
  let currentStart = startTs;
  let currentState = null;

  const flushSegment = (segmentEnd, stateName) => {
    if (currentState === null) return;
    if (segmentEnd <= currentStart) return;
    segments.push({
      state: stateName,
      start: currentStart,
      end: segmentEnd,
      duration: segmentEnd - currentStart,
    });
  };

  for (let cursor = startTs; cursor <= endTs; cursor += stepSec) {
    while (idx < events.length && events[idx].ts <= cursor) {
      lastMessageTs = events[idx].ts;
      idx += 1;
    }

    const connected = lastMessageTs !== null && cursor - lastMessageTs <= disconnectThresholdSec;
    const nextState = connected ? "connected" : "disconnected";

    if (currentState === null) {
      currentState = nextState;
      currentStart = cursor;
      continue;
    }
    if (nextState !== currentState) {
      flushSegment(cursor, currentState);
      currentState = nextState;
      currentStart = cursor;
    }
  }

  flushSegment(endTs, currentState || "disconnected");

  if (!segments.length) {
    segments.push({
      state: "disconnected",
      start: startTs,
      end: endTs,
      duration: Math.max(1, endTs - startTs),
    });
  }

  return {
    segments,
    disconnectThresholdSec,
    maxExpectedIntervalSec,
  };
}

function hideConnectivitySegmentCard() {
  ui.connSegmentCard.hidden = true;
  ui.connSegmentCard.innerHTML = "";
}

function clearConnectivitySegmentSelection() {
  state.connSelectedSegmentKey = null;
  for (const item of ui.connTrack.querySelectorAll(".conn-segment.selected")) {
    item.classList.remove("selected");
  }
  hideConnectivitySegmentCard();
}

function positionConnectivitySegmentCard(segmentEl) {
  const wrapEl = ui.connTrack.parentElement;
  if (!wrapEl || !segmentEl || ui.connSegmentCard.hidden) return;

  const segmentRect = segmentEl.getBoundingClientRect();
  const wrapRect = wrapEl.getBoundingClientRect();
  const centerX = segmentRect.left - wrapRect.left + segmentRect.width / 2;
  const topY = segmentRect.top - wrapRect.top;

  ui.connSegmentCard.style.top = `${Math.max(0, topY)}px`;
  ui.connSegmentCard.style.left = `${centerX}px`;

  const cardWidth = ui.connSegmentCard.offsetWidth || 0;
  const halfWidth = cardWidth / 2;
  const minCenter = halfWidth + 6;
  const maxCenter = Math.max(minCenter, wrapRect.width - halfWidth - 6);
  const clampedCenter = Math.min(maxCenter, Math.max(minCenter, centerX));
  ui.connSegmentCard.style.left = `${clampedCenter}px`;
}

function showConnectivitySegmentCard(segmentEl) {
  const segmentState = String(segmentEl.dataset.state || "disconnected");
  const startTs = Number(segmentEl.dataset.startTs || 0);
  const endTs = Number(segmentEl.dataset.endTs || 0);
  const durationSec = Number(segmentEl.dataset.durationSec || 0);
  const stateLabel = segmentState === "connected" ? "Conectado" : "Desconectado";

  ui.connSegmentCard.innerHTML = `
    <div class="conn-segment-card-title ${escapeHtml(segmentState)}">${escapeHtml(stateLabel)}</div>
    <div class="conn-segment-card-row"><span>Inicio</span><strong>${escapeHtml(formatShortDateTime(startTs))}</strong></div>
    <div class="conn-segment-card-row"><span>Fim</span><strong>${escapeHtml(formatShortDateTime(endTs))}</strong></div>
    <div class="conn-segment-card-row"><span>Duracao</span><strong>${escapeHtml(fmtDuration(durationSec))}</strong></div>
  `;
  ui.connSegmentCard.hidden = false;
  positionConnectivitySegmentCard(segmentEl);
}

function restoreConnectivitySegmentSelection() {
  if (!state.connSelectedSegmentKey) {
    hideConnectivitySegmentCard();
    return;
  }

  const selectedEl = [...ui.connTrack.querySelectorAll(".conn-segment")].find(
    (item) => (item.dataset.key || "") === state.connSelectedSegmentKey
  );
  if (!selectedEl) {
    state.connSelectedSegmentKey = null;
    hideConnectivitySegmentCard();
    return;
  }

  selectedEl.classList.add("selected");
  showConnectivitySegmentCard(selectedEl);
}

function renderConnectivityTimeline(pivot) {
  const range = normalizeRange(pivot);
  const startTs = range.startTs;
  const endTs = range.endTs;
  const duration = Math.max(1, endTs - startTs);
  const connData = buildConnectivitySegments(pivot, startTs, endTs);
  const segments = connData.segments;

  let connectedSec = 0;
  let disconnectedSec = 0;

  ui.connTrack.innerHTML = segments
    .map((segment) => {
      const leftPct = ((segment.start - startTs) / duration) * 100;
      const widthPct = Math.max(0.15, (segment.duration / duration) * 100);
      if (segment.state === "connected") connectedSec += segment.duration;
      else disconnectedSec += segment.duration;
      const segmentKey = `${segment.state}:${segment.start}:${segment.end}`;
      const selectedClass = state.connSelectedSegmentKey === segmentKey ? " selected" : "";
      return `<div class="conn-segment ${segment.state}${selectedClass}" style="left:${leftPct}%;width:${widthPct}%;" data-key="${escapeHtml(
        segmentKey
      )}" data-state="${escapeHtml(segment.state)}" data-start-ts="${segment.start}" data-end-ts="${segment.end}" data-duration-sec="${segment.duration}"></div>`;
    })
    .join("");

  const connectedPct = Math.round((connectedSec / duration) * 100);
  const disconnectedPct = Math.max(0, 100 - connectedPct);

  ui.connSummary.innerHTML = `
    <span class="conn-pill connected">Conectado: <strong>${fmtDuration(connectedSec)}</strong> (${connectedPct}%)</span>
    <span class="conn-pill disconnected">Desconectado: <strong>${fmtDuration(disconnectedSec)}</strong> (${disconnectedPct}%)</span>
    <span class="conn-pill">Janela off: <strong>${fmtDuration(connData.disconnectThresholdSec)}</strong> (max mediana ${fmtDuration(
    connData.maxExpectedIntervalSec
  )})</span>
  `;
  ui.connStartLabel.textContent = formatShortDateTime(startTs);
  ui.connEndLabel.textContent = formatShortDateTime(endTs);

  if (!state.connCustomFrom) {
    state.connCustomFrom = toDateTimeLocal(Math.max(range.minTs, endTs - 24 * 3600));
  }
  if (!state.connCustomTo) {
    state.connCustomTo = toDateTimeLocal(endTs);
  }
  ui.connFrom.value = state.connCustomFrom;
  ui.connTo.value = state.connCustomTo;
  restoreConnectivitySegmentSelection();
}

function renderProbeDelayChart(pivot) {
  const summary = pivot.summary || {};
  const probe = summary.probe || {};
  const enabled = !!probe.enabled;

  const range = normalizeProbeDelayRange(pivot);
  const startTs = range.startTs;
  const endTs = range.endTs;

  if (!state.probeDelayCustomFrom) {
    state.probeDelayCustomFrom = toDateTimeLocal(Math.max(range.minTs, endTs - 24 * 3600));
  }
  if (!state.probeDelayCustomTo) {
    state.probeDelayCustomTo = toDateTimeLocal(endTs);
  }

  ui.probeDelayPreset.value = state.probeDelayPreset;
  ui.probeDelayFrom.value = state.probeDelayCustomFrom;
  ui.probeDelayTo.value = state.probeDelayCustomTo;
  const custom = state.probeDelayPreset === "custom";
  ui.probeDelayFromWrap.hidden = !custom;
  ui.probeDelayToWrap.hidden = !custom;

  ui.probeDelayStartLabel.textContent = formatShortDateTime(startTs);
  ui.probeDelayEndLabel.textContent = formatShortDateTime(endTs);

  if (!enabled) {
    ui.probeDelayHint.textContent = "Monitoramento ativo desabilitado para este pivot.";
    ui.probeDelayChart.innerHTML = `<div class="empty">Habilite o probe para acompanhar o delay medio no tempo.</div>`;
    return;
  }

  const series = buildProbeDelaySeries(pivot, startTs, endTs);
  const points = series.points;
  if (!points.length) {
    ui.probeDelayHint.textContent = "Sem respostas de probe na janela selecionada.";
    ui.probeDelayChart.innerHTML = `<div class="empty">Nenhuma resposta #11$ no periodo selecionado.</div>`;
    return;
  }

  const width = 980;
  const height = 250;
  const padLeft = 52;
  const padRight = 14;
  const padTop = 16;
  const padBottom = 30;
  const innerWidth = width - padLeft - padRight;
  const innerHeight = height - padTop - padBottom;
  const duration = Math.max(1, endTs - startTs);

  const avgValues = points.map((item) => item.avgLatencySec);
  let minY = Math.min(...avgValues);
  let maxY = Math.max(...avgValues);
  let spread = maxY - minY;
  if (!Number.isFinite(spread) || spread <= 0) {
    spread = Math.max(0.05, maxY * 0.1);
  }
  const domainMin = Math.max(0, minY - spread * 0.2);
  let domainMax = maxY + spread * 0.2;
  if (!(domainMax > domainMin)) domainMax = domainMin + 0.1;
  const domainSpan = domainMax - domainMin;

  const xForTs = (ts) => padLeft + ((ts - startTs) / duration) * innerWidth;
  const yForDelay = (delaySec) => padTop + ((domainMax - delaySec) / domainSpan) * innerHeight;

  const pathData = points
    .map((point, idx) => `${idx === 0 ? "M" : "L"}${xForTs(point.ts).toFixed(2)} ${yForDelay(point.avgLatencySec).toFixed(2)}`)
    .join(" ");

  const yTickCount = 4;
  const yTicks = [];
  for (let idx = 0; idx <= yTickCount; idx += 1) {
    const ratio = idx / yTickCount;
    const value = domainMax - ratio * domainSpan;
    const y = padTop + ratio * innerHeight;
    yTicks.push({ value, y });
  }
  const lastPoint = points[points.length - 1];

  ui.probeDelayChart.innerHTML = `
    <svg class="probe-delay-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Grafico de delay medio do probe">
      ${yTicks
        .map(
          (tick) => `
            <line class="probe-delay-grid" x1="${padLeft}" y1="${tick.y.toFixed(2)}" x2="${(padLeft + innerWidth).toFixed(
              2
            )}" y2="${tick.y.toFixed(2)}"></line>
            <text class="probe-delay-ylabel" x="${(padLeft - 8).toFixed(2)}" y="${(tick.y + 4).toFixed(2)}">${escapeHtml(
            fmtSecondsPrecise(tick.value)
          )}</text>
          `
        )
        .join("")}
      <path class="probe-delay-line" d="${pathData}"></path>
    </svg>
  `;

  ui.probeDelayHint.textContent =
    `Respostas na janela: ${series.responseCount} | ` +
    `Delay medio final: ${fmtSecondsPrecise(lastPoint.avgLatencySec)} | ` +
    `Ultima amostra: ${fmtSecondsPrecise(lastPoint.latencySec)} (${formatShortDateTime(lastPoint.ts)})`;
}

function renderTimeline(pivot) {
  const allEvents = pivot.timeline || [];
  const totalPages = Math.max(1, Math.ceil(allEvents.length / state.timelinePageSize));
  if (state.timelinePage > totalPages) state.timelinePage = totalPages;
  const start = (state.timelinePage - 1) * state.timelinePageSize;
  const pageEvents = allEvents.slice(start, start + state.timelinePageSize);

  if (!pageEvents.length) {
    ui.timelineList.innerHTML = `<div class="empty">Sem eventos para o pivot selecionado.</div>`;
  } else {
    ui.timelineList.innerHTML = pageEvents
      .map((event) => {
        const type = text(event.type, "event");
        const title = text(EVENT_LABEL[type], type);
        const detailsJson = JSON.stringify(event.details || {}, null, 2);
        return `
          <article class="event ${escapeHtml(type)}">
            <div class="event-head">
              <div class="event-title">${escapeHtml(title)}</div>
              <div class="event-time">${escapeHtml(text(event.at))}</div>
            </div>
            <div class="event-topic">Topico: ${escapeHtml(text(event.topic))}</div>
            <div>${escapeHtml(text(event.summary, ""))}</div>
            <details class="event-details">
              <summary>Detalhes</summary>
              <pre>${escapeHtml(detailsJson)}</pre>
            </details>
          </article>
        `;
      })
      .join("");
  }

  ui.timelinePageInfo.textContent = `Pagina ${state.timelinePage}/${totalPages}`;
  ui.timelinePrev.disabled = state.timelinePage <= 1;
  ui.timelineNext.disabled = state.timelinePage >= totalPages;
}

function renderCloud2Table(pivot) {
  const rows = (pivot.cloud2_events || []).slice(0, 20);
  if (!rows.length) {
    ui.cloud2Table.innerHTML = `<tr><td colspan="5">Sem eventos cloud2.</td></tr>`;
    return;
  }

  ui.cloud2Table.innerHTML = rows
    .map((row) => {
      return `
      <tr>
        <td>${escapeHtml(text(row.at))}</td>
        <td>${escapeHtml(text(row.rssi))}</td>
        <td>${escapeHtml(text(row.technology))}</td>
        <td>${escapeHtml(fmtDuration(row.drop_duration_sec))}</td>
        <td>${escapeHtml(text(row.firmware))}</td>
      </tr>
      `;
    })
    .join("");
}

function renderPivotView() {
  const pivot = state.pivotData;
  if (!pivot || !state.selectedPivot) {
    ui.pivotView.hidden = true;
    clearConnectivitySegmentSelection();
    return;
  }

  ui.pivotView.hidden = false;
  const summary = pivot.summary || {};
  const status = summary.status || {};
  const probe = summary.probe || {};

  ui.pivotTitle.textContent = text(pivot.pivot_id, "Pivot");
  ui.pivotStatus.textContent = text(status.label, "Inicial");
  ui.pivotStatus.className = `badge ${text(status.code, "gray")}`;

  ui.probeEnabled.checked = !!probe.enabled;
  ui.probeInterval.value = Number(probe.interval_sec || 0);
  const sentCount = Number(probe.sent_count || 0);
  const responseCount = Number(probe.response_count || 0);
  const responseCoverageText =
    sentCount > 0
      ? `${responseCount}/${sentCount} (${fmtPercent(probe.response_ratio_pct)})`
      : `${responseCount}/${sentCount}`;
  const probeHintParts = [
    `Ultimo envio: ${text(probe.last_sent_at)}`,
    `Ultima resposta: ${text(probe.last_response_at)}`,
    `Timeout streak: ${text(probe.timeout_streak, "0")}`,
    `Resp/envio: ${responseCoverageText}`,
    `Delay ultimo: ${fmtSecondsPrecise(probe.latency_last_sec)}`,
    `Delay medio: ${fmtSecondsPrecise(probe.latency_avg_sec)}`,
  ];
  ui.probeHint.textContent = probeHintParts.join(" | ");

  renderPivotMetrics(pivot);
  renderConnectivityTimeline(pivot);
  renderProbeDelayChart(pivot);
  renderTimeline(pivot);
  renderCloud2Table(pivot);
}

async function loadUiConfig() {
  try {
    const config = await getJson("data/ui_config.json");
    const refreshSec = Number(config.refresh_sec || 5);
    if (Number.isFinite(refreshSec) && refreshSec >= 1) {
      state.refreshMs = refreshSec * 1000;
    }
  } catch (err) {
    state.refreshMs = 5000;
  }
}

async function refreshState() {
  const data = await getJson("/api/state");
  state.rawState = data;
  state.pivots = data.pivots || [];
  renderHeader();
  renderStatusSummary();
  renderPending();
  renderCards();
}

async function refreshPivot() {
  if (!state.selectedPivot) {
    state.pivotData = null;
    renderPivotView();
    return;
  }

  try {
    const pivot = await getJson(`/api/pivot/${encodeURIComponent(state.selectedPivot)}`);
    state.pivotData = pivot;
  } catch (err) {
    state.pivotData = null;
  }
  renderPivotView();
}

async function refreshAll() {
  try {
    await refreshState();
    await refreshPivot();
  } catch (err) {
    ui.cardsGrid.innerHTML = `<div class="empty">Falha ao carregar dados do monitor. Tentando novamente...</div>`;
  }
}

async function openPivot(pivotId) {
  const id = String(pivotId || "").trim();
  if (!id) return;
  state.connSelectedSegmentKey = null;
  state.selectedPivot = id;
  state.timelinePage = 1;
  setHashPivot(id);
  await refreshPivot();
  ui.pivotView.scrollIntoView({ behavior: "smooth", block: "start" });
}

function closePivot() {
  state.connSelectedSegmentKey = null;
  state.selectedPivot = null;
  state.pivotData = null;
  setHashPivot("");
  renderPivotView();
}

async function saveProbeSetting() {
  if (!state.selectedPivot) return;

  const payload = {
    pivot_id: state.selectedPivot,
    enabled: !!ui.probeEnabled.checked,
    interval_sec: Number(ui.probeInterval.value || 0),
  };

  ui.saveProbe.disabled = true;
  try {
    const response = await fetch("/api/probe-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    ui.probeHint.textContent = "Configuracao de probe salva com sucesso.";
    await refreshAll();
  } catch (err) {
    ui.probeHint.textContent = `Falha ao salvar probe: ${err.message}`;
  } finally {
    ui.saveProbe.disabled = false;
  }
}

function wireEvents() {
  ui.searchInput.addEventListener("input", () => {
    state.search = ui.searchInput.value || "";
    state.cardsPage = 1;
    renderCards();
  });

  ui.sortSelect.addEventListener("change", () => {
    state.sort = ui.sortSelect.value || "critical";
    state.cardsPage = 1;
    renderCards();
  });

  ui.statusFilters.addEventListener("click", (event) => {
    const btn = event.target.closest("button[data-status]");
    if (!btn) return;

    for (const item of ui.statusFilters.querySelectorAll("button[data-status]")) {
      item.classList.remove("active");
    }
    btn.classList.add("active");
    state.statusFilter = btn.dataset.status || "all";
    state.cardsPage = 1;
    renderCards();
  });

  ui.cardsPrev.addEventListener("click", () => {
    state.cardsPage = Math.max(1, state.cardsPage - 1);
    renderCards();
  });

  ui.cardsNext.addEventListener("click", () => {
    const total = Math.max(1, Math.ceil(applyFilterSort().length / state.cardsPageSize));
    state.cardsPage = Math.min(total, state.cardsPage + 1);
    renderCards();
  });

  ui.closePivot.addEventListener("click", closePivot);
  ui.saveProbe.addEventListener("click", saveProbeSetting);

  ui.connPreset.addEventListener("change", () => {
    state.connPreset = ui.connPreset.value || "30d";
    const custom = state.connPreset === "custom";
    ui.connFromWrap.hidden = !custom;
    ui.connToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.connApply.addEventListener("click", () => {
    state.connPreset = ui.connPreset.value || "30d";
    state.connCustomFrom = ui.connFrom.value || "";
    state.connCustomTo = ui.connTo.value || "";
    const custom = state.connPreset === "custom";
    ui.connFromWrap.hidden = !custom;
    ui.connToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.probeDelayPreset.addEventListener("change", () => {
    state.probeDelayPreset = ui.probeDelayPreset.value || "30d";
    const custom = state.probeDelayPreset === "custom";
    ui.probeDelayFromWrap.hidden = !custom;
    ui.probeDelayToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.probeDelayApply.addEventListener("click", () => {
    state.probeDelayPreset = ui.probeDelayPreset.value || "30d";
    state.probeDelayCustomFrom = ui.probeDelayFrom.value || "";
    state.probeDelayCustomTo = ui.probeDelayTo.value || "";
    const custom = state.probeDelayPreset === "custom";
    ui.probeDelayFromWrap.hidden = !custom;
    ui.probeDelayToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.connTrack.addEventListener("click", (event) => {
    const segmentEl = event.target.closest(".conn-segment");
    if (!segmentEl || !ui.connTrack.contains(segmentEl)) return;

    const clickedKey = segmentEl.dataset.key || "";
    if (!clickedKey) return;

    if (state.connSelectedSegmentKey === clickedKey) {
      clearConnectivitySegmentSelection();
      return;
    }

    for (const item of ui.connTrack.querySelectorAll(".conn-segment.selected")) {
      item.classList.remove("selected");
    }
    state.connSelectedSegmentKey = clickedKey;
    segmentEl.classList.add("selected");
    showConnectivitySegmentCard(segmentEl);
  });

  document.addEventListener("click", (event) => {
    if (!state.connSelectedSegmentKey) return;
    const wrapEl = ui.connTrack.parentElement;
    if (wrapEl && wrapEl.contains(event.target)) return;
    clearConnectivitySegmentSelection();
  });

  ui.timelinePrev.addEventListener("click", () => {
    state.timelinePage = Math.max(1, state.timelinePage - 1);
    renderPivotView();
  });
  ui.timelineNext.addEventListener("click", () => {
    const total = Math.max(1, Math.ceil(((state.pivotData || {}).timeline || []).length / state.timelinePageSize));
    state.timelinePage = Math.min(total, state.timelinePage + 1);
    renderPivotView();
  });

  window.addEventListener("hashchange", () => {
    const hashPivot = parseHashPivot();
    if (!hashPivot) {
      closePivot();
      return;
    }
    openPivot(hashPivot);
  });

  window.addEventListener("resize", () => {
    if (!state.connSelectedSegmentKey || ui.connSegmentCard.hidden) return;
    const selectedEl = [...ui.connTrack.querySelectorAll(".conn-segment")].find(
      (item) => (item.dataset.key || "") === state.connSelectedSegmentKey
    );
    if (!selectedEl) {
      clearConnectivitySegmentSelection();
      return;
    }
    positionConnectivitySegmentCard(selectedEl);
  });
}

async function boot() {
  wireEvents();
  startDevAutoReload();
  await loadUiConfig();
  ui.connPreset.value = state.connPreset;
  ui.connFromWrap.hidden = true;
  ui.connToWrap.hidden = true;
  ui.probeDelayPreset.value = state.probeDelayPreset;
  ui.probeDelayFromWrap.hidden = true;
  ui.probeDelayToWrap.hidden = true;
  const hashPivot = parseHashPivot();
  if (hashPivot) {
    state.selectedPivot = hashPivot;
  }
  await refreshAll();
  setInterval(refreshAll, state.refreshMs);
}

boot();
