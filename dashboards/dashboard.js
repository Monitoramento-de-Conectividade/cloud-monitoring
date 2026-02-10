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
  pivotQuality: document.getElementById("pivotQuality"),
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
  probeStatLastSent: document.getElementById("probeStatLastSent"),
  probeStatLastResponse: document.getElementById("probeStatLastResponse"),
  probeStatTimeoutStreak: document.getElementById("probeStatTimeoutStreak"),
  probeStatResponseRatio: document.getElementById("probeStatResponseRatio"),
  probeStatDelayLast: document.getElementById("probeStatDelayLast"),
  probeStatDelayAvg: document.getElementById("probeStatDelayAvg"),
  probeDelayPreset: document.getElementById("probeDelayPreset"),
  probeDelayRange: document.getElementById("probeDelayRange"),
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
  toastRegion: document.getElementById("toastRegion"),
  sessionAction: document.getElementById("sessionAction"),
  sessionSelectWrap: document.getElementById("sessionSelectWrap"),
  sessionSelect: document.getElementById("sessionSelect"),
  sessionApply: document.getElementById("sessionApply"),
  purgeDatabase: document.getElementById("purgeDatabase"),
  sessionHint: document.getElementById("sessionHint"),
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
  toastSeq: 0,
  lastRefreshToastAtMs: 0,
  qualityOverridesByPivotId: {},
  statusOverridesByPivotId: {},
  qualityRefreshSeq: 0,
  sessionAction: "new",
  availableRuns: [],
  selectedRunId: null,
  selectedHistoryRunId: null,
  panelSessionMeta: null,
  panelRunMeta: null,
};

const STATUS_META = {
  all: { label: "Todos", css: "gray", rank: 99 },
  green: { label: "Online", css: "green", rank: 2 },
  yellow: { label: "Atencao", css: "yellow", rank: 99 },
  critical: { label: "Critico", css: "critical", rank: 99 },
  red: { label: "Offline", css: "red", rank: 0 },
  gray: { label: "Inicial", css: "gray", rank: 1 },
};

const QUALITY_META = {
  critical: { label: "Critico", rank: 0 },
  yellow: { label: "Atencao", rank: 1 },
  calculating: { label: "Calculando", rank: 2 },
  green: { label: "Saudavel", rank: 3 },
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
  session_started: "Nova sessao",
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

function fmtPercentWhole(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "-";
  return `${Math.round(n)}%`;
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

function getDisplayQuality(item) {
  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.qualityOverridesByPivotId[pivotId] : null;
  const base = override || ((item || {}).quality || {});
  const displayStatus = getDisplayStatus(item);
  const statusCode = text(displayStatus.code, "gray");
  const statusReason = text(displayStatus.reason, "");
  if (statusCode === "gray") {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: statusReason || "Aguardando amostras de cloudv2 para estimar mediana.",
      rank: QUALITY_META.calculating.rank,
    };
  }
  const code = text(base.code, "green");
  const meta = QUALITY_META[code] || QUALITY_META.green;
  const rankRaw = Number(base.rank);

  return {
    code,
    label: text(base.label, meta.label),
    reason: text(base.reason, ""),
    rank: Number.isFinite(rankRaw) ? rankRaw : meta.rank,
  };
}

function getDisplayStatus(item) {
  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.statusOverridesByPivotId[pivotId] : null;
  const base = override || ((item || {}).status || {});
  const code = text(base.code, "gray");
  const meta = STATUS_META[code] || STATUS_META.gray;
  const rankRaw = Number(base.rank);

  return {
    code,
    label: text(base.label, meta.label),
    reason: text(base.reason, ""),
    rank: Number.isFinite(rankRaw) ? rankRaw : Number(meta.rank ?? 99),
  };
}

function showToast(message, level = "success", ttlMs = 3200) {
  if (!ui.toastRegion) return;
  const textMessage = String(message || "").trim();
  if (!textMessage) return;

  const toast = document.createElement("div");
  const normalizedLevel = ["success", "error", "warn"].includes(level) ? level : "success";
  toast.className = `toast ${normalizedLevel}`;
  toast.setAttribute("role", normalizedLevel === "error" ? "alert" : "status");
  toast.setAttribute("aria-live", normalizedLevel === "error" ? "assertive" : "polite");
  toast.innerHTML = `
    <span class="toast-dot" aria-hidden="true"></span>
    <div>${escapeHtml(textMessage)}</div>
  `;
  toast.dataset.toastId = `toast-${Date.now()}-${state.toastSeq++}`;
  ui.toastRegion.appendChild(toast);

  const leaveDelay = Math.max(850, Number(ttlMs) - 260);
  window.setTimeout(() => {
    toast.classList.add("leave");
  }, leaveDelay);
  window.setTimeout(() => {
    toast.remove();
  }, Math.max(900, Number(ttlMs)));
}

async function getJson(url) {
  const response = await fetch(`${url}${url.includes("?") ? "&" : "?"}t=${Date.now()}`);
  if (!response.ok) throw new Error(`${response.status}`);
  return response.json();
}

function buildStateUrl(runId = null) {
  const normalizedRun = String(runId || "").trim();
  if (!normalizedRun) return "/api/state";
  return `/api/state?run_id=${encodeURIComponent(normalizedRun)}`;
}

function buildPivotPanelUrl(pivotId, runId = null, sessionId = null) {
  const normalized = String(pivotId || "").trim();
  if (!normalized) return "";
  const params = new URLSearchParams();
  const normalizedRun = String(runId || "").trim();
  if (normalizedRun) {
    params.set("run_id", normalizedRun);
  }
  const normalizedSession = String(sessionId || "").trim();
  if (normalizedSession) {
    params.set("session_id", normalizedSession);
  }
  const query = params.toString();
  const base = `/api/pivot/${encodeURIComponent(normalized)}/panel`;
  return query ? `${base}?${query}` : base;
}

function formatRunOption(run) {
  const item = run || {};
  const startedAt = text(item.started_at, "-");
  const duration = fmtDuration(item.duration_sec);
  const source = text(item.source, "runtime");
  const activeTag = item.is_active ? "Ativo" : "Historico";
  const pivots = Number(item.pivot_count || 0);
  return `${activeTag} | ${startedAt} | ${duration} | ${source} | ${pivots} pivots`;
}

function renderSessionControls() {
  const isHistory = state.sessionAction === "history";
  const runs = Array.isArray(state.availableRuns) ? state.availableRuns : [];
  const currentRun = state.rawState?.run || state.panelRunMeta || null;
  const currentRunLabel = currentRun
    ? `Monitoramento atual: ${text(currentRun.started_at)} (${currentRun.is_active ? "ativo" : "historico"})`
    : "";

  ui.sessionAction.value = state.sessionAction;
  ui.sessionSelectWrap.hidden = !isHistory;

  if (isHistory) {
    const selectedRunId = String(state.selectedHistoryRunId || state.selectedRunId || "").trim();

    if (!runs.length) {
      ui.sessionSelect.innerHTML = `<option value="">Nenhum historico salvo</option>`;
      ui.sessionSelect.disabled = true;
      ui.sessionApply.disabled = true;
      ui.sessionHint.textContent = "Sem monitoramentos globais salvos no banco.";
      return;
    }

    ui.sessionSelect.innerHTML = runs
      .map((run) => {
        const runId = text(run.run_id, "");
        const selected = selectedRunId && selectedRunId === runId ? " selected" : "";
        return `<option value="${escapeHtml(runId)}"${selected}>${escapeHtml(formatRunOption(run))}</option>`;
      })
      .join("");
    ui.sessionSelect.disabled = false;
    if (!ui.sessionSelect.value && runs.length) {
      ui.sessionSelect.value = text(runs[0].run_id, "");
    }
    ui.sessionApply.disabled = !ui.sessionSelect.value;
    ui.sessionHint.textContent =
      "Selecione um monitoramento global salvo e clique em Aplicar para carregar o historico completo." +
      (currentRunLabel ? ` ${currentRunLabel}` : "");
    return;
  }

  ui.sessionSelect.innerHTML = "";
  ui.sessionSelect.disabled = true;
  ui.sessionApply.disabled = false;
  ui.sessionHint.textContent =
    "Clique em Aplicar para iniciar um novo monitoramento global para todo o sistema." +
    (currentRunLabel ? ` ${currentRunLabel}` : "");
}

async function loadMonitoringRuns() {
  try {
    const payload = await getJson("/api/monitoring/runs");
    state.availableRuns = Array.isArray(payload.runs) ? payload.runs : [];
  } catch (err) {
    state.availableRuns = [];
  }
  renderSessionControls();
}

async function applyMonitoringSessionAction() {
  if (state.sessionAction === "new") {
    ui.sessionApply.disabled = true;
    try {
      const response = await fetch("/api/monitoring/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source: "ui_header_global" }),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.error || `HTTP ${response.status}`);
      }
      const created = data.created || {};
      const runId = text(created.run_id, "").trim();
      state.selectedRunId = null;
      state.selectedHistoryRunId = runId || null;
      await loadMonitoringRuns();
      await refreshAll();
      showToast("Novo monitoramento global iniciado.", "success", 3200);
    } catch (err) {
      ui.sessionHint.textContent = `Falha ao iniciar monitoramento global: ${err.message}`;
      showToast(`Falha ao iniciar monitoramento global: ${err.message}`, "error", 4000);
    } finally {
      renderSessionControls();
    }
    return;
  }

  const selectedRunId = text(ui.sessionSelect.value, "").trim();
  if (!selectedRunId) {
    ui.sessionHint.textContent = "Selecione um historico valido para carregar.";
    return;
  }
  ui.sessionApply.disabled = true;
  try {
    const response = await fetch("/api/monitoring/history", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_id: selectedRunId, source: "ui_header_global" }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    state.selectedHistoryRunId = selectedRunId;
    state.selectedRunId = selectedRunId;
    await refreshAll();
    showToast("Historico global carregado.", "success", 2800);
  } catch (err) {
    ui.sessionHint.textContent = `Falha ao carregar historico global: ${err.message}`;
    showToast(`Falha ao carregar historico global: ${err.message}`, "error", 3800);
  } finally {
    renderSessionControls();
  }
}

async function purgeDatabaseRecords() {
  const confirmed = window.confirm(
    "Essa acao vai APAGAR TODOS os dados do banco (runs, sessoes, snapshots e eventos). Deseja continuar?"
  );
  if (!confirmed) return;

  const password = window.prompt("Digite a senha para deletar todos os dados do banco:");
  if (password === null) return;
  const normalizedPassword = String(password);
  if (!normalizedPassword.trim()) {
    showToast("Senha obrigatoria para limpar o banco.", "warn", 3200);
    return;
  }

  ui.purgeDatabase.disabled = true;
  try {
    const response = await fetch("/api/admin/purge-database", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        password: normalizedPassword,
        source: "ui_header_global",
      }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }

    state.availableRuns = [];
    state.selectedRunId = null;
    state.selectedHistoryRunId = null;
    closePivot();
    await loadMonitoringRuns();
    await refreshAll();
    showToast("Todos os dados do banco foram removidos.", "success", 3600);
  } catch (err) {
    showToast(`Falha ao limpar banco: ${err.message}`, "error", 4200);
  } finally {
    ui.purgeDatabase.disabled = false;
    renderSessionControls();
  }
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
    const quality = getDisplayQuality(item);
    const status = getDisplayStatus(item);
    if (state.statusFilter !== "all") {
      const selectedCode = state.statusFilter;
      const stateCode = status.code;
      const qualityCode = quality.code;
      if (selectedCode === "quality_green") {
        if (qualityCode !== "green") return false;
      } else if (selectedCode === "quality_calculating") {
        if (qualityCode !== "calculating") return false;
      } else if (selectedCode === "yellow" || selectedCode === "critical") {
        if (qualityCode !== selectedCode) return false;
      } else if (stateCode !== selectedCode) {
        return false;
      }
    }
    if (needle) {
      const pivotId = String(item.pivot_id || "").toLowerCase();
      if (!pivotId.includes(needle)) return false;
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

    const aStateRank = Number(getDisplayStatus(a).rank ?? 99);
    const bStateRank = Number(getDisplayStatus(b).rank ?? 99);
    if (aStateRank !== bStateRank) return aStateRank - bStateRank;

    const aQualityRank = Number(getDisplayQuality(a).rank ?? 99);
    const bQualityRank = Number(getDisplayQuality(b).rank ?? 99);
    if (aQualityRank !== bQualityRank) return aQualityRank - bQualityRank;

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
  const stateCounts = { all: 0, green: 0, red: 0, gray: 0 };
  const qualityCounts = { green: 0, yellow: 0, critical: 0, calculating: 0 };
  for (const pivot of state.pivots) {
    stateCounts.all += 1;
    const stateCode = getDisplayStatus(pivot).code;
    if (stateCounts[stateCode] !== undefined) stateCounts[stateCode] += 1;

    const qualityCode = getDisplayQuality(pivot).code;
    if (qualityCounts[qualityCode] !== undefined) qualityCounts[qualityCode] += 1;
  }

  ui.statusSummary.innerHTML = `
    <div class="summary-pill"><span>Total</span><strong>${stateCounts.all}</strong></div>
    <div class="summary-pill"><span>Estado Online</span><strong>${stateCounts.green}</strong></div>
    <div class="summary-pill"><span>Estado Offline</span><strong>${stateCounts.red}</strong></div>
    <div class="summary-pill"><span>Estado Inicial</span><strong>${stateCounts.gray}</strong></div>
    <div class="summary-pill"><span>Qualidade Saudavel</span><strong>${qualityCounts.green}</strong></div>
    <div class="summary-pill"><span>Qualidade Calculando</span><strong>${qualityCounts.calculating}</strong></div>
    <div class="summary-pill"><span>Qualidade Atencao</span><strong>${qualityCounts.yellow}</strong></div>
    <div class="summary-pill"><span>Qualidade Critico</span><strong>${qualityCounts.critical}</strong></div>
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
        const status = getDisplayStatus(pivot);
        const quality = getDisplayQuality(pivot);
        const statusCode = text(status.code, "gray");
        const statusLabel = escapeHtml(text(status.label, "Inicial"));
        const qualityCode = text(quality.code, "green");
        const qualityLabel = escapeHtml(text(quality.label, "Saudavel"));
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
              <div class="badge-stack">
                <span class="badge ${statusCode}">Estado: ${statusLabel}</span>
                <span class="badge ${qualityCode}">Qualidade: ${qualityLabel}</span>
              </div>
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

function renderPivotMetrics(pivot, qualityView = null, connectivityView = null) {
  const summary = pivot.summary || {};
  const metrics = pivot.metrics || {};
  const status = summary.status || {};
  const quality = qualityView || summary.quality || {};
  const probe = summary.probe || {};
  const sentCount = Number(probe.sent_count || 0);
  const responseCount = Number(probe.response_count || 0);
  const timeoutCount = Number(probe.timeout_count || 0);
  const latencySampleCount = Number(probe.latency_sample_count || 0);
  const responseCoverageText =
    sentCount > 0
      ? `${responseCount}/${sentCount} (${fmtPercent(probe.response_ratio_pct)})`
      : `${responseCount}/${sentCount}`;
  const connectedPctText =
    connectivityView && Number.isFinite(Number(connectivityView.connectedPct))
      ? fmtPercentWhole(connectivityView.connectedPct)
      : fmtPercent(summary.connected_pct);
  const disconnectedPctText =
    connectivityView && Number.isFinite(Number(connectivityView.disconnectedPct))
      ? fmtPercentWhole(connectivityView.disconnectedPct)
      : fmtPercent(summary.disconnected_pct);

  const cards = [
    { label: "Estado atual", value: text(status.label) },
    { label: "Motivo estado", value: text(status.reason) },
    { label: "Qualidade", value: text(quality.label, "Saudavel") },
    { label: "Motivo qualidade", value: text(quality.reason) },
    { label: "% Conectado (janela)", value: connectedPctText },
    { label: "% Desconectado (janela)", value: disconnectedPctText },
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
  const persistedPoints = (pivot.probe_delay_points || [])
    .map((point) => ({
      ts: Number(point.ts || 0),
      latencySec: Number(point.latency_sec),
      sampleCount: Number(point.sample_count || 0),
      avgLatencySec: Number(point.avg_latency_sec),
    }))
    .filter(
      (point) =>
        Number.isFinite(point.ts) &&
        point.ts >= startTs &&
        point.ts <= endTs &&
        Number.isFinite(point.latencySec) &&
        point.latencySec >= 0 &&
        Number.isFinite(point.avgLatencySec) &&
        point.avgLatencySec >= 0
    )
    .sort((a, b) => a.ts - b.ts);

  if (persistedPoints.length) {
    return {
      points: persistedPoints,
      responseCount: persistedPoints.length,
    };
  }

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

  let hasPrincipalPayloadInWindow = false;
  let hasAuxPayloadInWindow = false;
  for (const event of events) {
    if (event.ts < startTs || event.ts > endTs) continue;
    if (event.topic === "cloudv2") hasPrincipalPayloadInWindow = true;
    if (event.topic === "cloudv2-ping" || event.topic === "cloudv2-network" || event.topic === "cloudv2-info") {
      hasAuxPayloadInWindow = true;
    }
  }

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
    hasPrincipalPayloadInWindow,
    hasAuxPayloadInWindow,
  };
}

function summarizeConnectivitySegments(segments, durationSec) {
  let connectedSec = 0;
  let disconnectedSec = 0;
  const safeDurationSec = Math.max(1, Number(durationSec || 0));

  for (const segment of segments || []) {
    const segmentDurationSec = Math.max(0, Number(segment.duration || 0));
    if (segmentDurationSec <= 0) continue;
    if (segment.state === "connected") connectedSec += segmentDurationSec;
    else disconnectedSec += segmentDurationSec;
  }

  const connectedPct = Math.max(0, Math.min(100, Math.round((connectedSec / safeDurationSec) * 100)));
  const disconnectedPct = Math.max(0, Math.min(100, 100 - connectedPct));

  return {
    connectedSec,
    disconnectedSec,
    connectedPct,
    disconnectedPct,
  };
}

function buildConnectivityQualityInput(connData, durationSec) {
  const summary = summarizeConnectivitySegments(connData.segments, durationSec);
  return {
    connectedSec: summary.connectedSec,
    disconnectedSec: summary.disconnectedSec,
    connectedPct: summary.connectedPct,
    disconnectedPct: summary.disconnectedPct,
    hasPrincipalPayloadInWindow: !!connData.hasPrincipalPayloadInWindow,
    hasAuxPayloadInWindow: !!connData.hasAuxPayloadInWindow,
  };
}

function computeConnectivityView(pivot) {
  const range = normalizeRange(pivot);
  const startTs = range.startTs;
  const endTs = range.endTs;
  const durationSec = Math.max(1, endTs - startTs);
  const connData = buildConnectivitySegments(pivot, startTs, endTs);
  const connectivityQualityInput = buildConnectivityQualityInput(connData, durationSec);

  return {
    range,
    startTs,
    endTs,
    durationSec,
    connData,
    segments: connData.segments,
    connectivityQualityInput,
  };
}

function buildQualityFromConnectivity(pivot, connectivitySummary) {
  const summary = pivot.summary || {};
  const fallbackQuality = summary.quality || {};
  const status = summary.status || {};
  const statusCode = text(status.code, "gray");
  const statusReason = text(status.reason, "");

  if (statusCode === "gray") {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: statusReason || "Aguardando amostras de cloudv2 para estimar mediana.",
      rank: QUALITY_META.calculating.rank,
    };
  }

  if (!connectivitySummary) {
    const fallbackCode = text(fallbackQuality.code, "green");
    return {
      code: fallbackCode,
      label: text(fallbackQuality.label, (QUALITY_META[fallbackCode] || QUALITY_META.green).label),
      reason: text(fallbackQuality.reason),
      rank: Number(fallbackQuality.rank ?? (QUALITY_META[fallbackCode] || QUALITY_META.green).rank),
    };
  }

  const settings = (state.rawState || {}).settings || {};
  const attentionThresholdRaw = Number(
    summary.attention_disconnected_pct_threshold ?? settings.attention_disconnected_pct_threshold ?? 20
  );
  const attentionThreshold = Number.isFinite(attentionThresholdRaw)
    ? Math.max(0, Math.min(100, attentionThresholdRaw))
    : 20;
  const criticalThresholdRaw = Number(
    summary.critical_disconnected_pct_threshold ?? settings.critical_disconnected_pct_threshold ?? 50
  );
  const criticalThreshold = Number.isFinite(criticalThresholdRaw)
    ? Math.max(attentionThreshold, Math.min(100, criticalThresholdRaw))
    : 50;

  const disconnectedPct = Math.max(0, Math.min(100, Math.round(Number(connectivitySummary.disconnectedPct || 0))));
  const criticalByDisconnectedPct = disconnectedPct > criticalThreshold;
  const attentionByDisconnectedPct = disconnectedPct > attentionThreshold;
  const attentionByOnlyAuxTopics =
    !connectivitySummary.hasPrincipalPayloadInWindow && connectivitySummary.hasAuxPayloadInWindow;

  let qualityCode = "green";
  let qualityReason = "Percentuais e topicos monitorados dentro da faixa saudavel na janela atual.";

  if (criticalByDisconnectedPct) {
    qualityCode = "critical";
    qualityReason =
      "Percentual desconectado na janela de monitoramento " +
      `(${disconnectedPct}%) acima do limite critico (${criticalThreshold.toFixed(1)}%).`;
  } else if (attentionByDisconnectedPct) {
    qualityCode = "yellow";
    qualityReason =
      "Percentual desconectado na janela de monitoramento " +
      `(${disconnectedPct}%) acima do limite de atencao (${attentionThreshold.toFixed(1)}%).`;
  } else if (attentionByOnlyAuxTopics) {
    qualityCode = "yellow";
    qualityReason =
      "Sem payload no topico principal cloudv2 na janela atual; " +
      "recebendo apenas ping/network/info.";
  }

  return {
    code: qualityCode,
    label: (QUALITY_META[qualityCode] || QUALITY_META.green).label,
    reason: qualityReason,
    rank: (QUALITY_META[qualityCode] || QUALITY_META.green).rank,
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
  const view = computeConnectivityView(pivot);
  const range = view.range;
  const startTs = view.startTs;
  const endTs = view.endTs;
  const duration = view.durationSec;
  const connData = view.connData;
  const segments = view.segments;
  const connectivitySummary = view.connectivityQualityInput;

  ui.connTrack.innerHTML = segments
    .map((segment) => {
      const leftPct = ((segment.start - startTs) / duration) * 100;
      const widthPct = Math.max(0.15, (segment.duration / duration) * 100);
      const segmentKey = `${segment.state}:${segment.start}:${segment.end}`;
      const selectedClass = state.connSelectedSegmentKey === segmentKey ? " selected" : "";
      return `<div class="conn-segment ${segment.state}${selectedClass}" style="left:${leftPct}%;width:${widthPct}%;" data-key="${escapeHtml(
        segmentKey
      )}" data-state="${escapeHtml(segment.state)}" data-start-ts="${segment.start}" data-end-ts="${segment.end}" data-duration-sec="${segment.duration}"></div>`;
    })
    .join("");

  ui.connSummary.innerHTML = `
    <span class="conn-pill connected">Conectado: <strong>${fmtDuration(connectivitySummary.connectedSec)}</strong> (${connectivitySummary.connectedPct}%)</span>
    <span class="conn-pill disconnected">Desconectado: <strong>${fmtDuration(connectivitySummary.disconnectedSec)}</strong> (${connectivitySummary.disconnectedPct}%)</span>
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
  return connectivitySummary;
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
  ui.probeDelayRange.hidden = !custom;
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
  const connectivitySummary = renderConnectivityTimeline(pivot);
  const quality = buildQualityFromConnectivity(pivot, connectivitySummary);
  const pivotId = text(pivot.pivot_id, "").trim();
  if (pivotId) {
    state.qualityOverridesByPivotId[pivotId] = quality;
    state.statusOverridesByPivotId[pivotId] = {
      code: text(status.code, "gray"),
      label: text(status.label, "Inicial"),
      reason: text(status.reason, ""),
      rank: Number(status.rank ?? 99),
    };
  }

  ui.pivotTitle.textContent = text(pivot.pivot_id, "Pivot");
  ui.pivotStatus.textContent = `Estado: ${text(status.label, "Inicial")}`;
  ui.pivotStatus.className = `badge ${text(status.code, "gray")}`;
  if (ui.pivotQuality) {
    ui.pivotQuality.textContent = `Qualidade: ${text(quality.label, "Saudavel")}`;
    ui.pivotQuality.className = `badge ${text(quality.code, "green")}`;
  }

  ui.probeEnabled.checked = !!probe.enabled;
  ui.probeInterval.value = Number(probe.interval_sec || 0);
  const sentCount = Number(probe.sent_count || 0);
  const responseCount = Number(probe.response_count || 0);
  const responseCoverageText =
    sentCount > 0
      ? `${responseCount}/${sentCount} (${fmtPercent(probe.response_ratio_pct)})`
      : `${responseCount}/${sentCount}`;
  ui.probeStatLastSent.textContent = text(probe.last_sent_at);
  ui.probeStatLastResponse.textContent = text(probe.last_response_at);
  ui.probeStatTimeoutStreak.textContent = text(probe.timeout_streak, "0");
  ui.probeStatResponseRatio.textContent = responseCoverageText;
  ui.probeStatDelayLast.textContent = fmtSecondsPrecise(probe.latency_last_sec);
  ui.probeStatDelayAvg.textContent = fmtSecondsPrecise(probe.latency_avg_sec);
  ui.probeHint.textContent = "";

  renderStatusSummary();
  renderCards();
  renderPivotMetrics(pivot, quality, connectivitySummary);
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
  const data = await getJson(buildStateUrl(state.selectedRunId));
  state.rawState = data;
  state.pivots = data.pivots || [];
  state.panelRunMeta = data.run || null;
  const currentPivotIds = new Set(state.pivots.map((item) => text(item.pivot_id, "").trim()).filter(Boolean));
  for (const pivotId of Object.keys(state.qualityOverridesByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.qualityOverridesByPivotId[pivotId];
    }
  }
  for (const pivotId of Object.keys(state.statusOverridesByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.statusOverridesByPivotId[pivotId];
    }
  }
  if (state.selectedPivot && !currentPivotIds.has(state.selectedPivot)) {
    closePivot();
  }
  renderHeader();
  renderStatusSummary();
  renderPending();
  renderCards();
}

async function refreshPivot() {
  if (!state.selectedPivot) {
    state.pivotData = null;
    state.panelSessionMeta = null;
    state.panelRunMeta = state.rawState?.run || null;
    renderSessionControls();
    renderPivotView();
    return;
  }

  const pivotId = String(state.selectedPivot || "").trim();
  const selectedRunId = text(state.selectedRunId, "").trim() || null;

  try {
    state.pivotData = await getJson(buildPivotPanelUrl(pivotId, selectedRunId));
  } catch (err) {
    state.pivotData = null;
  }

  if (state.pivotData) {
    state.panelSessionMeta = state.pivotData.session || null;
    state.panelRunMeta = state.pivotData.run || state.rawState?.run || null;
  } else {
    state.panelSessionMeta = null;
    state.panelRunMeta = state.rawState?.run || null;
  }
  renderSessionControls();
  renderPivotView();
}

async function refreshQualityOverrides() {
  const pivots = state.pivots || [];
  if (!pivots.length) {
    state.qualityOverridesByPivotId = {};
    state.statusOverridesByPivotId = {};
    return;
  }

  const seq = Number(state.qualityRefreshSeq || 0) + 1;
  state.qualityRefreshSeq = seq;

  const pivotIds = pivots.map((item) => text(item.pivot_id, "").trim()).filter(Boolean);
  const nextOverrides = {};
  const nextStatusOverrides = {};
  let cursor = 0;
  const workerCount = Math.max(1, Math.min(6, pivotIds.length));

  const runWorker = async () => {
    while (cursor < pivotIds.length) {
      const currentIndex = cursor;
      cursor += 1;
      const pivotId = pivotIds[currentIndex];
      if (!pivotId) continue;
      try {
        const pivotData = await getJson(buildPivotPanelUrl(pivotId, state.selectedRunId));
        const view = computeConnectivityView(pivotData);
        const quality = buildQualityFromConnectivity(pivotData, view.connectivityQualityInput);
        nextOverrides[pivotId] = quality;
        const summary = (pivotData || {}).summary || {};
        const detailStatus = summary.status || {};
        nextStatusOverrides[pivotId] = {
          code: text(detailStatus.code, "gray"),
          label: text(detailStatus.label, "Inicial"),
          reason: text(detailStatus.reason, ""),
          rank: Number(detailStatus.rank ?? 99),
        };
      } catch (err) {
        continue;
      }
    }
  };

  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
  if (seq !== state.qualityRefreshSeq) return;

  state.qualityOverridesByPivotId = nextOverrides;
  state.statusOverridesByPivotId = nextStatusOverrides;
  renderStatusSummary();
  renderCards();
}

async function refreshAll() {
  try {
    await refreshState();
    await refreshQualityOverrides();
    await refreshPivot();
    state.lastRefreshToastAtMs = 0;
  } catch (err) {
    ui.cardsGrid.innerHTML = `<div class="empty">Falha ao carregar dados do monitor. Tentando novamente...</div>`;
    const now = Date.now();
    if (now - Number(state.lastRefreshToastAtMs || 0) > 15000) {
      showToast("Falha ao carregar dados do monitor.", "error", 3800);
      state.lastRefreshToastAtMs = now;
    }
  }
}

async function openPivot(pivotId) {
  const id = String(pivotId || "").trim();
  if (!id) return;
  state.connSelectedSegmentKey = null;
  state.selectedPivot = id;
  state.panelSessionMeta = null;
  state.timelinePage = 1;
  setHashPivot(id);
  await refreshPivot();
  ui.pivotView.scrollIntoView({ behavior: "smooth", block: "start" });
}

function closePivot() {
  state.connSelectedSegmentKey = null;
  state.selectedPivot = null;
  state.pivotData = null;
  state.panelSessionMeta = null;
  state.panelRunMeta = state.rawState?.run || null;
  setHashPivot("");
  renderSessionControls();
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
    showToast("Configuracao de probe salva.", "success", 3000);
    await refreshAll();
  } catch (err) {
    ui.probeHint.textContent = `Falha ao salvar probe: ${err.message}`;
    showToast(`Falha ao salvar probe: ${err.message}`, "error", 4200);
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
  ui.sessionAction.addEventListener("change", async () => {
    state.sessionAction = ui.sessionAction.value || "new";
    if (state.sessionAction === "history") {
      await loadMonitoringRuns();
      return;
    }
    renderSessionControls();
  });
  ui.sessionApply.addEventListener("click", applyMonitoringSessionAction);
  ui.purgeDatabase.addEventListener("click", purgeDatabaseRecords);

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
    ui.probeDelayRange.hidden = !custom;
    ui.probeDelayFromWrap.hidden = !custom;
    ui.probeDelayToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.probeDelayApply.addEventListener("click", () => {
    state.probeDelayPreset = ui.probeDelayPreset.value || "30d";
    state.probeDelayCustomFrom = ui.probeDelayFrom.value || "";
    state.probeDelayCustomTo = ui.probeDelayTo.value || "";
    const custom = state.probeDelayPreset === "custom";
    ui.probeDelayRange.hidden = !custom;
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
  ui.probeDelayRange.hidden = true;
  ui.probeDelayFromWrap.hidden = true;
  ui.probeDelayToWrap.hidden = true;
  ui.sessionAction.value = state.sessionAction;
  const hashPivot = parseHashPivot();
  if (hashPivot) {
    state.selectedPivot = hashPivot;
  }
  await loadMonitoringRuns();
  renderSessionControls();
  await refreshAll();
  setInterval(refreshAll, state.refreshMs);
}

boot();
