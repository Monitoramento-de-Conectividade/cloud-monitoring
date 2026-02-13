const HAS_DOM = typeof document !== "undefined";
const HAS_WINDOW = typeof window !== "undefined";

function resolveApiBaseUrl() {
  if (!HAS_WINDOW) return "";
  const fromGlobal = String(window.CLOUDV2_API_BASE_URL || "").trim();
  if (fromGlobal) {
    return fromGlobal.replace(/\/+$/, "");
  }
  if (!HAS_DOM) return "";
  const meta = document.querySelector('meta[name="cloudv2-api-base-url"]');
  if (!meta) return "";
  const fromMeta = String(meta.getAttribute("content") || "").trim();
  return fromMeta.replace(/\/+$/, "");
}

const API_BASE_URL = resolveApiBaseUrl();

function buildApiUrl(url) {
  const normalized = String(url || "").trim();
  if (!normalized) return normalized;
  if (/^https?:\/\//i.test(normalized)) return normalized;
  if (!normalized.startsWith("/")) return normalized;
  const isApiPath =
    normalized.startsWith("/api/")
    || normalized.startsWith("/auth/")
    || normalized.startsWith("/account/")
    || normalized.startsWith("/admin/");
  if (!isApiPath || !API_BASE_URL) return normalized;
  return `${API_BASE_URL}${normalized}`;
}

const ui = HAS_DOM
  ? {
      updatedAt: document.getElementById("updatedAt"),
      countsMeta: document.getElementById("countsMeta"),
      logoutBtn: document.getElementById("logoutBtn"),
      adminCreateAccountLink: document.getElementById("adminCreateAccountLink"),
      adminUsersPanel: document.getElementById("adminUsersPanel"),
      adminUsersRefresh: document.getElementById("adminUsersRefresh"),
      adminUsersTable: document.getElementById("adminUsersTable"),
      adminUsersEmpty: document.getElementById("adminUsersEmpty"),
      searchInput: document.getElementById("searchInput"),
      sortSelect: document.getElementById("sortSelect"),
      clearFilters: document.getElementById("clearFilters"),
      technologyFilter: document.getElementById("technologyFilter"),
      firmwareFilter: document.getElementById("firmwareFilter"),
      statusFilterSelect: document.getElementById("statusFilterSelect"),
      connectivityFilterSelect: document.getElementById("connectivityFilterSelect"),
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
      deletePivotBtn: document.getElementById("deletePivotBtn"),
      pivotMoreInfoBtn: document.getElementById("pivotMoreInfoBtn"),
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
      sessionHint: document.getElementById("sessionHint"),
      initialLoadingOverlay: document.getElementById("initialLoadingOverlay"),
      initialLoadingText: document.getElementById("initialLoadingText"),
    }
  : {};

const state = {
  rawState: null,
  pivots: [],
  statusFilter: "all",
  connectivityFilter: "all",
  search: "",
  sort: "critical",
  technologyFilter: "all",
  firmwareFilter: "all",
  technologyOptions: [],
  firmwareOptions: [],
  cardsPage: 1,
  cardsPageSize: 18,
  selectedPivot: null,
  pivotData: null,
  pivotMetricsExpanded: false,
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
  selectedRunId: null,
  lastRunResolveAttemptMs: 0,
  runAutoDetected: false,
  panelSessionMeta: null,
  panelRunMeta: null,
  refreshInFlight: false,
  authUserRole: "user",
  authUserEmail: "",
  pivotDeleteAllowed: false,
  adminUsers: [],
};

const API_REQUEST_TIMEOUT_MS = 12000;
const CONNECTIVITY_EVENTS_MAX_PAGES = 3;

const STATUS_META = {
  all: { label: "Todos", css: "gray", rank: 99 },
  green: { label: "Conectado", css: "green", rank: 2 },
  yellow: { label: "Instável", css: "yellow", rank: 99 },
  critical: { label: "Crítico", css: "critical", rank: 99 },
  red: { label: "Desconectado", css: "red", rank: 0 },
  gray: { label: "Inicial", css: "gray", rank: 1 },
};

const QUALITY_META = {
  critical: { label: "Crítico", rank: 0 },
  yellow: { label: "Instável", rank: 1 },
  calculating: { label: "Em análise", rank: 2 },
  green: { label: "Estável", rank: 3 },
};

const EVENT_LABEL = {
  pivot_discovered: "Início de monitoramento",
  cloudv2: "Atualização de conectividade",
  ping: "Atualização de conectividade",
  cloud2: "Atualização de rede",
  probe_sent: "Solicitação de latência enviada",
  probe_response: "Resposta de latência recebida",
  probe_timeout: "Falha de resposta de latência",
  probe_response_unmatched: "Resposta fora da janela",
  session_started: "Nova sessão",
};

const INTERNAL_TERMS = [
  "cloudv2",
  "cloud2",
  "probe",
  "#11$",
  "payload",
  "topico",
  "tópico",
  "topic",
  "ping",
  "network",
  "mqtt",
];

const FIXED_PIVOT_DELETE_EMAIL = "eduardocostar03@gmail.com";

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

function normalizeFilterText(value) {
  return String(value || "").trim();
}

function normalizeFilterKey(value) {
  const normalized = normalizeFilterText(value);
  return normalized ? normalized.toLowerCase() : "";
}

function buildFilterOptionItems(values) {
  const byKey = new Map();
  for (const rawValue of values || []) {
    const label = normalizeFilterText(rawValue);
    if (!label) continue;
    const key = normalizeFilterKey(label);
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, label);
  }

  return [...byKey.entries()]
    .sort((a, b) => a[1].localeCompare(b[1], "pt-BR", { sensitivity: "base" }))
    .map(([key, label]) => ({ key, label }));
}

function pivotTechnologyValue(item) {
  const cloud2 = (item || {}).last_cloud2 || {};
  return normalizeFilterText(cloud2.technology);
}

function pivotFirmwareValue(item) {
  const cloud2 = (item || {}).last_cloud2 || {};
  return normalizeFilterText(cloud2.firmware);
}

function fallbackCloud2FilterOptionsFromPivots(pivots) {
  const technologies = [];
  const firmwares = [];
  for (const pivot of pivots || []) {
    const technology = pivotTechnologyValue(pivot);
    const firmware = pivotFirmwareValue(pivot);
    if (technology) technologies.push(technology);
    if (firmware) firmwares.push(firmware);
  }
  return { technologies, firmwares };
}

function renderDynamicSelectOptions(selectEl, defaultLabel, options, selectedKey) {
  if (!selectEl) return "all";

  const currentKey = normalizeFilterKey(selectedKey);
  const normalizedCurrent = currentKey || "all";
  const available = new Set(options.map((item) => item.key));
  const safeSelected = normalizedCurrent !== "all" && available.has(normalizedCurrent) ? normalizedCurrent : "all";

  const html = [`<option value="all">${escapeHtml(defaultLabel)}</option>`];
  for (const option of options) {
    const selected = option.key === safeSelected ? " selected" : "";
    html.push(`<option value="${escapeHtml(option.key)}"${selected}>${escapeHtml(option.label)}</option>`);
  }
  selectEl.innerHTML = html.join("");
  selectEl.value = safeSelected;
  return safeSelected;
}

function syncCloud2FilterOptions(rawState) {
  const raw = rawState || {};
  const fromState = raw.cloud2_filter_options && typeof raw.cloud2_filter_options === "object"
    ? raw.cloud2_filter_options
    : {};
  const fallback = fallbackCloud2FilterOptionsFromPivots(state.pivots);

  const technologiesSource = Array.isArray(fromState.technologies) && fromState.technologies.length
    ? fromState.technologies
    : fallback.technologies;
  const firmwaresSource = Array.isArray(fromState.firmwares) && fromState.firmwares.length
    ? fromState.firmwares
    : fallback.firmwares;

  const technologyOptions = buildFilterOptionItems(technologiesSource);
  const firmwareOptions = buildFilterOptionItems(firmwaresSource);
  state.technologyOptions = technologyOptions;
  state.firmwareOptions = firmwareOptions;

  state.technologyFilter = renderDynamicSelectOptions(
    ui.technologyFilter,
    "Todas tecnologias",
    technologyOptions,
    state.technologyFilter
  );
  state.firmwareFilter = renderDynamicSelectOptions(
    ui.firmwareFilter,
    "Todos firmwares",
    firmwareOptions,
    state.firmwareFilter
  );
}

function fmtDuration(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
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
  if (delta < 60) return `${delta}s atrás`;
  if (delta < 3600) return `${Math.floor(delta / 60)} min atrás`;
  if (delta < 86400) return `${(delta / 3600).toFixed(1)}h atrás`;
  return `${(delta / 86400).toFixed(1)}d atrás`;
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

function formatDateTimeValue(value) {
  if (value === null || value === undefined || value === "") return "-";

  const asNumber = Number(value);
  let date = null;
  if (Number.isFinite(asNumber) && asNumber > 0) {
    date = asNumber > 1000000000000 ? new Date(asNumber) : new Date(asNumber * 1000);
  } else {
    const parsed = new Date(String(value));
    if (!Number.isNaN(parsed.getTime())) {
      date = parsed;
    }
  }

  if (!date || Number.isNaN(date.getTime())) {
    return text(value, "-");
  }

  return date.toLocaleString("pt-BR", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function sanitizeUserText(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const lowered = raw.toLowerCase();
  for (const term of INTERNAL_TERMS) {
    if (lowered.includes(term)) return "";
  }
  return raw;
}

function statusReasonByCode(code) {
  if (code === "green") return "Conectividade dentro do esperado.";
  if (code === "red") return "Sem comunicação recente.";
  if (code === "yellow") return "Oscilações de conectividade detectadas.";
  if (code === "critical") return "Conectividade crítica no período.";
  return "Coletando dados para definir o status.";
}

function qualityReasonByCode(code) {
  if (code === "green") return "Conectividade estável no período selecionado.";
  if (code === "yellow") return "Conectividade instável no período selecionado.";
  if (code === "critical") return "Conectividade crítica no período selecionado.";
  return "Coletando dados para avaliar a conectividade.";
}

function buildEventSummary(event) {
  const type = String((event || {}).type || "").toLowerCase();
  if (type === "pivot_discovered") return "Monitoramento iniciado para este pivô.";
  if (type === "cloudv2" || type === "ping" || type === "cloud2") return "Atualização de conectividade registrada.";
  if (type === "probe_sent") return "Solicitação de latência enviada.";
  if (type === "probe_response") return "Resposta de latência recebida no período esperado.";
  if (type === "probe_timeout") return "Sem resposta de latência no tempo esperado.";
  if (type === "probe_response_unmatched") return "Resposta de latência registrada fora da janela esperada.";
  if (type === "session_started") return "Nova sessão de monitoramento iniciada.";
  return sanitizeUserText((event || {}).summary) || "Evento registrado.";
}

function buildEventDetailsText(event) {
  const details = (event || {}).details || {};
  const lines = [];
  const latencySec = Number(details.latency_sec);
  const dropDurationSec = Number(details.drop_duration_sec);
  const timeoutStreak = Number(details.timeout_streak);
  const count = Number(details.count);

  if (Number.isFinite(latencySec) && latencySec >= 0) {
    lines.push(`Latência: ${fmtSecondsPrecise(latencySec)}`);
  }
  if (Number.isFinite(dropDurationSec) && dropDurationSec >= 0) {
    lines.push(`Duração da desconexão: ${fmtDuration(dropDurationSec)}`);
  }
  if (Number.isFinite(timeoutStreak) && timeoutStreak >= 0) {
    lines.push(`Falhas consecutivas: ${timeoutStreak}`);
  }
  if (Number.isFinite(count) && count >= 0) {
    lines.push(`Quantidade: ${count}`);
  }

  return lines.length ? lines.join("\n") : "Sem detalhes adicionais.";
}

function getDisplayQuality(item) {
  const baseQuality = resolveQualityBase(item);
  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.qualityOverridesByPivotId[pivotId] : null;
  const base = override || baseQuality;
  const displayStatus = getDisplayStatus(item);
  const statusCode = text(displayStatus.code, "gray");
  const statusReason = sanitizeUserText(displayStatus.reason);
  if (statusCode === "gray") {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: statusReason || qualityReasonByCode("calculating"),
      rank: QUALITY_META.calculating.rank,
    };
  }
  const code = text(base.code, "green");
  const meta = QUALITY_META[code] || QUALITY_META.green;
  const rankRaw = Number(base.rank);
  const baseReason = sanitizeUserText(base.reason);

  return {
    code,
    label: meta.label,
    reason: baseReason || qualityReasonByCode(code),
    rank: Number.isFinite(rankRaw) ? rankRaw : meta.rank,
  };
}

function resolveQualityBase(item) {
  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.qualityOverridesByPivotId[pivotId] : null;
  return override || ((item || {}).quality || {});
}

function getDisplayStatus(item) {
  const qualityBase = resolveQualityBase(item);
  const qualityCode = text((qualityBase || {}).code, "").trim().toLowerCase();
  if (qualityCode === "calculating") {
    return {
      code: "gray",
      label: STATUS_META.gray.label,
      reason: "Conectividade em análise.",
      rank: Number(STATUS_META.gray.rank ?? 99),
    };
  }

  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.statusOverridesByPivotId[pivotId] : null;
  const base = override || ((item || {}).status || {});
  const code = text(base.code, "gray");
  const meta = STATUS_META[code] || STATUS_META.gray;
  const rankRaw = Number(base.rank);
  const baseReason = sanitizeUserText(base.reason);

  return {
    code,
    label: meta.label,
    reason: baseReason || statusReasonByCode(code),
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

function setInitialLoading(loading, message = "") {
  if (!HAS_DOM) return;
  if (ui.initialLoadingText) {
    const safeMessage = String(message || "").trim();
    if (safeMessage) {
      ui.initialLoadingText.textContent = safeMessage;
    }
  }
  if (ui.initialLoadingOverlay) {
    ui.initialLoadingOverlay.hidden = !loading;
  }
  if (document.body) {
    document.body.classList.toggle("initial-loading", !!loading);
  }
}

async function getJson(url) {
  const targetUrl = buildApiUrl(url);
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeoutId = controller
    ? window.setTimeout(() => controller.abort(), API_REQUEST_TIMEOUT_MS)
    : null;
  let response;
  try {
    response = await fetch(`${targetUrl}${targetUrl.includes("?") ? "&" : "?"}t=${Date.now()}`, {
      credentials: "include",
      signal: controller ? controller.signal : undefined,
    });
  } catch (err) {
    if (controller && err && err.name === "AbortError") {
      throw new Error(`timeout:${API_REQUEST_TIMEOUT_MS}`);
    }
    throw err;
  } finally {
    if (timeoutId !== null) window.clearTimeout(timeoutId);
  }
  let payload = {};
  try {
    payload = await response.json();
  } catch (err) {
    payload = {};
  }
  if (!response.ok) {
    const redirect = String(payload.redirect || "").trim();
    if (redirect) {
      window.location.assign(redirect);
      throw new Error(`AUTH_REDIRECT_${response.status}`);
    }
    throw new Error(String(payload.error || payload.message || response.status));
  }
  return payload;
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

function normalizeRunId(value) {
  return String(value || "").trim();
}

function pickBestRunIdFromRuns(runs) {
  const list = Array.isArray(runs) ? runs : [];
  if (!list.length) return null;

  const normalized = list
    .map((item) => {
      const runId = normalizeRunId(item?.run_id);
      if (!runId) return null;
      return {
        runId,
        isActive: !!item?.is_active,
        pivotCount: Number(item?.pivot_count || 0),
      };
    })
    .filter(Boolean);

  if (!normalized.length) return null;

  const activeWithData = normalized.find((item) => item.isActive && item.pivotCount > 0);
  if (activeWithData) return activeWithData.runId;

  const latestWithData = normalized.find((item) => item.pivotCount > 0);
  if (latestWithData) return latestWithData.runId;

  const activeAny = normalized.find((item) => item.isActive);
  if (activeAny) return activeAny.runId;

  return normalized[0].runId;
}

async function autoResolveRunIdFromBackend({ force = false, allowOverride = false } = {}) {
  const currentRunId = normalizeRunId(state.selectedRunId);
  if (currentRunId && !allowOverride) return false;

  const nowMs = Date.now();
  if (!force && nowMs - Number(state.lastRunResolveAttemptMs || 0) < 30000) {
    return false;
  }
  state.lastRunResolveAttemptMs = nowMs;

  let payload = null;
  try {
    payload = await getJson("/api/monitoring/runs?limit=200");
  } catch (err) {
    return false;
  }

  const resolvedRunId = pickBestRunIdFromRuns(payload?.runs);
  if (!resolvedRunId) return false;
  if (currentRunId && resolvedRunId === currentRunId) return false;

  state.selectedRunId = resolvedRunId;
  if (!state.runAutoDetected) {
    showToast("Sessão de monitoramento restaurada automaticamente.", "success", 3200);
    state.runAutoDetected = true;
  }
  return true;
}

function renderSessionControls() {
  return;
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
  const selectedStatus = normalizeFilterKey(state.statusFilter) || "all";
  const selectedConnectivity = normalizeFilterKey(state.connectivityFilter) || "all";
  const selectedTechnology = normalizeFilterKey(state.technologyFilter) || "all";
  const selectedFirmware = normalizeFilterKey(state.firmwareFilter) || "all";

  let filtered = list.filter((item) => {
    const quality = getDisplayQuality(item);
    const status = getDisplayStatus(item);
    if (selectedStatus !== "all" && status.code !== selectedStatus) return false;
    if (selectedConnectivity !== "all" && quality.code !== selectedConnectivity) return false;
    if (needle) {
      const pivotId = String(item.pivot_id || "").toLowerCase();
      if (!pivotId.includes(needle)) return false;
    }
    if (selectedTechnology !== "all") {
      const pivotTechnology = normalizeFilterKey(pivotTechnologyValue(item));
      if (pivotTechnology !== selectedTechnology) return false;
    }
    if (selectedFirmware !== "all") {
      const pivotFirmware = normalizeFilterKey(pivotFirmwareValue(item));
      if (pivotFirmware !== selectedFirmware) return false;
    }
    return true;
  });

  filtered.sort((a, b) => {
    const ap = String(a.pivot_id || "");
    const bp = String(b.pivot_id || "");
    if (state.sort === "pivot_asc") return ap.localeCompare(bp);

    const aActivity = Number(a.last_activity_ts || 0);
    const bActivity = Number(b.last_activity_ts || 0);
    if (state.sort === "samples_desc") return compareBySamplesDesc(a, b, ap, bp, aActivity, bActivity);

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

function pivotSampleCount(item) {
  const fromSummary = Number((item || {}).median_sample_count);
  if (Number.isFinite(fromSummary) && fromSummary >= 0) return Math.floor(fromSummary);
  const nested = Number((((item || {}).summary || {}).median_sample_count));
  if (Number.isFinite(nested) && nested >= 0) return Math.floor(nested);
  return 0;
}

function compareBySamplesDesc(a, b, ap = String(a?.pivot_id || ""), bp = String(b?.pivot_id || ""), aActivity = null, bActivity = null) {
  const aSamples = pivotSampleCount(a);
  const bSamples = pivotSampleCount(b);
  if (aSamples !== bSamples) return bSamples - aSamples;

  const aAct = Number.isFinite(Number(aActivity)) ? Number(aActivity) : Number(a?.last_activity_ts || 0);
  const bAct = Number.isFinite(Number(bActivity)) ? Number(bActivity) : Number(b?.last_activity_ts || 0);
  if (aAct !== bAct) return bAct - aAct;

  return ap.localeCompare(bp);
}

function resetAllFilters() {
  state.search = "";
  state.sort = "critical";
  state.statusFilter = "all";
  state.connectivityFilter = "all";
  state.technologyFilter = "all";
  state.firmwareFilter = "all";
  state.cardsPage = 1;

  if (ui.searchInput) ui.searchInput.value = "";
  if (ui.sortSelect) ui.sortSelect.value = "critical";
  if (ui.statusFilterSelect) ui.statusFilterSelect.value = "all";
  if (ui.connectivityFilterSelect) ui.connectivityFilterSelect.value = "all";
  if (ui.technologyFilter) ui.technologyFilter.value = "all";
  if (ui.firmwareFilter) ui.firmwareFilter.value = "all";

  renderCards();
}

function renderHeader() {
  const raw = state.rawState || {};
  const updatedAt = text(raw.updated_at, "-");
  const counts = raw.counts || {};
  ui.updatedAt.textContent = `Última atualização: ${updatedAt}`;
  ui.countsMeta.textContent = `${counts.pivots || 0} pivôs • ${counts.duplicate_drops || 0} duplicidades`;
  if (ui.sessionHint) {
    const runMeta = raw.run || state.panelRunMeta || null;
    const startedAt = text((runMeta || {}).started_at, "").trim();
    const usingHistoricalRun = !!text(state.selectedRunId, "").trim();
    if (usingHistoricalRun) {
      ui.sessionHint.textContent = startedAt
        ? `Exibindo sessão restaurada automaticamente (${startedAt}).`
        : "Exibindo sessão restaurada automaticamente.";
    } else {
      ui.sessionHint.textContent =
        "Este painel exibe automaticamente os dados mais recentes.";
    }
  }
}

function renderStatusSummary() {
  const stateCounts = { all: 0, green: 0, red: 0, gray: 0 };
  const qualityCounts = { green: 0, yellow: 0, critical: 0, calculating: 0 };
  const settings = (state.rawState || {}).settings || {};
  const minSamplesRaw = Number(settings.cloudv2_min_samples ?? 5);
  const minSamples = Number.isFinite(minSamplesRaw) && minSamplesRaw >= 1 ? Math.round(minSamplesRaw) : 5;
  const attentionThresholdRaw = Number(settings.attention_disconnected_pct_threshold ?? 20);
  const attentionThreshold = Number.isFinite(attentionThresholdRaw)
    ? Math.max(0, Math.min(100, attentionThresholdRaw))
    : 20;
  const criticalThresholdRaw = Number(settings.critical_disconnected_pct_threshold ?? 50);
  const criticalThreshold = Number.isFinite(criticalThresholdRaw)
    ? Math.max(attentionThreshold, Math.min(100, criticalThresholdRaw))
    : 50;

  for (const pivot of state.pivots) {
    stateCounts.all += 1;
    const stateCode = getDisplayStatus(pivot).code;
    if (stateCounts[stateCode] !== undefined) stateCounts[stateCode] += 1;

    const qualityCode = getDisplayQuality(pivot).code;
    if (qualityCounts[qualityCode] !== undefined) qualityCounts[qualityCode] += 1;
  }

  const summaryItems = [
    {
      label: "Total",
      value: stateCounts.all,
      tooltip: "Quantidade total de pivos monitorados no painel.",
    },
    {
      label: "Conectados",
      value: stateCounts.green,
      tooltip:
        "Pivos com Status Conectado. O status fica Conectado quando ha comunicacao recente dentro do limite de desconexao do proprio pivo.",
    },
    {
      label: "Desconectados",
      value: stateCounts.red,
      tooltip:
        "Pivos com Status Desconectado. O status vira Desconectado quando o tempo sem comunicacao ultrapassa o limite de desconexao calculado para o pivo.",
    },
    {
      label: "Iniciais",
      value: stateCounts.gray,
      tooltip:
        `Pivos com Status Inicial. Isso ocorre enquanto o sistema ainda esta coletando dados para definir o comportamento normal (minimo de ${minSamples} amostras cloudv2).`,
    },
    {
      label: "Conectividade estavel",
      value: qualityCounts.green,
      tooltip:
        `Conectividade dentro do esperado no periodo selecionado: percentual desconectado ate ${attentionThreshold.toFixed(1)}% e sem sinais de instabilidade.`,
    },
    {
      label: "Em analise",
      value: qualityCounts.calculating,
      tooltip:
        `Conectividade ainda em analise. Ocorre quando o pivo nao atingiu amostras suficientes (minimo ${minSamples}) para calcular a mediana de intervalo.`,
    },
    {
      label: "Conectividade instavel",
      value: qualityCounts.yellow,
      tooltip:
        `Conectividade com alerta. Ocorre quando o percentual desconectado supera ${attentionThreshold.toFixed(1)}% sem passar do limite critico (${criticalThreshold.toFixed(1)}%), ou quando so ha sinais auxiliares no periodo.`,
    },
    {
      label: "Conectividade critica",
      value: qualityCounts.critical,
      tooltip:
        `Conectividade critica no periodo selecionado. Ocorre quando o percentual desconectado fica acima de ${criticalThreshold.toFixed(1)}%.`,
    },
  ];

  ui.statusSummary.innerHTML = summaryItems
    .map(
      (item) => `
        <div
          class="summary-pill"
          title="${escapeHtml(item.tooltip)}"
          aria-label="${escapeHtml(item.label + ": " + item.tooltip)}"
          tabindex="0"
        >
          <span>${escapeHtml(item.label)}</span>
          <strong>${item.value}</strong>
        </div>
      `
    )
    .join("");
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
    ui.pendingList.innerHTML = `<div class="empty">Nenhum pivô com atividade inicial no momento.</div>`;
    return;
  }

  ui.pendingList.innerHTML = pending
    .slice(0, 40)
    .map((item) => {
      const id = escapeHtml(text(item.pivot_id));
      const count = Number(item.count || 0);
      const last = escapeHtml(text(item.last_seen_at));
      return `<div class="list-item"><strong>${id}</strong> com ${count} registro(s) (última atualização em ${last})</div>`;
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
    ui.cardsGrid.innerHTML = `<div class="empty">Nenhum pivô encontrado para os filtros selecionados.</div>`;
  } else {
    ui.cardsGrid.innerHTML = pageItems
      .map((pivot) => {
        const status = getDisplayStatus(pivot);
        const quality = getDisplayQuality(pivot);
        const statusCode = text(status.code, "gray");
        const statusLabel = escapeHtml(text(status.label, "Inicial"));
        const qualityCode = text(quality.code, "green");
        const qualityLabel = escapeHtml(text(quality.label, "Estável"));
        const pivotId = escapeHtml(text(pivot.pivot_id, "pivô"));
        const lastPing = escapeHtml(text(pivot.last_ping_at));
        const lastCloudv2 = escapeHtml(text(pivot.last_cloudv2_at));
        const cloud2 = pivot.last_cloud2 || {};
        const medianReady = !!pivot.median_ready;
        const samples = Number(pivot.median_sample_count || 0);
        const medianText = medianReady
          ? `${fmtDuration(pivot.median_cloudv2_interval_sec)} (${samples} amostras)`
          : `${samples} amostras (em análise)`;

        const rssi = escapeHtml(text(cloud2.rssi));
        const technology = escapeHtml(text(cloud2.technology));
        const firmware = escapeHtml(text(cloud2.firmware));

        return `
          <article class="pivot-card">
            <div class="pivot-head">
              <div class="pivot-id">${pivotId}</div>
              <div class="badge-stack">
                <span class="badge ${statusCode}">Status: ${statusLabel}</span>
                <span class="badge ${qualityCode}">Conectividade: ${qualityLabel}</span>
              </div>
            </div>
            <div class="kv-grid">
              <div class="k">Última atualização de conectividade</div><div>${lastPing}</div>
              <div class="k">Última atualização de dados</div><div>${lastCloudv2}</div>
              <div class="k">Intervalo típico de atualização</div><div>${escapeHtml(medianText)}</div>
              <div class="k">Última atualização</div><div>${escapeHtml(text(pivot.last_activity_at))}</div>
              <div class="k">Sinal / Tecnologia</div><div>${rssi} / ${technology}</div>
              <div class="k">Firmware</div><div>${firmware}</div>
            </div>
            <div class="card-actions">
              <button class="ghost open-pivot" data-pivot="${pivotId}">Abrir visão</button>
            </div>
          </article>
        `;
      })
      .join("");
  }

  ui.cardsPageInfo.textContent = `Página ${state.cardsPage}/${totalPages}`;
  ui.cardsPrev.disabled = state.cardsPage <= 1;
  ui.cardsNext.disabled = state.cardsPage >= totalPages;

  for (const button of ui.cardsGrid.querySelectorAll(".open-pivot")) {
    button.addEventListener("click", () => openPivot(button.dataset.pivot || ""));
  }
}

function renderPivotMetrics(pivot, statusView = null, qualityView = null, connectivityView = null) {
  const summary = pivot.summary || {};
  const metrics = pivot.metrics || {};
  const status = statusView || summary.status || {};
  const statusCode = text(status.code, "gray");
  const statusLabel = text(status.label, (STATUS_META[statusCode] || STATUS_META.gray).label);
  const safeStatusReason = sanitizeUserText(status.reason) || statusReasonByCode(statusCode);
  const quality = qualityView || summary.quality || {};
  const qualityCode = text(quality.code, "green");
  const qualityLabel = (QUALITY_META[qualityCode] || QUALITY_META.green).label;
  const safeQualityReason = sanitizeUserText(quality.reason) || qualityReasonByCode(qualityCode);
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
    { key: "status", label: "Status atual", value: statusLabel },
    { key: "status_reason", label: "Detalhe do status", value: safeStatusReason },
    { key: "connectivity", label: "Conectividade", value: qualityLabel },
    { key: "connectivity_reason", label: "Detalhe da conectividade", value: safeQualityReason },
    { key: "connected_pct", label: "% Conectado (janela)", value: connectedPctText },
    { key: "disconnected_pct", label: "% Desconectado (janela)", value: disconnectedPctText },
    { key: "last_ping", label: "Última atualização de conectividade", value: text(summary.last_ping_at) },
    { key: "last_cloudv2", label: "Última atualização de dados", value: text(summary.last_cloudv2_at) },
    {
      key: "median_cloudv2",
      label: "Intervalo típico de atualização",
      value: summary.median_ready
        ? `${fmtDuration(summary.median_cloudv2_interval_sec)} (${summary.median_sample_count} amostras)`
        : `${summary.median_sample_count} amostras`,
    },
    { key: "drops_24h", label: "Desconexões (24h)", value: text(metrics.drops_24h, "0") },
    { key: "drops_7d", label: "Desconexões (7 dias)", value: text(metrics.drops_7d, "0") },
    { key: "last_drop_duration", label: "Duração da última desconexão", value: fmtDuration(metrics.last_drop_duration_sec) },
    { key: "last_rssi", label: "Último sinal (RSSI)", value: text(metrics.last_rssi) },
    { key: "last_technology", label: "Última tecnologia de rede", value: text(metrics.last_technology) },
    { key: "firmware", label: "Firmware", value: text(metrics.last_firmware) },
    { key: "timeout_streak", label: "Falhas consecutivas de resposta", value: text(probe.timeout_streak, "0") },
    { key: "response_ratio", label: "Respostas/solicitações", value: responseCoverageText },
    { key: "timeout_total", label: "Total de falhas de resposta", value: text(timeoutCount, "0") },
    { key: "latency_last", label: "Latência da última resposta", value: fmtSecondsPrecise(probe.latency_last_sec) },
    { key: "latency_avg", label: "Latência média", value: fmtSecondsPrecise(probe.latency_avg_sec) },
    { key: "latency_median", label: "Latência mediana", value: fmtSecondsPrecise(probe.latency_median_sec) },
    {
      key: "latency_min_max",
      label: "Latência mínima/máxima",
      value: `${fmtSecondsPrecise(probe.latency_min_sec)} / ${fmtSecondsPrecise(probe.latency_max_sec)}`,
    },
    { key: "latency_samples", label: "Amostras de latência", value: text(latencySampleCount, "0") },
  ];

  const collapsedCardKeys = new Set([
    "status",
    "connectivity",
    "connected_pct",
    "disconnected_pct",
    "last_rssi",
    "last_technology",
  ]);
  const hasExtraCards = cards.length > collapsedCardKeys.size;
  const visibleCards = state.pivotMetricsExpanded
    ? cards
    : cards.filter((item) => collapsedCardKeys.has(String(item.key || "")));

  if (ui.pivotMoreInfoBtn) {
    ui.pivotMoreInfoBtn.hidden = !hasExtraCards;
    ui.pivotMoreInfoBtn.textContent = state.pivotMetricsExpanded ? "Menos informações" : "Mais informações";
    ui.pivotMoreInfoBtn.setAttribute("aria-expanded", state.pivotMetricsExpanded ? "true" : "false");
  }

  ui.pivotMetrics.innerHTML = visibleCards
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

function maxEventTs(events) {
  if (!Array.isArray(events)) return null;
  let maxTs = null;
  for (const event of events) {
    const ts = Number((event || {}).ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    if (maxTs === null || ts > maxTs) maxTs = ts;
  }
  return maxTs;
}

function resolveTimelineReferenceNowTs(pivot) {
  const wallClockNowTs = Math.floor(Date.now() / 1000);
  const safePivot = pivot || {};
  const payloadUpdatedTs = Number(safePivot.updated_at_ts || 0);
  const timelineLastTs = maxEventTs(safePivot.timeline);
  const probeLastTs = maxEventTs(safePivot.probe_events);
  const cloud2LastTs = maxEventTs(safePivot.cloud2_events);

  let persistedLatestTs = 0;
  for (const candidate of [payloadUpdatedTs, timelineLastTs, probeLastTs, cloud2LastTs]) {
    const ts = Number(candidate || 0);
    if (Number.isFinite(ts) && ts > persistedLatestTs) persistedLatestTs = ts;
  }

  const pivotRun = safePivot.run && typeof safePivot.run === "object" ? safePivot.run : null;
  const stateRun = (state.rawState || {}).run;
  const runMeta = pivotRun || state.panelRunMeta || stateRun || null;
  const isRunActive =
    runMeta && Object.prototype.hasOwnProperty.call(runMeta, "is_active")
      ? !!runMeta.is_active
      : String((state.rawState || {}).mode || "").toLowerCase() === "live";

  if (isRunActive) {
    return Math.max(wallClockNowTs, persistedLatestTs || 0);
  }
  if (persistedLatestTs > 0) {
    return persistedLatestTs;
  }
  return wallClockNowTs;
}

function getConnectivityEventsPanelCapped(pivot) {
  const events = Array.isArray((pivot || {}).timeline) ? pivot.timeline : [];
  const maxEvents = state.timelinePageSize * CONNECTIVITY_EVENTS_MAX_PAGES;
  if (events.length <= maxEvents) return events;
  return events.slice(events.length - maxEvents);
}

function normalizeRange(pivot) {
  const nowTs = resolveTimelineReferenceNowTs(pivot);
  const timeline = Array.isArray((pivot || {}).timeline) ? pivot.timeline : [];
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
  const nowTs = resolveTimelineReferenceNowTs(pivot);
  const probeEvents = Array.isArray((pivot || {}).probe_events) ? pivot.probe_events : [];
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

function resolveDisconnectThresholdSec(summary, settings) {
  const safeSummary = summary || {};
  const safeSettings = settings || {};
  const tolerance = Number(safeSettings.tolerance_factor || 1.5) || 1.5;
  const monitoredTopics = ["cloudv2", "cloudv2-ping", "cloudv2-info", "cloudv2-network"];
  const expectedByTopic = safeSummary.expected_by_topic_sec || {};

  const candidates = [];
  for (const topic of monitoredTopics) {
    const value = Number(expectedByTopic[topic]);
    if (Number.isFinite(value) && value > 0) candidates.push(value);
  }

  const maxExpectedFromSummary = Number(safeSummary.max_expected_interval_sec || 0);
  const maxExpectedIntervalSec =
    Number.isFinite(maxExpectedFromSummary) && maxExpectedFromSummary > 0
      ? maxExpectedFromSummary
      : candidates.length
      ? Math.max(...candidates)
      : Number(safeSettings.ping_expected_sec || 180);

  const thresholdFromSummary = Number(safeSummary.disconnect_threshold_sec || 0);
  const disconnectThresholdSec =
    Number.isFinite(thresholdFromSummary) && thresholdFromSummary > 0
      ? thresholdFromSummary
      : Math.max(30, maxExpectedIntervalSec * tolerance);

  return {
    disconnectThresholdSec,
    maxExpectedIntervalSec,
  };
}

function buildConnectivitySegments(pivot, startTs, endTs, settings = null) {
  const summary = (pivot || {}).summary || {};
  const monitoredTopics = ["cloudv2", "cloudv2-ping", "cloudv2-info", "cloudv2-network"];
  const { disconnectThresholdSec, maxExpectedIntervalSec } = resolveDisconnectThresholdSec(summary, settings);

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

  const segments = [];
  const safeStartTs = Number(startTs || 0);
  const safeEndTs = Number(endTs || 0);
  const durationRange = Math.max(1, safeEndTs - safeStartTs);
  const safeThreshold = Number(disconnectThresholdSec);

  const messageTs = events.map((event) => event.ts);
  const lastMessageTs = messageTs.length ? messageTs[messageTs.length - 1] : null;

  if (!Number.isFinite(safeThreshold) || safeThreshold <= 0 || !messageTs.length) {
    segments.push({
      state: "disconnected",
      start: safeStartTs,
      end: safeEndTs,
      duration: Math.max(1, safeEndTs - safeStartTs),
    });
    return {
      segments,
      disconnectThresholdSec: safeThreshold,
      maxExpectedIntervalSec,
      lastMessageTs,
      hasPrincipalPayloadInWindow,
      hasAuxPayloadInWindow,
      durationRange,
    };
  }

  const intervals = messageTs
    .map((ts) => ({ start: ts, end: ts + safeThreshold }))
    .sort((a, b) => a.start - b.start);

  const merged = [];
  for (const interval of intervals) {
    if (!merged.length) {
      merged.push({ start: interval.start, end: interval.end });
      continue;
    }
    const last = merged[merged.length - 1];
    if (interval.start <= last.end) {
      if (interval.end > last.end) last.end = interval.end;
      continue;
    }
    merged.push({ start: interval.start, end: interval.end });
  }

  let cursor = safeStartTs;
  for (const interval of merged) {
    if (interval.end <= safeStartTs) continue;
    if (interval.start >= safeEndTs) break;

    const segStart = Math.max(cursor, Math.max(safeStartTs, interval.start));
    const segEnd = Math.min(safeEndTs, interval.end);
    if (segEnd <= segStart) continue;

    if (segStart > cursor) {
      segments.push({
        state: "disconnected",
        start: cursor,
        end: segStart,
        duration: segStart - cursor,
      });
    }

    segments.push({
      state: "connected",
      start: segStart,
      end: segEnd,
      duration: segEnd - segStart,
    });
    cursor = segEnd;
    if (cursor >= safeEndTs) break;
  }

  if (cursor < safeEndTs) {
    segments.push({
      state: "disconnected",
      start: cursor,
      end: safeEndTs,
      duration: safeEndTs - cursor,
    });
  }

  const compacted = segments.filter((segment) => Number(segment.duration || 0) > 0);
  if (!compacted.length) {
    compacted.push({
      state: "disconnected",
      start: safeStartTs,
      end: safeEndTs,
      duration: Math.max(1, safeEndTs - safeStartTs),
    });
  }

  return {
    segments: compacted,
    disconnectThresholdSec: safeThreshold,
    maxExpectedIntervalSec,
    lastMessageTs,
    hasPrincipalPayloadInWindow,
    hasAuxPayloadInWindow,
    durationRange,
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

function buildConnectivityStatus(connData, referenceTs) {
  const lastSeenAtTs = connData ? connData.lastMessageTs : null;
  const thresholdSec = Number((connData || {}).disconnectThresholdSec);
  const ref = Number(referenceTs || 0);

  const connected = lastSeenAtTs !== null && Number.isFinite(ref) && ref - lastSeenAtTs <= thresholdSec;
  const code = connected ? "green" : "red";
  const meta = STATUS_META[code] || STATUS_META.gray;

  return {
    code,
    label: meta.label,
    reason: statusReasonByCode(code),
    rank: Number(meta.rank ?? 99),
    state: connected ? "connected" : "disconnected",
    referenceTs: ref,
    lastSeenAtTs,
    disconnectThresholdSec: thresholdSec,
  };
}

function computeConnectivityFromRange(pivot, startTs, endTs, settings = null) {
  const safeStart = Number(startTs || 0);
  const safeEnd = Number(endTs || 0);
  const durationSec = Math.max(1, safeEnd - safeStart);
  const connData = buildConnectivitySegments(pivot, safeStart, safeEnd, settings);
  const connectivityQualityInput = buildConnectivityQualityInput(connData, durationSec);
  const status = buildConnectivityStatus(connData, safeEnd);

  return {
    startTs: safeStart,
    endTs: safeEnd,
    durationSec,
    connData,
    segments: connData.segments,
    connectivityQualityInput,
    status,
  };
}

function computeConnectivityView(pivot) {
  const range = normalizeRange(pivot);
  const settings = ((state.rawState || {}).settings) || {};
  const view = computeConnectivityFromRange(pivot, range.startTs, range.endTs, settings);

  return {
    range,
    startTs: view.startTs,
    endTs: view.endTs,
    durationSec: view.durationSec,
    connData: view.connData,
    segments: view.segments,
    connectivityQualityInput: view.connectivityQualityInput,
    status: view.status,
  };
}

function buildQualityFromConnectivity(pivot, connectivitySummary) {
  const summary = pivot.summary || {};
  const fallbackQuality = summary.quality || {};
  const settings = (state.rawState || {}).settings || {};
  const minSamplesRaw = Number(settings.cloudv2_min_samples ?? 5);
  const minSamples = Number.isFinite(minSamplesRaw) && minSamplesRaw >= 1 ? Math.round(minSamplesRaw) : 5;
  const sampleCount = Math.max(0, Number(summary.median_sample_count || 0));
  const medianReady = !!summary.median_ready && sampleCount >= minSamples;
  const status = summary.status || {};
  const statusCode = text(status.code, "gray");
  const statusReason = text(status.reason, "");

  if (!medianReady) {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: `Aguardando amostras de cloudv2 para estimar mediana (${sampleCount}/${minSamples}).`,
      rank: QUALITY_META.calculating.rank,
    };
  }

  if (statusCode === "gray") {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: sanitizeUserText(statusReason) || qualityReasonByCode("calculating"),
      rank: QUALITY_META.calculating.rank,
    };
  }

  if (!connectivitySummary) {
    const fallbackCode = text(fallbackQuality.code, "green");
    return {
      code: fallbackCode,
      label: (QUALITY_META[fallbackCode] || QUALITY_META.green).label,
      reason: sanitizeUserText(fallbackQuality.reason) || qualityReasonByCode(fallbackCode),
      rank: Number(fallbackQuality.rank ?? (QUALITY_META[fallbackCode] || QUALITY_META.green).rank),
    };
  }

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
  let qualityReason = "Conectividade dentro da faixa esperada no período selecionado.";

  if (criticalByDisconnectedPct) {
    qualityCode = "critical";
    qualityReason =
      "Percentual desconectado no período " +
      `(${disconnectedPct}%) acima do limite crítico (${criticalThreshold.toFixed(1)}%).`;
  } else if (attentionByDisconnectedPct) {
    qualityCode = "yellow";
    qualityReason =
      "Percentual desconectado no período " +
      `(${disconnectedPct}%) acima do limite de instabilidade (${attentionThreshold.toFixed(1)}%).`;
  } else if (attentionByOnlyAuxTopics) {
    qualityCode = "yellow";
    qualityReason = "Sinais parciais de conectividade detectados no período selecionado.";
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
    <div class="conn-segment-card-row"><span>Início</span><strong>${escapeHtml(formatShortDateTime(startTs))}</strong></div>
    <div class="conn-segment-card-row"><span>Fim</span><strong>${escapeHtml(formatShortDateTime(endTs))}</strong></div>
    <div class="conn-segment-card-row"><span>Duração</span><strong>${escapeHtml(fmtDuration(durationSec))}</strong></div>
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
    <span class="conn-pill">Limite para desconexão: <strong>${fmtDuration(connData.disconnectThresholdSec)}</strong></span>
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
  return view;
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
    ui.probeDelayHint.textContent = "Monitoramento de latência desativado para este pivô.";
    ui.probeDelayChart.innerHTML = `<div class="empty">Ative o monitoramento de latência para acompanhar os dados no período.</div>`;
    return;
  }

  const series = buildProbeDelaySeries(pivot, startTs, endTs);
  const points = series.points;
  if (!points.length) {
    ui.probeDelayHint.textContent = "Nenhum dado de latência no período selecionado.";
    ui.probeDelayChart.innerHTML = `<div class="empty">Nenhum dado disponível para este período.</div>`;
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
    <svg class="probe-delay-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Gráfico de latência média ao longo do tempo">
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
    `Registros no período: ${series.responseCount} | ` +
    `Latência média final: ${fmtSecondsPrecise(lastPoint.avgLatencySec)} | ` +
    `Última medição: ${fmtSecondsPrecise(lastPoint.latencySec)} (${formatShortDateTime(lastPoint.ts)})`;
}

function renderTimeline(pivot) {
  const allEvents = getConnectivityEventsPanelCapped(pivot);
  const totalPages = Math.max(1, Math.ceil(allEvents.length / state.timelinePageSize));
  if (state.timelinePage > totalPages) state.timelinePage = totalPages;
  const start = (state.timelinePage - 1) * state.timelinePageSize;
  const pageEvents = allEvents.slice(start, start + state.timelinePageSize);

  if (!pageEvents.length) {
    ui.timelineList.innerHTML = `<div class="empty">Nenhum evento disponível para este período.</div>`;
  } else {
    ui.timelineList.innerHTML = pageEvents
      .map((event) => {
        const type = text(event.type, "event");
        const title = EVENT_LABEL[type] || "Evento";
        const summaryText = buildEventSummary(event);
        const detailsText = buildEventDetailsText(event);
        return `
          <article class="event ${escapeHtml(type)}">
            <div class="event-head">
              <div class="event-title">${escapeHtml(title)}</div>
              <div class="event-time">${escapeHtml(text(event.at))}</div>
            </div>
            <div class="event-topic">Categoria: Evento de conectividade</div>
            <div>${escapeHtml(summaryText)}</div>
            <details class="event-details">
              <summary>Informações adicionais</summary>
              <pre>${escapeHtml(detailsText)}</pre>
            </details>
          </article>
        `;
      })
      .join("");
  }

  ui.timelinePageInfo.textContent = `Página ${state.timelinePage}/${totalPages}`;
  ui.timelinePrev.disabled = state.timelinePage <= 1;
  ui.timelineNext.disabled = state.timelinePage >= totalPages;
}

function renderCloud2Table(pivot) {
  const rows = (pivot.cloud2_events || []).slice(0, 20);
  if (!rows.length) {
    ui.cloud2Table.innerHTML = `<tr><td colspan="5">Nenhum dado disponível para este período.</td></tr>`;
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
    syncPivotDeleteControl();
    clearConnectivitySegmentSelection();
    return;
  }

  ui.pivotView.hidden = false;
  syncPivotDeleteControl();
  const summary = pivot.summary || {};
  const probe = summary.probe || {};
  const connectivityView = renderConnectivityTimeline(pivot);
  const connectivitySummary = connectivityView.connectivityQualityInput;
  const quality = buildQualityFromConnectivity(pivot, connectivitySummary);
  const displayStatus = connectivityView.status;
  const pivotId = text(pivot.pivot_id, "").trim();
  if (pivotId) {
    state.qualityOverridesByPivotId[pivotId] = quality;
    state.statusOverridesByPivotId[pivotId] = {
      code: text(displayStatus.code, "gray"),
      label: text(displayStatus.label, "Inicial"),
      reason: text(displayStatus.reason, ""),
      rank: Number(displayStatus.rank ?? 99),
    };
  }

  ui.pivotTitle.textContent = text(pivot.pivot_id, "Pivô");
  ui.pivotStatus.textContent = `Status: ${text(displayStatus.label, "Inicial")}`;
  ui.pivotStatus.className = `badge ${text(displayStatus.code, "gray")}`;
  if (ui.pivotQuality) {
    ui.pivotQuality.textContent = `Conectividade: ${text(quality.label, "Estável")}`;
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
  renderPivotMetrics(pivot, displayStatus, quality, connectivitySummary);
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

async function refreshState(options = {}) {
  const skipRender = !!options.skipRender;
  const requestedRunId = normalizeRunId(state.selectedRunId) || null;
  const data = await getJson(buildStateUrl(requestedRunId));
  state.rawState = data;
  state.pivots = data.pivots || [];
  syncCloud2FilterOptions(data);
  state.panelRunMeta = data.run || null;
  let selectedRunChanged = false;
  const payloadRunId = normalizeRunId(data?.run_id);
  if (!requestedRunId && payloadRunId) {
    state.selectedRunId = payloadRunId;
    selectedRunChanged = true;
  }
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
    if (skipRender) {
      state.connSelectedSegmentKey = null;
      state.selectedPivot = null;
      state.pivotData = null;
      state.panelSessionMeta = null;
      state.panelRunMeta = data.run || null;
      setHashPivot("");
    } else {
      closePivot();
    }
  }
  if (!skipRender) {
    renderHeader();
    renderStatusSummary();
    renderPending();
    renderCards();
  }
  return { selectedRunChanged };
}

async function refreshPivot(options = {}) {
  const skipRender = !!options.skipRender;
  if (!state.selectedPivot) {
    state.pivotData = null;
    state.panelSessionMeta = null;
    state.panelRunMeta = state.rawState?.run || null;
    if (!skipRender) {
      renderSessionControls();
      renderPivotView();
    }
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
  if (!skipRender) {
    renderSessionControls();
    renderPivotView();
  }
}

async function refreshQualityOverrides(options = {}) {
  const skipRender = !!options.skipRender;
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
        const detailStatus = view.status || {};
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
  if (!skipRender) {
    renderStatusSummary();
    renderCards();
  }
}

async function refreshAll(options = {}) {
  const suppressInterimRender = !!options.suppressInterimRender;
  if (state.refreshInFlight) return;
  state.refreshInFlight = true;
  try {
    const stateResult = await refreshState({ skipRender: suppressInterimRender });
    if (stateResult?.selectedRunChanged) {
      await refreshState({ skipRender: suppressInterimRender });
    }
    if (!state.pivots.length) {
      const resolved = await autoResolveRunIdFromBackend({ allowOverride: true });
      if (resolved) {
        await refreshState({ skipRender: suppressInterimRender });
      }
    }
    await refreshQualityOverrides({ skipRender: suppressInterimRender });
    await refreshPivot({ skipRender: suppressInterimRender });
    if (suppressInterimRender) {
      renderHeader();
      renderStatusSummary();
      renderPending();
      renderCards();
      renderSessionControls();
      renderPivotView();
    }
    state.lastRefreshToastAtMs = 0;
  } catch (err) {
    ui.cardsGrid.innerHTML = `<div class="empty">Não foi possível carregar os dados. Tente novamente.</div>`;
    const now = Date.now();
    if (now - Number(state.lastRefreshToastAtMs || 0) > 15000) {
      showToast("Não foi possível carregar os dados. Tente novamente.", "error", 3800);
      state.lastRefreshToastAtMs = now;
    }
  } finally {
    state.refreshInFlight = false;
  }
}

async function openPivot(pivotId) {
  const id = String(pivotId || "").trim();
  if (!id) return;
  state.connSelectedSegmentKey = null;
  state.pivotMetricsExpanded = false;
  state.selectedPivot = id;
  state.panelSessionMeta = null;
  state.timelinePage = 1;
  setHashPivot(id);
  await refreshPivot();
  ui.pivotView.scrollIntoView({ behavior: "smooth", block: "start" });
}

function closePivot() {
  state.connSelectedSegmentKey = null;
  state.pivotMetricsExpanded = false;
  state.selectedPivot = null;
  state.pivotData = null;
  state.panelSessionMeta = null;
  state.panelRunMeta = state.rawState?.run || null;
  setHashPivot("");
  renderSessionControls();
  renderPivotView();
}

function canCurrentUserDeletePivots() {
  if (state.pivotDeleteAllowed === true) return true;
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  const email = String(state.authUserEmail || "").trim().toLowerCase();
  return role === "admin" && email === FIXED_PIVOT_DELETE_EMAIL;
}

function syncPivotDeleteControl() {
  if (!ui.deletePivotBtn) return;
  const allowed = canCurrentUserDeletePivots();
  ui.deletePivotBtn.hidden = !allowed;
  if (!allowed) {
    ui.deletePivotBtn.disabled = true;
  } else {
    ui.deletePivotBtn.disabled = false;
  }
}

async function deleteSelectedPivot() {
  if (!canCurrentUserDeletePivots()) {
    syncPivotDeleteControl();
    showToast("Apenas o administrador principal pode deletar pivos.", "error", 3800);
    return;
  }

  const pivotId = String(state.selectedPivot || "").trim();
  if (!pivotId) return;

  const confirmed = window.confirm(`Tem certeza que deseja deletar o pivô ${pivotId}?`);
  if (!confirmed) {
    return;
  }

  if (ui.deletePivotBtn) ui.deletePivotBtn.disabled = true;
  try {
    const response = await fetch(buildApiUrl(`/api/pivot/${encodeURIComponent(pivotId)}/delete`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ source: "ui" }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    showToast("Pivô deletado com sucesso.", "success", 3200);
    closePivot();
    await refreshAll();
  } catch (err) {
    showToast("Não foi possível deletar o pivô. Tente novamente.", "error", 4200);
  } finally {
    if (ui.deletePivotBtn) ui.deletePivotBtn.disabled = false;
  }
}

function syncAdminControls() {
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  const isAdmin = role === "admin";
  if (ui.adminCreateAccountLink) {
    ui.adminCreateAccountLink.hidden = !isAdmin;
  }
  if (ui.adminUsersPanel) {
    ui.adminUsersPanel.hidden = !isAdmin;
  }
  if (!isAdmin) {
    state.adminUsers = [];
    renderAdminUsers();
  }
  syncPivotDeleteControl();
}

function renderAdminUsers() {
  if (!ui.adminUsersTable || !ui.adminUsersEmpty) return;

  const role = String(state.authUserRole || "user").trim().toLowerCase();
  if (role !== "admin") {
    ui.adminUsersTable.innerHTML = "";
    ui.adminUsersEmpty.hidden = true;
    return;
  }

  const users = Array.isArray(state.adminUsers) ? state.adminUsers : [];
  if (!users.length) {
    ui.adminUsersTable.innerHTML = "";
    ui.adminUsersEmpty.hidden = false;
    return;
  }
  ui.adminUsersEmpty.hidden = true;

  ui.adminUsersTable.innerHTML = users
    .map((user) => {
      const userId = String(user.id || "").trim();
      const email = text(user.email, "-");
      const name = text(user.name, "-");
      const status = text(user.status, "-");
      const roleText = text(user.role, "user");
      const lastLogin = formatDateTimeValue(user.last_login_at);
      return `
        <tr>
          <td>${escapeHtml(email)}</td>
          <td>${escapeHtml(name)}</td>
          <td>${escapeHtml(roleText)}</td>
          <td>${escapeHtml(status)}</td>
          <td>${escapeHtml(lastLogin)}</td>
          <td>
            <button
              type="button"
              class="danger-inline"
              data-admin-delete-user-id="${escapeHtml(userId)}"
              data-admin-delete-user-email="${escapeHtml(email)}"
            >
              Deletar
            </button>
          </td>
        </tr>
      `;
    })
    .join("");
}

async function refreshAdminUsers() {
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  if (role !== "admin") return;

  if (ui.adminUsersRefresh) ui.adminUsersRefresh.disabled = true;
  try {
    const response = await fetch(buildApiUrl("/admin/users"), {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.message || data.error || `HTTP ${response.status}`);
    }
    state.adminUsers = Array.isArray(data.users) ? data.users : [];
    renderAdminUsers();
  } catch (err) {
    showToast("Nao foi possivel carregar as contas de usuario.", "error", 4200);
  } finally {
    if (ui.adminUsersRefresh) ui.adminUsersRefresh.disabled = false;
  }
}

async function deleteManagedUser(targetUserId, targetUserEmail) {
  const userId = String(targetUserId || "").trim();
  const email = String(targetUserEmail || "").trim();
  if (!userId) return;
  const confirmed = window.confirm(`Tem certeza que deseja deletar a conta ${email || userId}?`);
  if (!confirmed) return;

  try {
    const response = await fetch(buildApiUrl("/admin/users/delete"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ user_id: userId }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.message || data.error || `HTTP ${response.status}`);
    }
    showToast("Conta deletada com sucesso.", "success", 3200);
    await refreshAdminUsers();
  } catch (err) {
    const message = String((err && err.message) || "").trim();
    showToast(message || "Nao foi possivel deletar a conta.", "error", 4200);
  }
}

async function resolveViewerRole() {
  try {
    const response = await fetch(buildApiUrl("/auth/me"), {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const data = await response.json();
    if (!response.ok || !data.ok || !data.authenticated) {
      window.location.assign("/login");
      return false;
    }
    const user = ((data || {}).user || {});
    const role = String(user.role || "user").trim().toLowerCase();
    const email = String(user.email || "").trim().toLowerCase();
    const apiDeleteAllowed = Boolean((data || {}).pivot_delete_allowed);
    state.authUserRole = role || "user";
    state.authUserEmail = email;
    state.pivotDeleteAllowed = apiDeleteAllowed || (state.authUserRole === "admin" && state.authUserEmail === FIXED_PIVOT_DELETE_EMAIL);
    syncAdminControls();
    return true;
  } catch (err) {
    window.location.assign("/login");
    return false;
  }
}

async function logoutDashboard() {
  if (ui.logoutBtn) ui.logoutBtn.disabled = true;
  try {
    await fetch(buildApiUrl("/auth/logout"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({}),
    });
  } catch (err) {
    // no-op
  } finally {
    window.location.assign("/login");
  }
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
    const response = await fetch(buildApiUrl("/api/probe-config"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    ui.probeHint.textContent = "Configuração de monitoramento de latência salva com sucesso.";
    showToast("Configuração salva.", "success", 3000);
    await refreshAll();
  } catch (err) {
    ui.probeHint.textContent = "Não foi possível salvar a configuração. Tente novamente.";
    showToast("Não foi possível salvar a configuração. Tente novamente.", "error", 4200);
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

  if (ui.technologyFilter) {
    ui.technologyFilter.addEventListener("change", () => {
      state.technologyFilter = normalizeFilterKey(ui.technologyFilter.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.firmwareFilter) {
    ui.firmwareFilter.addEventListener("change", () => {
      state.firmwareFilter = normalizeFilterKey(ui.firmwareFilter.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.statusFilterSelect) {
    ui.statusFilterSelect.addEventListener("change", () => {
      state.statusFilter = normalizeFilterKey(ui.statusFilterSelect.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.connectivityFilterSelect) {
    ui.connectivityFilterSelect.addEventListener("change", () => {
      state.connectivityFilter = normalizeFilterKey(ui.connectivityFilterSelect.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.clearFilters) {
    ui.clearFilters.addEventListener("click", () => {
      resetAllFilters();
    });
  }

  if (ui.logoutBtn) {
    ui.logoutBtn.addEventListener("click", logoutDashboard);
  }

  if (ui.adminUsersRefresh) {
    ui.adminUsersRefresh.addEventListener("click", refreshAdminUsers);
  }

  if (ui.adminUsersTable) {
    ui.adminUsersTable.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const button = target.closest("button[data-admin-delete-user-id]");
      if (!button) return;
      const userId = String(button.getAttribute("data-admin-delete-user-id") || "").trim();
      const userEmail = String(button.getAttribute("data-admin-delete-user-email") || "").trim();
      deleteManagedUser(userId, userEmail);
    });
  }

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
  if (ui.deletePivotBtn) {
    ui.deletePivotBtn.addEventListener("click", deleteSelectedPivot);
  }
  if (ui.pivotMoreInfoBtn) {
    ui.pivotMoreInfoBtn.addEventListener("click", () => {
      state.pivotMetricsExpanded = !state.pivotMetricsExpanded;
      renderPivotView();
    });
  }
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
    const total = Math.max(
      1,
      Math.ceil(getConnectivityEventsPanelCapped(state.pivotData || {}).length / state.timelinePageSize)
    );
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
  const authorized = await resolveViewerRole();
  if (!authorized) return;
  setInitialLoading(true, "Buscando pivos e calculando conectividade inicial...");
  wireEvents();
  if (String(state.authUserRole || "").trim().toLowerCase() === "admin") {
    await refreshAdminUsers();
  }
  startDevAutoReload();
  await loadUiConfig();
  ui.connPreset.value = state.connPreset;
  ui.connFromWrap.hidden = true;
  ui.connToWrap.hidden = true;
  ui.probeDelayPreset.value = state.probeDelayPreset;
  ui.probeDelayRange.hidden = true;
  ui.probeDelayFromWrap.hidden = true;
  ui.probeDelayToWrap.hidden = true;
  const hashPivot = parseHashPivot();
  if (hashPivot) {
    state.selectedPivot = hashPivot;
  }
  try {
    await refreshAll({ suppressInterimRender: true });
  } finally {
    setInitialLoading(false);
  }
  setInterval(refreshAll, state.refreshMs);
}

if (HAS_DOM) {
  boot();
}

// Exports for Node-based regression tests (no DOM execution).
if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    _test: {
      pivotSampleCount,
      compareBySamplesDesc,
      getDisplayStatus,
      getDisplayQuality,
      buildQualityFromConnectivity,
      resolveTimelineReferenceNowTs,
      resolveDisconnectThresholdSec,
      buildConnectivitySegments,
      summarizeConnectivitySegments,
      buildConnectivityQualityInput,
      buildConnectivityStatus,
      computeConnectivityFromRange,
    },
  };
}
