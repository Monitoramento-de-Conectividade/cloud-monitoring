const HAS_DOM = typeof document !== "undefined";
const HAS_WINDOW = typeof window !== "undefined";

function resolveApiBaseUrl() {
  if (!HAS_WINDOW) return "";
  const fromGlobal = String(window.CLOUDV2_API_BASE_URL || "").trim();
  if (fromGlobal) return fromGlobal.replace(/\/+$/, "");
  if (!HAS_DOM) return "";
  const meta = document.querySelector('meta[name="cloudv2-api-base-url"]');
  if (!meta) return "";
  const fromMeta = String(meta.getAttribute("content") || "").trim();
  return fromMeta.replace(/\/+$/, "");
}

const API_BASE_URL = resolveApiBaseUrl();

function resolveApiOrigin() {
  if (!HAS_WINDOW || !API_BASE_URL) return "";
  try {
    const parsed = new URL(API_BASE_URL, window.location.href);
    return String(parsed.origin || "").trim();
  } catch (err) {
    return "";
  }
}

function buildAppUrl(url) {
  const normalized = String(url || "").trim();
  if (!normalized) return normalized;
  if (!normalized.startsWith("/") || /^https?:\/\//i.test(normalized)) return normalized;
  const apiOrigin = resolveApiOrigin();
  if (!apiOrigin) return normalized;
  return `${apiOrigin}${normalized}`;
}

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

const REQUEST_TIMEOUT_MS = 12000;
const MAP_DEFAULT_CENTER = [-14.235, -51.9253];
const MAP_DEFAULT_ZOOM = 4;
const MAP_MIN_ZOOM = 3;
const MAP_MAX_ZOOM = 18;
const MAP_TILE_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
const MAP_TILE_ATTRIBUTION = "&copy; OpenStreetMap contributors";
const QUALITY_COLOR_VAR_BY_CODE = {
  green: "--green",
  calculating: "--calculating",
  yellow: "--yellow",
  critical: "--critical",
};
const QUALITY_LABEL_BY_CODE = {
  green: "Estavel",
  calculating: "Em analise",
  yellow: "Instavel",
  critical: "Critico",
};
const STATUS_LABEL_BY_CODE = {
  green: "Conectado",
  red: "Desconectado",
  gray: "Inicial",
};
const MAP_POPUP_COLUMNS = [
  { key: "pivot_id", label: "Pivo" },
  { key: "status", label: "Status" },
  { key: "connectivity", label: "Conectividade" },
  { key: "timeline", label: "Timeline" },
  { key: "last_cloudv2_at", label: "Ultima atualizacao de dados" },
  { key: "median", label: "Intervalo tipico de atualizacao" },
  { key: "last_activity_at", label: "Ultima atualizacao" },
  { key: "signal", label: "Sinal" },
  { key: "technology", label: "Tecnologia" },
  { key: "firmware", label: "Firmware" },
];

const ui = HAS_DOM
  ? {
      mapUpdatedAt: document.getElementById("mapUpdatedAt"),
      mapCountsMeta: document.getElementById("mapCountsMeta"),
      mapRefreshBtn: document.getElementById("mapRefreshBtn"),
      mapStatus: document.getElementById("mapStatus"),
      mapFullscreenBtn: document.getElementById("mapFullscreenBtn"),
      mapFullscreenTarget: document.getElementById("mapFullscreenTarget"),
      pivotsMapWrap: document.getElementById("pivotsMapWrap"),
      pivotsMapCanvas: document.getElementById("pivotsMapCanvas"),
    }
  : {};

const state = {
  payload: null,
  pivots: [],
  refreshInFlight: false,
  selectedRunId: null,
  mapFullscreenActive: false,
};

let mapInstance = null;
let markerLayer = null;

function text(value, fallback = "-") {
  const normalized = String(value ?? "").trim();
  return normalized || String(fallback);
}

function normalizeRunId(value) {
  return String(value || "").trim();
}

function buildStateUrl(runId = null) {
  const normalizedRun = normalizeRunId(runId);
  if (!normalizedRun) return "/api/state";
  return `/api/state?run_id=${encodeURIComponent(normalizedRun)}`;
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function toBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (value === null || value === undefined) return fallback;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "y", "sim"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "nao", "não"].includes(normalized)) return false;
  }
  return fallback;
}

function fmtDuration(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
  return `${(s / 3600).toFixed(1)}h`;
}

function resolveCssVarColor(variableName, fallbackColor) {
  if (!HAS_WINDOW || !variableName) return fallbackColor;
  const root = window.getComputedStyle(document.documentElement);
  const value = String(root.getPropertyValue(variableName) || "").trim();
  return value || fallbackColor;
}

function mapPinSizeForZoom(zoom) {
  const parsedZoom = Number(zoom);
  if (!Number.isFinite(parsedZoom)) return 38;
  const size = 38 - ((parsedZoom - MAP_DEFAULT_ZOOM) * 2.4);
  return Math.max(14, Math.min(42, Math.round(size)));
}

function resolveMapQuality(pivot) {
  const safePivot = pivot && typeof pivot === "object" ? pivot : {};
  const summary = safePivot.summary && typeof safePivot.summary === "object" ? safePivot.summary : {};
  const settings = state.payload && typeof state.payload.settings === "object" ? state.payload.settings : {};
  const status = summary.status && typeof summary.status === "object"
    ? summary.status
    : (safePivot.status && typeof safePivot.status === "object" ? safePivot.status : {});
  const fallbackQuality = summary.quality && typeof summary.quality === "object"
    ? summary.quality
    : (safePivot.quality && typeof safePivot.quality === "object" ? safePivot.quality : {});

  const minSamplesRaw = Number(settings.cloudv2_min_samples ?? 5);
  const minSamples = Number.isFinite(minSamplesRaw) && minSamplesRaw >= 1 ? Math.round(minSamplesRaw) : 5;
  const sampleCount = Math.max(0, Number(summary.median_sample_count ?? safePivot.median_sample_count ?? 0));
  const medianReady = !!(summary.median_ready ?? safePivot.median_ready) && sampleCount >= minSamples;
  const statusCode = text(status.code, "gray").trim().toLowerCase();

  if (!medianReady || statusCode === "gray") {
    return { code: "calculating", label: QUALITY_LABEL_BY_CODE.calculating };
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

  const segments = normalizeTimelineMiniSegments(
    Array.isArray(safePivot.timeline_mini) ? safePivot.timeline_mini : summary.timeline_mini
  );
  if (segments.length) {
    const disconnectedRatio = segments
      .filter((segment) => segment.state === "offline")
      .reduce((total, segment) => total + Number(segment.ratio || 0), 0);
    const disconnectedPct = Math.max(0, Math.min(100, Math.round(disconnectedRatio * 100)));
    let code = "green";
    if (disconnectedPct > criticalThreshold) code = "critical";
    else if (disconnectedPct > attentionThreshold) code = "yellow";
    return { code, label: QUALITY_LABEL_BY_CODE[code] || QUALITY_LABEL_BY_CODE.green };
  }

  const fallbackCodeRaw = text(fallbackQuality.code, "green").trim().toLowerCase();
  const fallbackCode = QUALITY_COLOR_VAR_BY_CODE[fallbackCodeRaw] ? fallbackCodeRaw : "green";
  return {
    code: fallbackCode,
    label: text(fallbackQuality.label, QUALITY_LABEL_BY_CODE[fallbackCode] || QUALITY_LABEL_BY_CODE.green),
  };
}

function qualityCodeForPivot(pivot) {
  const quality = resolveMapQuality(pivot);
  const code = text(quality.code, "green").trim().toLowerCase();
  return QUALITY_COLOR_VAR_BY_CODE[code] ? code : "green";
}

function qualityLabelForPivot(pivot) {
  const quality = resolveMapQuality(pivot);
  const code = qualityCodeForPivot(pivot);
  return text(quality.label, QUALITY_LABEL_BY_CODE[code] || QUALITY_LABEL_BY_CODE.green);
}

function statusCodeForPivot(pivot) {
  const status = pivot && typeof pivot.status === "object" ? pivot.status : {};
  const code = text(status.code, "gray").trim().toLowerCase();
  return STATUS_LABEL_BY_CODE[code] ? code : "gray";
}

function statusLabelForPivot(pivot) {
  const status = pivot && typeof pivot.status === "object" ? pivot.status : {};
  const code = statusCodeForPivot(pivot);
  return text(status.label, STATUS_LABEL_BY_CODE[code] || STATUS_LABEL_BY_CODE.gray);
}

function pinColorForPivot(pivot) {
  const qualityCode = qualityCodeForPivot(pivot);
  const variableName = QUALITY_COLOR_VAR_BY_CODE[qualityCode] || QUALITY_COLOR_VAR_BY_CODE.green;
  return resolveCssVarColor(variableName, "#2a7e4c");
}

function buildPinIcon(pivot, zoomLevel) {
  if (!HAS_WINDOW || !window.L) return null;
  const pinSize = mapPinSizeForZoom(zoomLevel);
  const iconHeight = pinSize + 12;
  const color = pinColorForPivot(pivot);
  const html = `
    <span class="map-pin-wrap" style="--map-pin-size:${pinSize}px;--map-pin-color:${color};">
      <span class="map-pin-shape"></span>
    </span>
  `;
  return window.L.divIcon({
    className: "map-pin-icon",
    html,
    iconSize: [pinSize, iconHeight],
    iconAnchor: [Math.round(pinSize / 2), iconHeight - 2],
    popupAnchor: [0, -Math.round(pinSize * 0.62)],
  });
}

function extractPivotCoordinates(pivot) {
  const safePivot = pivot || {};
  const summary = safePivot.summary && typeof safePivot.summary === "object" ? safePivot.summary : {};
  const latitudeCandidates = [summary.latitude, safePivot.latitude];
  const longitudeCandidates = [summary.longitude, safePivot.longitude];

  let latitude = null;
  let longitude = null;
  for (const candidate of latitudeCandidates) {
    const parsed = Number(candidate);
    if (Number.isFinite(parsed)) {
      latitude = parsed;
      break;
    }
  }
  for (const candidate of longitudeCandidates) {
    const parsed = Number(candidate);
    if (Number.isFinite(parsed)) {
      longitude = parsed;
      break;
    }
  }
  return { latitude, longitude };
}

function hasValidPivotCoordinates(pivot) {
  const { latitude, longitude } = extractPivotCoordinates(pivot);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return false;
  if (latitude < -90 || latitude > 90) return false;
  if (longitude < -180 || longitude > 180) return false;
  if (Math.abs(latitude) < 1e-12 || Math.abs(longitude) < 1e-12) return false;
  return true;
}

function normalizeTimelineMiniSegments(segments) {
  if (!Array.isArray(segments)) return [];
  const cleaned = [];
  for (const segment of segments) {
    if (!segment || typeof segment !== "object") continue;
    const stateValue = text(segment.state, "").trim().toLowerCase();
    if (stateValue !== "online" && stateValue !== "offline") continue;
    const ratioValue = Number(segment.ratio);
    if (!Number.isFinite(ratioValue) || ratioValue <= 0) continue;
    cleaned.push({ state: stateValue, ratio: ratioValue });
  }
  if (!cleaned.length) return [];
  const total = cleaned.reduce((acc, segment) => acc + segment.ratio, 0);
  if (!Number.isFinite(total) || total <= 0) return [];
  return cleaned.map((segment) => ({
    state: segment.state,
    ratio: segment.ratio / total,
  }));
}

function buildTimelineMiniHtml(pivot) {
  const safePivot = pivot || {};
  const summary = safePivot.summary && typeof safePivot.summary === "object" ? safePivot.summary : {};
  const raw = Array.isArray(safePivot.timeline_mini) ? safePivot.timeline_mini : summary.timeline_mini;
  const segments = normalizeTimelineMiniSegments(raw);
  if (!segments.length) {
    return `
      <div class="pivot-timeline-mini empty" aria-hidden="true">
        <span class="pivot-timeline-mini-segment neutral" style="width: 100%;"></span>
      </div>
    `;
  }
  const inner = segments
    .map((segment) => {
      const widthPct = Math.max(0, Math.min(100, segment.ratio * 100));
      const cssState = segment.state === "online" ? "online" : "offline";
      return `<span class="pivot-timeline-mini-segment ${cssState}" style="width: ${widthPct.toFixed(4)}%;"></span>`;
    })
    .join("");
  return `<div class="pivot-timeline-mini" aria-hidden="true">${inner}</div>`;
}

function pivotCloud2Info(pivot) {
  const safePivot = pivot || {};
  return safePivot.last_cloud2 && typeof safePivot.last_cloud2 === "object" ? safePivot.last_cloud2 : {};
}

function pivotSignalValue(pivot) {
  const cloud2 = pivotCloud2Info(pivot);
  return text((pivot || {}).signal, text(cloud2.rssi));
}

function pivotTechnologyValue(pivot) {
  if (toBoolean((pivot || {}).is_concentrator, false)) return "concentrador";
  const cloud2 = pivotCloud2Info(pivot);
  return text((pivot || {}).technology, text(cloud2.technology));
}

function popupValueHtml(pivot, columnKey) {
  const safePivot = pivot || {};
  const cloud2 = pivotCloud2Info(safePivot);
  if (columnKey === "pivot_id") {
    return escapeHtml(text(safePivot.pivot_id));
  }
  if (columnKey === "status") {
    const statusCode = statusCodeForPivot(safePivot);
    const statusLabel = statusLabelForPivot(safePivot);
    return `<span class="badge ${escapeHtml(statusCode)}">${escapeHtml(statusLabel)}</span>`;
  }
  if (columnKey === "connectivity") {
    const qualityCode = qualityCodeForPivot(safePivot);
    const qualityLabel = qualityLabelForPivot(safePivot);
    return `<span class="badge ${escapeHtml(qualityCode)}">${escapeHtml(qualityLabel)}</span>`;
  }
  if (columnKey === "timeline") {
    return buildTimelineMiniHtml(safePivot);
  }
  if (columnKey === "last_cloudv2_at") {
    return escapeHtml(text(safePivot.last_cloudv2_at));
  }
  if (columnKey === "median") {
    const medianReady = !!safePivot.median_ready;
    const sampleCount = Number(safePivot.median_sample_count || 0);
    const medianText = medianReady
      ? `${fmtDuration(safePivot.median_cloudv2_interval_sec)} (${sampleCount} amostras)`
      : `${sampleCount} amostras (em analise)`;
    return escapeHtml(medianText);
  }
  if (columnKey === "last_activity_at") {
    return escapeHtml(text(safePivot.last_activity_at));
  }
  if (columnKey === "signal") {
    return escapeHtml(text(pivotSignalValue(safePivot)));
  }
  if (columnKey === "technology") {
    return escapeHtml(text(pivotTechnologyValue(safePivot)));
  }
  if (columnKey === "firmware") {
    return escapeHtml(text(cloud2.firmware));
  }
  return "-";
}

function buildPopupHtml(pivot) {
  const rows = MAP_POPUP_COLUMNS
    .map((column) => {
      const value = popupValueHtml(pivot, column.key);
      return `
        <div class="map-popup-row">
          <span class="map-popup-label">${escapeHtml(column.label)}</span>
          <span class="map-popup-value">${value}</span>
        </div>
      `;
    })
    .join("");
  return `<div class="map-popup">${rows}</div>`;
}

function setStatus(message) {
  if (!ui.mapStatus) return;
  ui.mapStatus.textContent = String(message || "").trim();
}

function applyMapFullscreenState(isActive) {
  if (!HAS_DOM || !document.body) return;
  state.mapFullscreenActive = !!isActive;
  document.body.classList.toggle("map-fullscreen-active", state.mapFullscreenActive);
  if (ui.mapFullscreenTarget) {
    ui.mapFullscreenTarget.classList.toggle("is-fullscreen", state.mapFullscreenActive);
  }
  if (ui.mapFullscreenBtn) {
    ui.mapFullscreenBtn.textContent = state.mapFullscreenActive ? "Sair da tela cheia" : "Tela cheia";
    ui.mapFullscreenBtn.setAttribute("aria-pressed", state.mapFullscreenActive ? "true" : "false");
    ui.mapFullscreenBtn.setAttribute(
      "aria-label",
      state.mapFullscreenActive ? "Sair da tela cheia do mapa" : "Expandir mapa para tela cheia"
    );
  }
  window.setTimeout(() => {
    if (!mapInstance) return;
    mapInstance.invalidateSize();
  }, 0);
}

function toggleMapFullscreen() {
  applyMapFullscreenState(!state.mapFullscreenActive);
}

function renderHeader(payload) {
  const safePayload = payload && typeof payload === "object" ? payload : {};
  const counts = safePayload.counts && typeof safePayload.counts === "object" ? safePayload.counts : {};
  if (ui.mapUpdatedAt) {
    ui.mapUpdatedAt.textContent = `Ultima atualizacao: ${text(safePayload.updated_at)}`;
  }
  if (ui.mapCountsMeta) {
    ui.mapCountsMeta.textContent = `${Number(counts.pivots || 0)} pivôs • ${Number(counts.duplicate_drops || 0)} duplicidades`;
  }
}

function ensureMapReady() {
  if (!HAS_WINDOW || !HAS_DOM || !ui.pivotsMapCanvas) return false;
  if (!window.L || typeof window.L.map !== "function") {
    setStatus("Mapa indisponivel no momento.");
    return false;
  }
  if (mapInstance) return true;

  mapInstance = window.L.map(ui.pivotsMapCanvas, {
    minZoom: MAP_MIN_ZOOM,
    maxZoom: MAP_MAX_ZOOM,
    zoomControl: true,
    worldCopyJump: true,
  });
  window.L.tileLayer(MAP_TILE_URL, { attribution: MAP_TILE_ATTRIBUTION }).addTo(mapInstance);
  markerLayer = window.L.layerGroup().addTo(mapInstance);
  mapInstance.setView(MAP_DEFAULT_CENTER, MAP_DEFAULT_ZOOM);
  mapInstance.on("zoomend", renderMapMarkers);
  return true;
}

function renderMapMarkers() {
  if (!mapInstance || !markerLayer || !window.L) return;
  markerLayer.clearLayers();

  const zoomLevel = Number(mapInstance.getZoom() || MAP_DEFAULT_ZOOM);
  let markerCount = 0;
  for (const pivot of state.pivots) {
    if (!hasValidPivotCoordinates(pivot)) continue;
    const { latitude, longitude } = extractPivotCoordinates(pivot);
    const icon = buildPinIcon(pivot, zoomLevel);
    if (!icon) continue;

    const marker = window.L.marker([latitude, longitude], {
      icon,
      keyboard: true,
      title: text((pivot || {}).pivot_id, "pivo"),
    });
    marker.bindPopup(buildPopupHtml(pivot), {
      maxWidth: 360,
      closeButton: false,
      autoPan: true,
    });
    marker.on("mouseover", () => marker.openPopup());
    marker.addTo(markerLayer);
    markerCount += 1;
  }

  if (!markerCount) {
    setStatus("Nenhum pivo com latitude/longitude valida para exibir no mapa.");
    return;
  }
  setStatus(`${markerCount} pivo(s) com coordenadas validas no mapa.`);
}

async function getJson(url) {
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeoutId = controller ? window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS) : null;
  try {
    const response = await fetch(buildApiUrl(url), {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" },
      signal: controller ? controller.signal : undefined,
    });
    let data = {};
    try {
      data = await response.json();
    } catch (err) {
      data = {};
    }

    if (!response.ok) {
      const redirectTo = text((data || {}).redirect, "");
      if ((response.status === 401 || response.status === 403) && redirectTo) {
        window.location.assign(buildAppUrl(redirectTo));
      }
      throw new Error(text((data || {}).error, `HTTP ${response.status}`));
    }
    return data;
  } finally {
    if (timeoutId !== null) {
      window.clearTimeout(timeoutId);
    }
  }
}

async function ensureAuthenticated() {
  try {
    const payload = await getJson("/auth/me");
    if (payload && payload.authenticated) return true;
  } catch (err) {
    // no-op
  }
  window.location.assign(buildAppUrl("/login"));
  return false;
}

async function resolveRunIdForMap() {
  const currentRun = normalizeRunId(state.selectedRunId);
  if (currentRun) return currentRun;
  try {
    const payload = await getJson("/api/monitoring/runs?limit=200");
    const resolved = pickBestRunIdFromRuns(payload?.runs);
    state.selectedRunId = normalizeRunId(resolved);
    return state.selectedRunId || null;
  } catch (err) {
    return null;
  }
}

async function refreshMapData() {
  if (state.refreshInFlight) return;
  state.refreshInFlight = true;
  if (ui.mapRefreshBtn) ui.mapRefreshBtn.disabled = true;
  try {
    const requestedRunId = await resolveRunIdForMap();
    const statePayload = await getJson(buildStateUrl(requestedRunId));
    state.payload = statePayload && typeof statePayload === "object" ? statePayload : {};
    const payloadRunId = normalizeRunId(state.payload.run_id);
    if (payloadRunId) state.selectedRunId = payloadRunId;
    state.pivots = Array.isArray(state.payload.pivots) ? state.payload.pivots : [];

    renderHeader(state.payload);
    renderMapMarkers();
  } catch (err) {
    setStatus("Nao foi possivel carregar os dados do mapa.");
  } finally {
    if (ui.mapRefreshBtn) ui.mapRefreshBtn.disabled = false;
    state.refreshInFlight = false;
  }
}

async function boot() {
  if (!HAS_DOM) return;
  if (!(await ensureAuthenticated())) return;
  if (!ensureMapReady()) return;
  applyMapFullscreenState(false);

  if (ui.mapRefreshBtn) {
    ui.mapRefreshBtn.addEventListener("click", () => {
      void refreshMapData();
    });
  }
  if (ui.mapFullscreenBtn) {
    ui.mapFullscreenBtn.addEventListener("click", () => {
      toggleMapFullscreen();
    });
  }

  window.addEventListener("resize", () => {
    if (!mapInstance) return;
    mapInstance.invalidateSize();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (!state.mapFullscreenActive) return;
    applyMapFullscreenState(false);
  });

  await refreshMapData();
  window.setInterval(() => {
    void refreshMapData();
  }, 30000);
}

if (HAS_DOM) {
  void boot();
}
