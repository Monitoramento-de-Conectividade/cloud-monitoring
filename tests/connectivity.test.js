const test = require("node:test");
const assert = require("node:assert/strict");

const { _test } = require("../frontend/dashboard.js");

test("connectivity: timeline end state matches status at referenceTime (regression for 100% connected vs disconnected)", () => {
  const pivot = {
    summary: {
      // Force a known threshold so the test is independent from expected interval heuristics.
      disconnect_threshold_sec: 70,
    },
    timeline: [
      // Single message at the window start. Should disconnect before the window end.
      { topic: "cloudv2", ts: 1000 },
    ],
  };

  const view = _test.computeConnectivityFromRange(pivot, 1000, 1100, {});

  // At endTs=1100, lastSeenAt=1000, threshold=70 => 1100-1000=100 > 70 => disconnected.
  assert.equal(view.status.state, "disconnected");
  assert.equal(view.status.code, "red");
  assert.equal(view.segments.at(-1).state, "disconnected");
  assert.notEqual(view.connectivityQualityInput.connectedPct, 100);
});

test("connectivity: status is derived from the same timeline rule (ignores backend summary.status for filtered ranges)", () => {
  const pivot = {
    summary: {
      disconnect_threshold_sec: 70,
      // Simulates backend/current status being offline, while a past window can still be online.
      status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
    },
    timeline: [{ topic: "cloudv2", ts: 1000 }],
  };

  const view = _test.computeConnectivityFromRange(pivot, 1000, 1050, {});

  // endTs=1050, lastSeenAt=1000, threshold=70 => connected
  assert.equal(view.status.state, "connected");
  assert.equal(view.status.code, "green");
  assert.equal(view.segments.at(-1).state, "connected");
  assert.equal(view.connectivityQualityInput.connectedPct, 100);
});

test("connectivity: active run uses wall-clock time as the current reference", () => {
  const originalNow = Date.now;
  Date.now = () => 2_000_000;
  try {
    const referenceTs = _test.resolveTimelineReferenceNowTs({
      updated_at_ts: 1200,
      timeline: [{ topic: "cloudv2", ts: 1200 }],
      run: { is_active: true },
    });
    assert.equal(referenceTs, 2000);
  } finally {
    Date.now = originalNow;
  }
});

test("connectivity: historical run keeps persisted timestamp as the current reference", () => {
  const originalNow = Date.now;
  Date.now = () => 5_000_000;
  try {
    const referenceTs = _test.resolveTimelineReferenceNowTs({
      updated_at_ts: 1200,
      timeline: [{ topic: "cloudv2", ts: 1300 }],
      run: { is_active: false },
    });
    assert.equal(referenceTs, 1300);
  } finally {
    Date.now = originalNow;
  }
});

test("display: status is forced to inicial when connectivity is em analise", () => {
  const item = {
    pivot_id: "PivotA",
    status: { code: "green", reason: "Conectividade dentro do esperado." },
    quality: { code: "calculating", reason: "Coletando dados para avaliar a conectividade." },
  };

  const status = _test.getDisplayStatus(item);
  assert.equal(status.code, "gray");
  assert.equal(status.label, "Inicial");
});

test("display: status keeps timeline-derived value when connectivity is not em analise", () => {
  const item = {
    pivot_id: "PivotB",
    status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
    quality: { code: "yellow", reason: "Conectividade instÃ¡vel no perÃ­odo selecionado." },
  };

  const status = _test.getDisplayStatus(item);
  assert.equal(status.code, "red");
});

test("display: quality stays em analise when median samples are not ready, even with high disconnected pct", () => {
  const pivot = {
    summary: {
      status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
      median_ready: false,
      median_sample_count: 0,
    },
  };
  const connectivitySummary = {
    disconnectedPct: 100,
    hasPrincipalPayloadInWindow: false,
    hasAuxPayloadInWindow: false,
  };

  const quality = _test.buildQualityFromConnectivity(pivot, connectivitySummary);
  assert.equal(quality.code, "calculating");
});

test("display: quality can be critical after median is ready", () => {
  const pivot = {
    summary: {
      status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
      median_ready: true,
      median_sample_count: 5,
      attention_disconnected_pct_threshold: 20,
      critical_disconnected_pct_threshold: 50,
    },
  };
  const connectivitySummary = {
    disconnectedPct: 80,
    hasPrincipalPayloadInWindow: true,
    hasAuxPayloadInWindow: true,
  };

  const quality = _test.buildQualityFromConnectivity(pivot, connectivitySummary);
  assert.equal(quality.code, "critical");
});

test("sorting: pivots with more samples come first", () => {
  const a = { pivot_id: "A", median_sample_count: 2, last_activity_ts: 100 };
  const b = { pivot_id: "B", median_sample_count: 9, last_activity_ts: 90 };
  const compare = _test.compareBySamplesDesc(a, b);
  assert.ok(compare > 0);
});

test("sorting: sample count fallback reads nested summary", () => {
  const value = _test.pivotSampleCount({ summary: { median_sample_count: 7 } });
  assert.equal(value, 7);
});

test("sorting: latência ativa prioriza maior relação respostas/solicitações", () => {
  const a = {
    pivot_id: "A",
    probe: { enabled: true, response_ratio_pct: 52.0, sent_count: 25, response_count: 13 },
    last_activity_ts: 100,
  };
  const b = {
    pivot_id: "B",
    probe: { enabled: true, response_ratio_pct: 80.0, sent_count: 5, response_count: 4 },
    last_activity_ts: 90,
  };

  const compare = _test.compareByProbeResponseRatioDesc(a, b);
  assert.ok(compare > 0);
});

test("sorting: relação respostas/solicitações é calculada quando percentual não veio no payload", () => {
  const ratio = _test.pivotProbeResponseRatioPct({
    probe: { enabled: true, sent_count: 10, response_count: 7 },
  });
  assert.equal(ratio, 70);
});

test("display: concentrador override tem prioridade na tecnologia do pivô", () => {
  const technology = _test.pivotTechnologyValue({
    is_concentrator: true,
    last_cloud2: { technology: "LTE" },
  });
  assert.equal(technology, "concentrador");
});

test("display: tecnologia do cloud2 é usada quando pivô não é concentrador", () => {
  const technology = _test.pivotTechnologyValue({
    is_concentrator: false,
    last_cloud2: { technology: "LTE CAT" },
  });
  assert.equal(technology, "LTE CAT");
});

test("quality signature: changes when session or activity changes", () => {
  const base = {
    run_id: "run-1",
    session_id: "session-1",
    last_activity_ts: 100,
    last_ping_ts: 90,
    last_cloudv2_ts: 95,
    last_cloud2: { ts: 98 },
    median_sample_count: 5,
    median_ready: true,
    status: { code: "green" },
    quality: { code: "green" },
    probe: { sent_count: 10, response_count: 9 },
  };
  const sigA = _test.buildQualitySourceSignature(base);
  const sigB = _test.buildQualitySourceSignature({ ...base, session_id: "session-2" });
  const sigC = _test.buildQualitySourceSignature({ ...base, last_activity_ts: 101 });
  assert.notEqual(sigA, sigB);
  assert.notEqual(sigA, sigC);
});
