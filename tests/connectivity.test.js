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
