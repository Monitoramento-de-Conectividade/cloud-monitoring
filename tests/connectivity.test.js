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

