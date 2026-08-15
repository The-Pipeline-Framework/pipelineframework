import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  awaitOutputCountFromDownstreamStart,
  hasAwaitLifecycleCounterEvidence
} from "../../tools/replay-viewer/replay-counter-policy.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../..");

test("derives Await output from downstream starts without lifecycle counter evidence", () => {
  assert.equal(hasAwaitLifecycleCounterEvidence(null, null), false);
  assert.equal(awaitOutputCountFromDownstreamStart(false, 1_000), 1_000);
});

test("does not duplicate explicit Await lifecycle counters", () => {
  assert.equal(hasAwaitLifecycleCounterEvidence(1_000, null), true);
  assert.equal(hasAwaitLifecycleCounterEvidence(null, 1_000), true);
  assert.equal(awaitOutputCountFromDownstreamStart(true, 1_000), 0);
});

test("CSV built-in Await finishes with 1,000 derived outputs", () => {
  const replay = JSON.parse(readFileSync(
    path.join(repoRoot, "tools", "replay-viewer", "datasets", "csv-payments-built-in.json"),
    "utf8"
  ));
  const awaitEvents = replay.events.filter((event) => event.step === "AwaitPaymentProvider");
  const lifecycleCounterEvidence = awaitEvents.some((event) => hasAwaitLifecycleCounterEvidence(
    event.attributes?.["tpf.await.expected_item_count"],
    event.attributes?.["tpf.await.completed_item_count"]
  ));
  const downstreamOutputCount = replay.events
    .filter((event) => event.event === "start" && event.from === "AwaitPaymentProvider")
    .reduce((count, event) => count + (event.itemId ? 1 : 0), 0);

  assert.equal(lifecycleCounterEvidence, false);
  assert.equal(
    awaitOutputCountFromDownstreamStart(lifecycleCounterEvidence, downstreamOutputCount),
    1_000
  );
});
