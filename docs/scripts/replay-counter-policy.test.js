import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  awaitOutputCountFromDownstreamStart,
  deferredStartReceiptCount,
  downstreamReceiptEvidenceKey,
  hasAwaitLifecycleCounterEvidence,
  isDeferredCompletionStep
} from "../../tools/replay-viewer/replay-counter-policy.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../..");

function renderHomepageCinematic(replay) {
  const tempDirectory = mkdtempSync(path.join(os.tmpdir(), "tpf-replay-cinematic-"));
  const input = path.join(tempDirectory, "replay.json");
  const output = path.join(tempDirectory, "cinematic.json");
  try {
    writeFileSync(input, JSON.stringify(replay));
    execFileSync(process.execPath, [
      path.join(repoRoot, "tools", "homepage-replay-video", "prepare_replay_data.cjs"),
      "--input",
      input,
      "--output",
      output
    ]);
    return JSON.parse(readFileSync(output, "utf8"));
  } finally {
    rmSync(tempDirectory, { recursive: true, force: true });
  }
}

test("derives Await output from downstream starts without lifecycle counter evidence", () => {
  assert.equal(hasAwaitLifecycleCounterEvidence(null, null), false);
  assert.equal(awaitOutputCountFromDownstreamStart(false, 1_000), 1_000);
  assert.equal(awaitOutputCountFromDownstreamStart(false, 17), 17);
});

test("does not duplicate explicit Await lifecycle counters", () => {
  assert.equal(hasAwaitLifecycleCounterEvidence(1_000, null), true);
  assert.equal(hasAwaitLifecycleCounterEvidence(null, 1_000), true);
  assert.equal(awaitOutputCountFromDownstreamStart(true, 1_000), 0);
});

test("recognises deferred completion as an overlay without an Await render role", () => {
  assert.equal(isDeferredCompletionStep({ deferredCompletion: true, renderRole: "primary" }), true);
  assert.equal(isDeferredCompletionStep({ deferredCompletion: false, renderRole: "primary" }), false);
  assert.equal(isDeferredCompletionStep({ renderRole: "await" }), true);
});

test("does not infer deferred completion from an Await-like step name", () => {
  const cinematic = renderHomepageCinematic({
    pipeline: "ImmediatePipeline",
    status: "COMPLETED",
    topology: {
      steps: [
        { step: "AwaitButImmediate", index: 0, deferredCompletion: false },
        { step: "FollowingStep", index: 1 }
      ],
      transitions: [
        { from: "AwaitButImmediate", to: "FollowingStep", relationKind: "primary" }
      ]
    },
    events: []
  });

  assert.equal(cinematic.nodes.find((node) => node.id === "AwaitButImmediate")?.deferredCompletion, false);
  assert.equal(cinematic.nodes.some((node) => node.role === "broker" || node.role === "provider"), false);
});

test("animates first-step deferred requests and releases output only after completion", () => {
  const cinematic = renderHomepageCinematic({
    pipeline: "DeferredPipeline",
    status: "COMPLETED",
    topology: {
      steps: [
        { step: "DeferredFirst", index: 0, deferredCompletion: true },
        { step: "FollowingStep", index: 1 }
      ],
      transitions: [
        { from: "DeferredFirst", to: "FollowingStep", relationKind: "primary" }
      ]
    },
    events: [
      { event: "await_interaction_dispatched", step: "DeferredFirst", itemId: "request-1" },
      { event: "start", step: "FollowingStep", from: "DeferredFirst", itemId: "completion-1" }
    ]
  });

  const requestPulses = cinematic.pulses.filter((pulse) =>
    pulse.edgeId === "DeferredFirst->KafkaBroker:request");
  const continuationPulses = cinematic.pulses.filter((pulse) =>
    pulse.edgeId === "DeferredFirst->FollowingStep:primary");

  assert.equal(requestPulses.length, 1);
  assert.equal(continuationPulses.length, 1);
  assert.ok(continuationPulses[0].start >= cinematic.phases.completion[0]);
});

test("suppresses only the exact deferred source emit receipt", () => {
  const explicitEmitCounterEvidence = new Set([
    downstreamReceiptEvidenceKey("FirstTarget", "first-item")
  ]);

  assert.equal(deferredStartReceiptCount(
    true,
    explicitEmitCounterEvidence,
    "FirstTarget",
    ["first-item"],
    1
  ), 0);
  assert.equal(deferredStartReceiptCount(
    true,
    explicitEmitCounterEvidence,
    "FirstTarget",
    ["first-item", "second-item"],
    2
  ), 1);
  assert.equal(deferredStartReceiptCount(
    true,
    explicitEmitCounterEvidence,
    "SecondTarget",
    ["first-item"],
    1
  ), 1);
  assert.equal(deferredStartReceiptCount(
    false,
    explicitEmitCounterEvidence,
    "FirstTarget",
    ["first-item"],
    1
  ), 0);
});

test("CSV built-in deferred completion has unique support actors and 1,000 final outputs", () => {
  const replay = JSON.parse(readFileSync(
    path.join(repoRoot, "tools", "replay-viewer", "datasets", "csv-payments-built-in.json"),
    "utf8"
  ));
  const deferredStep = replay.topology.steps.find(isDeferredCompletionStep);
  assert.equal(deferredStep?.step, "ProcessCsvPaymentsInput");
  assert.equal(new Set(replay.topology.steps.map((step) => step.step)).size, replay.topology.steps.length);
  assert.equal(new Set(replay.topology.transitions.map((transition) => transition.id)).size, replay.topology.transitions.length);

  const broker = replay.topology.steps.find((step) => step.renderRole === "broker");
  const provider = replay.topology.steps.find((step) => step.renderRole === "external-provider");
  assert.equal(broker?.parentStep, deferredStep.step);
  assert.equal(provider?.parentStep, deferredStep.step);
  assert.notEqual(provider?.step, deferredStep.step);

  const awaitEvents = replay.events.filter((event) => event.step === deferredStep.step);
  const lifecycleCounterEvidence = awaitEvents.some((event) => hasAwaitLifecycleCounterEvidence(
    event.attributes?.["tpf.await.expected_item_count"],
    event.attributes?.["tpf.await.completed_item_count"]
  ));
  const finalOutputCount = replay.events
    .filter((event) => event.event === "emit" && event.step === deferredStep.step)
    .reduce((count, event) => count + (event.itemId ? 1 : 0), 0);

  assert.equal(lifecycleCounterEvidence, false);
  assert.equal(finalOutputCount, 1_000);
});
