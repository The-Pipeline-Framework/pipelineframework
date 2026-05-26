#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const CLIP_DURATION_SECONDS = 12;
const FPS = 24;
const SEGMENTS = {
  ingress: [0.55, 1.5],
  request: [1.7, 5.15],
  suspend: [5.05, 6.15],
  completion: [6.35, 8.95],
  continuation: [8.55, 10.85],
  store: [0.95, 6.55]
};

function parseArgs(argv) {
  const args = {};
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === "--input") {
      args.input = next;
      index += 1;
    } else if (token === "--output") {
      args.output = next;
      index += 1;
    }
  }
  if (!args.input || !args.output) {
    throw new Error("Usage: prepare_replay_data.cjs --input <replay.json> --output <cinematic.json>");
  }
  return args;
}

function sampleEvenly(items, count) {
  if (!Array.isArray(items) || items.length === 0 || count <= 0) {
    return [];
  }
  if (count === 1) {
    return [items[Math.floor((items.length - 1) / 2)]];
  }
  if (items.length <= count) {
    return items;
  }
  const indexes = new Set();
  for (let index = 0; index < count; index += 1) {
    indexes.add(Math.round(index * (items.length - 1) / (count - 1)));
  }
  return [...indexes].sort((left, right) => left - right).map((index) => items[index]);
}

function spreadAcrossSegment(items, segmentStart, segmentEnd) {
  if (items.length === 0) {
    return [];
  }
  if (items.length === 1) {
    return [{ ...items[0], start: Number(((segmentStart + segmentEnd) / 2).toFixed(3)) }];
  }
  const span = segmentEnd - segmentStart;
  return items.map((item, index) => ({
    ...item,
    start: Number((segmentStart + (span * index / (items.length - 1))).toFixed(3))
  }));
}

function findStep(steps, predicate, label) {
  const step = steps.find(predicate);
  if (!step) {
    throw new Error(`Missing required step for homepage cinematic: ${label}`);
  }
  return step;
}

function buildNodes(steps) {
  const folder = findStep(steps, (step) => step.step === "ProcessFolder", "ProcessFolder");
  const input = findStep(steps, (step) => step.step === "ProcessCsvPaymentsInput", "ProcessCsvPaymentsInput");
  const awaitStep = findStep(steps, (step) => step.renderRole === "await", "await");
  const status = findStep(steps, (step) => step.step === "ProcessPaymentStatus", "ProcessPaymentStatus");
  const output = findStep(steps, (step) => step.step === "ProcessCsvPaymentsOutputFile", "ProcessCsvPaymentsOutputFile");
  const broker = findStep(steps, (step) => step.renderRole === "broker", "broker");
  const provider = findStep(steps, (step) => step.renderRole === "external-provider", "external-provider");
  const store = findStep(steps, (step) => step.renderRole === "store", "store");

  return [
    { id: folder.step, role: "folder", tier: "primary", x: -6.0, y: 2.55, z: 0.45, scale: 1.06 },
    { id: input.step, role: "input", tier: "primary", x: -3.15, y: 2.05, z: 0.8, scale: 1.1 },
    { id: awaitStep.step, role: "await", tier: "primary", x: 0.05, y: 1.7, z: 1.05, scale: 1.22 },
    { id: status.step, role: "status", tier: "primary", x: 3.3, y: 2.08, z: 0.95, scale: 1.12 },
    { id: output.step, role: "output", tier: "primary", x: 6.25, y: 2.52, z: 0.5, scale: 1.02 },
    { id: broker.step, role: "broker", tier: "support", x: -1.75, y: 4.05, z: 0.92, scale: 0.84, parentId: awaitStep.step },
    { id: provider.step, role: "provider", tier: "support", x: 2.1, y: 4.45, z: 0.22, scale: 0.92, parentId: awaitStep.step },
    { id: store.step, role: "store", tier: "support", x: 0.65, y: -3.2, z: -1.85, scale: 0.96 }
  ];
}

function buildEdges(transitions, nodes) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const keepKinds = new Set(["primary", "await-request", "await-completion", "store"]);
  const edges = transitions
    .filter((transition) => keepKinds.has(transition.relationKind || "primary"))
    .map((transition) => ({
      id: transition.id || `${transition.from}->${transition.to}`,
      from: transition.from,
      to: transition.to,
      kind: transition.relationKind || "primary",
      cardinality: transition.cardinality || "one-to-one"
    }))
    .filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to));
  const edgeIds = new Set(edges.map((edge) => edge.id));
  [
    { from: "ProcessPaymentStatus", to: "Database", kind: "store", cardinality: "one-to-one" },
    { from: "ProcessCsvPaymentsOutputFile", to: "Database", kind: "store", cardinality: "one-to-one" }
  ].forEach((edge) => {
    const id = `${edge.from}->${edge.to}`;
    if (nodeIds.has(edge.from) && nodeIds.has(edge.to) && !edgeIds.has(id)) {
      edges.push({ id, ...edge });
      edgeIds.add(id);
    }
  });
  return edges;
}

function eventMatches(event, fromStep, toStep, eventName) {
  if (!event || event.event !== eventName) {
    return false;
  }
  if (fromStep && event.from !== fromStep) {
    return false;
  }
  if (toStep && event.to !== toStep) {
    return false;
  }
  return true;
}

function pulseBundle(edgeId, kind, items, segmentStart, segmentEnd, duration, size, color) {
  return spreadAcrossSegment(items, segmentStart, segmentEnd).map((item, index) => ({
    id: `${edgeId}:${kind}:${index + 1}`,
    edgeId,
    kind,
    start: item.start,
    duration,
    size,
    color
  }));
}

function buildPulses(replay, edges) {
  const events = Array.isArray(replay.events) ? replay.events : [];
  const pulses = [];

  const folderToInputEmits = sampleEvenly(
    events.filter((event) => eventMatches(event, "ProcessFolder", "ProcessCsvPaymentsInput", "emit")),
    3
  );
  const inputToAwaitEmits = sampleEvenly(
    events.filter((event) => eventMatches(event, "ProcessCsvPaymentsInput", "AwaitPaymentProvider", "emit")),
    14
  );
  const awaitResumeStarts = sampleEvenly(
    events.filter((event) => event.event === "start" && event.step === "ProcessPaymentStatus" && event.from === "AwaitPaymentProvider"),
    10
  );
  const outputStarts = sampleEvenly(
    events.filter((event) => event.event === "start" && event.step === "ProcessCsvPaymentsOutputFile" && event.from === "ProcessPaymentStatus"),
    5
  );
  const storeFolderWrites = sampleEvenly(
    events.filter((event) => event.event === "success" && event.step === "PersistenceCsvPaymentsInputFileSideEffect"),
    2
  );
  const storeInputWrites = sampleEvenly(
    events.filter((event) => event.event === "success" && event.step === "PersistencePaymentRecordSideEffect"),
    12
  );
  const storeStatusWrites = sampleEvenly(awaitResumeStarts, 7);
  const storeOutputWrites = sampleEvenly(outputStarts, 4);

  const ensureEdgeId = (from, to) => edges.find((edge) => edge.from === from && edge.to === to)?.id ?? `${from}->${to}`;

  pulses.push(...pulseBundle(ensureEdgeId("ProcessFolder", "ProcessCsvPaymentsInput"), "primary", folderToInputEmits, ...SEGMENTS.ingress, 0.78, 0.14, "#7de3ff"));
  pulses.push(...pulseBundle(ensureEdgeId("ProcessCsvPaymentsInput", "AwaitPaymentProvider"), "primary", inputToAwaitEmits, ...SEGMENTS.request, 0.86, 0.13, "#96f5ff"));
  pulses.push(...pulseBundle(ensureEdgeId("AwaitPaymentProvider", "KafkaBroker"), "request", inputToAwaitEmits, SEGMENTS.request[0] + 0.08, SEGMENTS.request[1] - 0.22, 0.92, 0.12, "#8fa9ff"));
  pulses.push(...pulseBundle(ensureEdgeId("KafkaBroker", "PaymentProvider"), "request", inputToAwaitEmits, SEGMENTS.request[0] + 0.22, SEGMENTS.request[1], 0.96, 0.118, "#c1b4ff"));
  pulses.push(...pulseBundle(ensureEdgeId("PaymentProvider", "KafkaBroker"), "completion", awaitResumeStarts, SEGMENTS.completion[0], SEGMENTS.completion[1] - 0.22, 0.96, 0.118, "#c1b4ff"));
  pulses.push(...pulseBundle(ensureEdgeId("KafkaBroker", "AwaitPaymentProvider"), "completion", awaitResumeStarts, SEGMENTS.completion[0] + 0.16, SEGMENTS.completion[1], 0.92, 0.12, "#8fa9ff"));
  pulses.push(...pulseBundle(ensureEdgeId("AwaitPaymentProvider", "ProcessPaymentStatus"), "primary", awaitResumeStarts, SEGMENTS.completion[0] + 0.28, SEGMENTS.completion[1], 1.02, 0.13, "#85f1d2"));
  pulses.push(...pulseBundle(ensureEdgeId("ProcessPaymentStatus", "ProcessCsvPaymentsOutputFile"), "primary", outputStarts, ...SEGMENTS.continuation, 0.9, 0.12, "#7fe5c4"));
  pulses.push(...pulseBundle(ensureEdgeId("ProcessFolder", "Database"), "store", storeFolderWrites, SEGMENTS.store[0], SEGMENTS.store[0] + 0.9, 1.1, 0.12, "#6fd9ff"));
  pulses.push(...pulseBundle(ensureEdgeId("ProcessCsvPaymentsInput", "Database"), "store", storeInputWrites, SEGMENTS.store[0] + 0.65, SEGMENTS.store[1], 1.18, 0.12, "#6fd9ff"));
  pulses.push(...pulseBundle(ensureEdgeId("ProcessPaymentStatus", "Database"), "store", storeStatusWrites, SEGMENTS.completion[0] + 0.18, SEGMENTS.continuation[0] + 0.15, 1.02, 0.115, "#6fd9ff"));
  pulses.push(...pulseBundle(ensureEdgeId("ProcessCsvPaymentsOutputFile", "Database"), "store", storeOutputWrites, SEGMENTS.continuation[0] + 0.2, SEGMENTS.continuation[1], 0.96, 0.11, "#6fd9ff"));

  return pulses.sort((left, right) => left.start - right.start);
}

function buildHighlights() {
  return [
    { targetId: "AwaitPaymentProvider", kind: "suspend", start: SEGMENTS.suspend[0], end: SEGMENTS.suspend[1], intensity: 1.25, color: "#95b8ff" },
    { targetId: "Database", kind: "store", start: SEGMENTS.store[0], end: SEGMENTS.store[1], intensity: 0.85, color: "#76d8ff" },
    { targetId: "PaymentProvider", kind: "completion", start: SEGMENTS.completion[0], end: SEGMENTS.completion[1] - 0.1, intensity: 0.95, color: "#d2c3ff" }
  ];
}

function main() {
  const { input, output } = parseArgs(process.argv);
  const replay = JSON.parse(fs.readFileSync(input, "utf8"));
  const steps = Array.isArray(replay.topology?.steps) ? replay.topology.steps : [];
  const transitions = Array.isArray(replay.topology?.transitions) ? replay.topology.transitions : [];
  const nodes = buildNodes(steps);
  const edges = buildEdges(transitions, nodes);
  const cinematic = {
    sourceReplay: path.relative(process.cwd(), input),
    pipeline: replay.pipeline,
    status: replay.status,
    clipDurationSeconds: CLIP_DURATION_SECONDS,
    fps: FPS,
    viewport: { width: 1600, height: 900 },
    phases: SEGMENTS,
    nodes,
    edges,
    pulses: buildPulses(replay, edges),
    highlights: buildHighlights()
  };
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, JSON.stringify(cinematic, null, 2) + "\n");
}

main();
