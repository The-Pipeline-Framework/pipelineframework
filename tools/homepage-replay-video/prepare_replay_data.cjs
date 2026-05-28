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
const PRIMARY_ROLES = ["primary-a", "primary-b", "primary-c", "primary-d", "primary-e"];

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

function displayRole(step) {
  if (step?.renderRole) {
    return step.renderRole;
  }
  if (step?.sideEffect === true && step?.pluginKind === "persistence") {
    return "persistence-plugin";
  }
  if (typeof step?.step === "string" && step.step.toLowerCase().includes("await")) {
    return "await";
  }
  return "primary";
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

function orderedMainlineSteps(steps, transitions) {
  const mainlineSteps = steps
    .filter((step) => step && step.sideEffect !== true)
    .sort((left, right) => (left.index ?? Number.MAX_SAFE_INTEGER) - (right.index ?? Number.MAX_SAFE_INTEGER));
  const mainlineIds = new Set(mainlineSteps.map((step) => step.step));
  const mainlineTransitions = transitions.filter((transition) => mainlineIds.has(transition.from) && mainlineIds.has(transition.to));
  const targets = new Set(mainlineTransitions.map((transition) => transition.to));
  const starts = mainlineSteps.filter((step) => !targets.has(step.step));
  const start = starts[0] ?? mainlineSteps[0];
  if (!start) {
    return [];
  }
  const byFrom = new Map();
  for (const transition of mainlineTransitions) {
    if (!byFrom.has(transition.from)) {
      byFrom.set(transition.from, transition);
    }
  }
  const byId = new Map(mainlineSteps.map((step) => [step.step, step]));
  const ordered = [];
  const visited = new Set();
  let cursor = start.step;
  while (cursor && byId.has(cursor) && !visited.has(cursor)) {
    visited.add(cursor);
    ordered.push(byId.get(cursor));
    cursor = byFrom.get(cursor)?.to;
  }
  for (const step of mainlineSteps) {
    if (!visited.has(step.step)) {
      ordered.push(step);
    }
  }
  return ordered;
}

function deriveProviderId(awaitStep) {
  const raw = awaitStep?.step ?? "ExternalProvider";
  const derived = raw.replace(/^Await/, "");
  return derived && derived !== raw ? derived : "ExternalProvider";
}

function buildNodeSet(steps, transitions) {
  const mainline = orderedMainlineSteps(steps, transitions);
  const awaitStep = mainline.find((step) => displayRole(step) === "await");
  const explicitBroker = steps.find((step) => displayRole(step) === "broker");
  const explicitProvider = steps.find((step) => {
    const role = displayRole(step);
    return role === "provider" || role === "external-provider";
  });
  const explicitStore = steps.find((step) => displayRole(step) === "store");
  const persistenceSteps = steps.filter((step) => displayRole(step) === "persistence-plugin");
  const nodes = [];
  const primaryCount = mainline.length;
  const minX = -6.1;
  const maxX = 6.1;
  const xSpan = primaryCount <= 1 ? 0 : maxX - minX;
  let primaryRoleIndex = 0;

  mainline.forEach((step, index) => {
    const awaitRole = displayRole(step) === "await";
    const centerOffset = primaryCount <= 1 ? 0.5 : index / (primaryCount - 1);
    const edgeLift = Math.abs(centerOffset - 0.5) * 1.08;
    const role = awaitRole ? "await" : PRIMARY_ROLES[Math.min(primaryRoleIndex++, PRIMARY_ROLES.length - 1)];
    nodes.push({
      id: step.step,
      sourceId: step.step,
      role,
      tier: "primary",
      x: Number((minX + xSpan * centerOffset).toFixed(3)),
      y: Number((1.55 + edgeLift).toFixed(3)),
      z: Number((0.55 + (awaitRole ? 0.6 : 0.25 * (1 - edgeLift))).toFixed(3)),
      scale: awaitRole ? 1.22 : 1.08
    });
  });

  if (awaitStep) {
    const awaitNode = nodes.find((node) => node.id === awaitStep.step);
    const brokerId = explicitBroker?.step ?? "KafkaBroker";
    const providerId = explicitProvider?.step ?? deriveProviderId(awaitStep);
    nodes.push({
      id: brokerId,
      sourceId: explicitBroker?.step,
      role: "broker",
      tier: "support",
      x: Number(((awaitNode?.x ?? 0) - 1.9).toFixed(3)),
      y: 4.18,
      z: 0.92,
      scale: 0.84,
      parentId: awaitStep.step
    });
    nodes.push({
      id: providerId,
      sourceId: explicitProvider?.step,
      role: "provider",
      tier: "support",
      x: Number(((awaitNode?.x ?? 0) + 2.05).toFixed(3)),
      y: 4.52,
      z: 0.22,
      scale: 0.92,
      parentId: awaitStep.step
    });
  }

  if (explicitStore || persistenceSteps.length > 0) {
    nodes.push({
      id: explicitStore?.step ?? "Database",
      sourceId: explicitStore?.step,
      role: "store",
      tier: "support",
      x: 0.65,
      y: -2.52,
      z: -1.85,
      scale: 0.86
    });
  }
  return { nodes, mainline, awaitStep, persistenceSteps };
}

function edgeId(from, to, kind) {
  return `${from}->${to}:${kind}`;
}

function buildEdges(transitions, nodes, mainline, awaitStep, persistenceSteps) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const storeNode = nodes.find((node) => node.role === "store");
  const brokerNode = nodes.find((node) => node.role === "broker");
  const providerNode = nodes.find((node) => node.role === "provider");
  const edges = [];
  const seen = new Set();
  const addEdge = (from, to, kind, cardinality = "one-to-one") => {
    if (!from || !to || !nodeIds.has(from) || !nodeIds.has(to)) {
      return;
    }
    const id = edgeId(from, to, kind);
    if (!seen.has(id)) {
      seen.add(id);
      edges.push({ id, from, to, kind, cardinality });
    }
  };

  for (let index = 0; index < mainline.length - 1; index += 1) {
    const from = mainline[index].step;
    const to = mainline[index + 1].step;
    const transition = transitions.find((candidate) => candidate.from === from && candidate.to === to);
    if (!transition) {
      continue;
    }
    addEdge(from, to, "primary", transition.cardinality ?? "one-to-one");
  }
  if (awaitStep && brokerNode && providerNode) {
    addEdge(awaitStep.step, brokerNode.id, "request");
    addEdge(brokerNode.id, providerNode.id, "request");
    addEdge(providerNode.id, brokerNode.id, "completion");
    addEdge(brokerNode.id, awaitStep.step, "completion");
  }
  if (storeNode) {
    const sideEffectParents = new Set(
      persistenceSteps
        .map((step) => step.parentStep ?? transitions.find((transition) => transition.to === step.step)?.from)
        .filter(Boolean)
    );
    for (const parent of sideEffectParents) {
      addEdge(parent, storeNode.id, "store");
    }
  }
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

function pulseBundle(edge, kind, items, segmentStart, segmentEnd, duration, size, color) {
  if (!edge) {
    return [];
  }
  return spreadAcrossSegment(items, segmentStart, segmentEnd).map((item, index) => ({
    id: `${edge.id}:${kind}:${index + 1}`,
    edgeId: edge.id,
    kind,
    start: item.start,
    duration,
    size,
    color
  }));
}

function findEdge(edges, from, to, kind) {
  return edges.find((edge) => edge.from === from && edge.to === to && edge.kind === kind);
}

function buildPulses(replay, edges, mainline, awaitStep, persistenceSteps) {
  const events = Array.isArray(replay.events) ? replay.events : [];
  const pulses = [];
  const mainlineEdges = [];
  for (let index = 0; index < mainline.length - 1; index += 1) {
    const from = mainline[index].step;
    const to = mainline[index + 1].step;
    mainlineEdges.push({ from, to, edge: findEdge(edges, from, to, "primary") });
  }

  mainlineEdges.forEach((entry, index) => {
    const matches = events.filter((event) =>
      eventMatches(event, entry.from, entry.to, "emit")
        || (event.event === "start" && event.step === entry.to && event.from === entry.from)
    );
    const segment = index === 0
      ? SEGMENTS.ingress
      : entry.from === awaitStep?.step
        ? SEGMENTS.completion
        : index === mainlineEdges.length - 1
          ? SEGMENTS.continuation
          : SEGMENTS.request;
    pulses.push(...pulseBundle(entry.edge, "primary", sampleEvenly(matches, index === 0 ? 3 : 12), ...segment, 0.86, 0.13, "#8eeeff"));
  });

  if (awaitStep) {
    const awaitIndex = mainline.findIndex((step) => step.step === awaitStep.step);
    const previous = mainline[awaitIndex - 1]?.step;
    const next = mainline[awaitIndex + 1]?.step;
    const requestItems = sampleEvenly(
      events.filter((event) => eventMatches(event, previous, awaitStep.step, "emit")
        || (event.event === "start" && event.step === awaitStep.step && event.from === previous)),
      14
    );
    const completionItems = sampleEvenly(
      events.filter((event) => event.event === "start" && event.step === next && event.from === awaitStep.step),
      10
    );
    const broker = edges.find((edge) => edge.from === awaitStep.step && edge.kind === "request")?.to;
    const provider = edges.find((edge) => edge.from === broker && edge.kind === "request")?.to;
    pulses.push(...pulseBundle(findEdge(edges, awaitStep.step, broker, "request"), "request", requestItems, SEGMENTS.request[0] + 0.08, SEGMENTS.request[1] - 0.22, 0.92, 0.12, "#8fa9ff"));
    pulses.push(...pulseBundle(findEdge(edges, broker, provider, "request"), "request", requestItems, SEGMENTS.request[0] + 0.22, SEGMENTS.request[1], 0.96, 0.118, "#c1b4ff"));
    pulses.push(...pulseBundle(findEdge(edges, provider, broker, "completion"), "completion", completionItems, SEGMENTS.completion[0], SEGMENTS.completion[1] - 0.22, 0.96, 0.118, "#c1b4ff"));
    pulses.push(...pulseBundle(findEdge(edges, broker, awaitStep.step, "completion"), "completion", completionItems, SEGMENTS.completion[0] + 0.16, SEGMENTS.completion[1], 0.92, 0.12, "#8fa9ff"));
  }

  const sideEffectByParent = new Map();
  for (const step of persistenceSteps) {
    const parent = step.parentStep ?? replay.topology?.transitions?.find((transition) => transition.to === step.step)?.from;
    if (parent) {
      if (!sideEffectByParent.has(parent)) {
        sideEffectByParent.set(parent, []);
      }
      sideEffectByParent.get(parent).push(step.step);
    }
  }
  for (const edge of edges.filter((candidate) => candidate.kind === "store")) {
    const sideEffects = sideEffectByParent.get(edge.from) ?? [];
    const writes = events.filter((event) => event.event === "success" && sideEffects.includes(event.step));
    const mainlineIndex = mainline.findIndex((step) => step.step === edge.from);
    const segmentStart = mainlineIndex <= 1 ? SEGMENTS.store[0] + Math.max(0, mainlineIndex) * 0.65 : SEGMENTS.completion[0] + 0.18;
    const segmentEnd = mainlineIndex >= mainline.length - 1 ? SEGMENTS.continuation[1] : Math.min(SEGMENTS.store[1], segmentStart + 2.8);
    pulses.push(...pulseBundle(edge, "store", sampleEvenly(writes, mainlineIndex <= 0 ? 2 : 8), segmentStart, segmentEnd, 1.08, 0.115, "#6fd9ff"));
  }
  return pulses.sort((left, right) => left.start - right.start);
}

function buildHighlights(nodes) {
  return [
    { targetId: nodes.find((node) => node.role === "await")?.id, kind: "suspend", start: SEGMENTS.suspend[0], end: SEGMENTS.suspend[1], intensity: 1.25, color: "#95b8ff" },
    { targetId: nodes.find((node) => node.role === "store")?.id, kind: "store", start: SEGMENTS.store[0], end: SEGMENTS.store[1], intensity: 0.85, color: "#76d8ff" },
    { targetId: nodes.find((node) => node.role === "provider")?.id, kind: "completion", start: SEGMENTS.completion[0], end: SEGMENTS.completion[1] - 0.1, intensity: 0.95, color: "#d2c3ff" }
  ].filter((highlight) => highlight.targetId);
}

function main() {
  const { input, output } = parseArgs(process.argv);
  const replay = JSON.parse(fs.readFileSync(input, "utf8"));
  const steps = Array.isArray(replay.topology?.steps) ? replay.topology.steps : [];
  const transitions = Array.isArray(replay.topology?.transitions) ? replay.topology.transitions : [];
  const { nodes, mainline, awaitStep, persistenceSteps } = buildNodeSet(steps, transitions);
  const edges = buildEdges(transitions, nodes, mainline, awaitStep, persistenceSteps);
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
    pulses: buildPulses(replay, edges, mainline, awaitStep, persistenceSteps),
    highlights: buildHighlights(nodes)
  };
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, JSON.stringify(cinematic, null, 2) + "\n");
}

main();
