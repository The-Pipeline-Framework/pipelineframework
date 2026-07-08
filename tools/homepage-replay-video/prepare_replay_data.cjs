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
  publish: [10.75, 11.55],
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
  if (isObjectIngestStep(step)) {
    return "object-ingest";
  }
  if (isObjectPublishStep(step)) {
    return "object-publish";
  }
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

function isObjectIngestStep(step) {
  return step?.step === "ObjectIngest"
    || step?.runtimeStepClass === "runtime::ObjectIngest"
    || step?.service === "ObjectIngestConnector";
}

function isObjectPublishStep(step) {
  return step?.step === "ObjectPublish"
    || step?.runtimeStepClass === "runtime::ObjectPublish"
    || step?.service === "ObjectPublishConnector";
}

function isConnectorStep(step) {
  return isObjectIngestStep(step) || isObjectPublishStep(step);
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
  const branchLayout = analyzePrimaryBranchLayout(steps, transitions);
  const baseSteps = steps
    .filter((step) => step && step.sideEffect !== true && !isConnectorStep(step))
    .sort((left, right) => (left.index ?? Number.MAX_SAFE_INTEGER) - (right.index ?? Number.MAX_SAFE_INTEGER));
  return baseSteps.filter((step) => !branchLayout.branchNodes.has(step.step));
}

function analyzePrimaryBranchLayout(steps, transitions) {
  const baseSteps = steps
    .filter((step) => step && step.sideEffect !== true && !isConnectorStep(step))
    .sort((left, right) => (left.index ?? Number.MAX_SAFE_INTEGER) - (right.index ?? Number.MAX_SAFE_INTEGER));
  const baseStepNames = new Set(baseSteps.map((step) => step.step));
  const primaryTransitions = transitions.filter((transition) =>
    (transition?.relationKind ?? "primary") === "primary"
      && baseStepNames.has(transition.from)
      && baseStepNames.has(transition.to));
  const outbound = new Map();
  const inbound = new Map();
  const stepIndex = new Map(baseSteps.map((step, index) => [step.step, index]));
  for (const step of baseSteps) {
    outbound.set(step.step, []);
    inbound.set(step.step, []);
  }
  for (const transition of primaryTransitions) {
    outbound.get(transition.from)?.push(transition.to);
    inbound.get(transition.to)?.push(transition.from);
  }

  const branchNodes = new Set();
  const branchGroups = [];
  for (const step of baseSteps) {
    const branchStarts = outbound.get(step.step) ?? [];
    if (branchStarts.length < 2) {
      continue;
    }
    const merge = findPrimaryBranchMerge(branchStarts, outbound, inbound, stepIndex);
    if (!merge) {
      continue;
    }
    const paths = branchStarts
      .map((branchStart) => tracePrimaryBranchPath(branchStart, merge, outbound))
      .filter((path) => path.length > 0);
    if (paths.length < 2) {
      continue;
    }
    for (const path of paths) {
      for (const branchNode of path) {
        branchNodes.add(branchNode);
      }
    }
    branchGroups.push({ split: step.step, merge, paths });
  }
  return { branchNodes, branchGroups };
}

function findPrimaryBranchMerge(branchStarts, outbound, inbound, stepIndex) {
  let shared = null;
  for (const branchStart of branchStarts) {
    const descendants = collectPrimaryDescendants(branchStart, outbound);
    shared = shared == null
      ? descendants
      : new Set([...shared].filter((candidate) => descendants.has(candidate)));
  }
  if (!shared || shared.size === 0) {
    return null;
  }
  return [...shared]
    .filter((candidate) => (inbound.get(candidate)?.length ?? 0) > 1)
    .sort((left, right) => (stepIndex.get(left) ?? Number.MAX_SAFE_INTEGER) - (stepIndex.get(right) ?? Number.MAX_SAFE_INTEGER))[0]
    ?? null;
}

function collectPrimaryDescendants(startStep, outbound) {
  const queue = [startStep];
  const visited = new Set();
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || visited.has(current)) {
      continue;
    }
    visited.add(current);
    for (const next of outbound.get(current) ?? []) {
      if (!visited.has(next)) {
        queue.push(next);
      }
    }
  }
  return visited;
}

function tracePrimaryBranchPath(branchStart, mergeStep, outbound) {
  const path = [];
  const visited = new Set();
  let cursor = branchStart;
  while (cursor && cursor !== mergeStep && !visited.has(cursor)) {
    path.push(cursor);
    visited.add(cursor);
    const nextSteps = outbound.get(cursor) ?? [];
    if (nextSteps.length !== 1) {
      break;
    }
    cursor = nextSteps[0];
  }
  return cursor === mergeStep ? path : [];
}

function deriveProviderId(awaitStep) {
  const raw = awaitStep?.step ?? "ExternalProvider";
  const derived = raw.replace(/^Await/, "");
  return derived && derived !== raw ? derived : "ExternalProvider";
}

function buildNodeSet(steps, transitions) {
  const branchLayout = analyzePrimaryBranchLayout(steps, transitions);
  const mainline = orderedMainlineSteps(steps, transitions);
  const awaitStep = mainline.find((step) => displayRole(step) === "await");
  const explicitBroker = steps.find((step) => displayRole(step) === "broker");
  const explicitProvider = steps.find((step) => {
    const role = displayRole(step);
    return role === "provider" || role === "external-provider";
  });
  const explicitStore = steps.find((step) => displayRole(step) === "store");
  const objectIngestStep = steps.find(isObjectIngestStep);
  const objectPublishStep = steps.find(isObjectPublishStep);
  const persistenceSteps = steps.filter((step) => displayRole(step) === "persistence-plugin");
  const nodes = [];
  const businessSteps = steps
    .filter((step) => step && step.sideEffect !== true && !isConnectorStep(step))
    .sort((left, right) => (left.index ?? Number.MAX_SAFE_INTEGER) - (right.index ?? Number.MAX_SAFE_INTEGER));
  const roleByBusinessStep = new Map();
  let primaryRoleIndex = 0;
  for (const step of businessSteps) {
    const role = displayRole(step) === "await"
      ? "await"
      : PRIMARY_ROLES[Math.min(primaryRoleIndex++, PRIMARY_ROLES.length - 1)];
    roleByBusinessStep.set(step.step, role);
  }
  const primaryCount = mainline.length;
  const minX = -4.15;
  const maxX = 4.15;
  const xSpan = primaryCount <= 1 ? 0 : maxX - minX;

  if (objectIngestStep) {
    nodes.push({
      id: objectIngestStep.step,
      sourceId: objectIngestStep.step,
      role: "object-ingest",
      tier: "connector",
      x: -6.75,
      y: 2.4,
      z: 0.38,
      scale: 0.92
    });
  }

  mainline.forEach((step, index) => {
    const awaitRole = displayRole(step) === "await";
    const centerOffset = primaryCount <= 1 ? 0.5 : index / (primaryCount - 1);
    const edgeLift = Math.abs(centerOffset - 0.5) * 1.08;
    const role = roleByBusinessStep.get(step.step) ?? (awaitRole ? "await" : "primary-a");
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

  for (const group of branchLayout.branchGroups) {
    const splitNode = nodes.find((node) => node.id === group.split);
    const mergeNode = nodes.find((node) => node.id === group.merge);
    if (!splitNode || !mergeNode) {
      continue;
    }
    const span = Math.max(mergeNode.x - splitNode.x, 2.6);
    const branchY = Math.min(splitNode.y, mergeNode.y) - 2.2;
    group.paths.forEach((path, branchIndex) => {
      const laneCenter = splitNode.x + (span * (branchIndex + 1) / (group.paths.length + 1));
      path.forEach((stepId, stepIndex) => {
        const step = businessSteps.find((candidate) => candidate.step === stepId);
        if (!step) {
          return;
        }
        const offsetX = path.length === 1
          ? 0
          : ((stepIndex / (path.length - 1)) - 0.5) * Math.min(1.18, span * 0.22);
        nodes.push({
          id: step.step,
          sourceId: step.step,
          role: roleByBusinessStep.get(step.step) ?? "primary-b",
          tier: "primary",
          x: Number((laneCenter + offsetX).toFixed(3)),
          y: Number((branchY - Math.floor(stepIndex / 3) * 0.62).toFixed(3)),
          z: 0.42,
          scale: 1.02
        });
      });
    });
  }

  if (objectPublishStep) {
    nodes.push({
      id: objectPublishStep.step,
      sourceId: objectPublishStep.step,
      role: "object-publish",
      tier: "connector",
      x: 6.75,
      y: 2.4,
      z: 0.38,
      scale: 0.92
    });
  }

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
  return { nodes, mainline, awaitStep, persistenceSteps, objectIngestStep, objectPublishStep, branchLayout };
}

function edgeId(from, to, kind) {
  return `${from}->${to}:${kind}`;
}

function buildEdges(transitions, nodes, mainline, awaitStep, persistenceSteps, objectIngestStep, objectPublishStep) {
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

  for (const transition of transitions) {
    if ((transition?.relationKind ?? "primary") !== "primary") {
      continue;
    }
    addEdge(transition.from, transition.to, "primary", transition.cardinality ?? "one-to-one");
  }
  if (objectIngestStep && mainline[0]) {
    addEdge(objectIngestStep.step, mainline[0].step, "ingest");
  }
  if (objectPublishStep && mainline.length > 0) {
    addEdge(mainline[mainline.length - 1].step, objectPublishStep.step, "publish");
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

function buildPulses(replay, edges, mainline, awaitStep, persistenceSteps, objectIngestStep, objectPublishStep, branchLayout) {
  const events = Array.isArray(replay.events) ? replay.events : [];
  const pulses = [];
  const primaryEdges = edges.filter((edge) => edge.kind === "primary");
  const branchNodeNames = branchLayout?.branchNodes ?? new Set();
  const mergeStepNames = new Set((branchLayout?.branchGroups ?? []).map((group) => group.merge));

  primaryEdges.forEach((entry, index) => {
    const matches = events.filter((event) =>
      eventMatches(event, entry.from, entry.to, "emit")
        || (event.event === "start" && event.step === entry.to && event.from === entry.from)
    );
    const segment = index === 0
      ? [SEGMENTS.ingress[1] - 0.1, SEGMENTS.request[0] + 0.55]
      : entry.from === awaitStep?.step
        ? SEGMENTS.completion
        : branchNodeNames.has(entry.from) || branchNodeNames.has(entry.to) || mergeStepNames.has(entry.to)
          ? SEGMENTS.continuation
          : index === primaryEdges.length - 1
            ? SEGMENTS.continuation
            : SEGMENTS.request;
    pulses.push(...pulseBundle(entry, "primary", sampleEvenly(matches, index === 0 ? 3 : 10), ...segment, 0.86, 0.13, "#8eeeff"));
  });

  if (objectIngestStep && mainline[0]) {
    const ingestEvents = events.filter((event) =>
      event.event === "object_ingest_listed" || event.event === "object_ingest_submitted"
    );
    pulses.push(...pulseBundle(
      findEdge(edges, objectIngestStep.step, mainline[0].step, "ingest"),
      "ingest",
      sampleEvenly(ingestEvents, 3),
      SEGMENTS.ingress[0],
      SEGMENTS.ingress[1],
      0.95,
      0.135,
      "#7af7c6"
    ));
  }

  if (awaitStep) {
    const awaitIndex = mainline.findIndex((step) => step.step === awaitStep.step);
    const previous = mainline[awaitIndex - 1]?.step;
    const requestItems = sampleEvenly(
      events.filter((event) => eventMatches(event, previous, awaitStep.step, "emit")
        || (event.event === "start" && event.step === awaitStep.step && event.from === previous)),
      14
    );
    const completionItems = sampleEvenly(
      events.filter((event) => event.event === "start" && event.from === awaitStep.step),
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
  if (objectPublishStep && mainline.length > 0) {
    const publishEvents = events.filter((event) =>
      event.event === "object_publish_grouped" || event.event === "object_publish_published"
    );
    pulses.push(...pulseBundle(
      findEdge(edges, mainline[mainline.length - 1].step, objectPublishStep.step, "publish"),
      "publish",
      sampleEvenly(publishEvents, 3),
      SEGMENTS.publish[0],
      SEGMENTS.publish[1],
      0.45,
      0.135,
      "#ffe08a"
    ));
  }
  return pulses.sort((left, right) => left.start - right.start);
}

function buildHighlights(nodes) {
  return [
    { targetId: nodes.find((node) => node.role === "object-ingest")?.id, kind: "ingest", start: SEGMENTS.ingress[0], end: SEGMENTS.ingress[1], intensity: 1.05, color: "#7af7c6" },
    { targetId: nodes.find((node) => node.role === "await")?.id, kind: "suspend", start: SEGMENTS.suspend[0], end: SEGMENTS.suspend[1], intensity: 1.25, color: "#95b8ff" },
    { targetId: nodes.find((node) => node.role === "store")?.id, kind: "store", start: SEGMENTS.store[0], end: SEGMENTS.store[1], intensity: 0.85, color: "#76d8ff" },
    { targetId: nodes.find((node) => node.role === "provider")?.id, kind: "completion", start: SEGMENTS.completion[0], end: SEGMENTS.completion[1] - 0.1, intensity: 0.95, color: "#d2c3ff" },
    { targetId: nodes.find((node) => node.role === "object-publish")?.id, kind: "publish", start: SEGMENTS.publish[0], end: SEGMENTS.publish[1], intensity: 1.1, color: "#ffe08a" }
  ].filter((highlight) => highlight.targetId);
}

function main() {
  const { input, output } = parseArgs(process.argv);
  const replay = JSON.parse(fs.readFileSync(input, "utf8"));
  const steps = Array.isArray(replay.topology?.steps) ? replay.topology.steps : [];
  const transitions = Array.isArray(replay.topology?.transitions) ? replay.topology.transitions : [];
  const { nodes, mainline, awaitStep, persistenceSteps, objectIngestStep, objectPublishStep, branchLayout } = buildNodeSet(steps, transitions);
  const edges = buildEdges(transitions, nodes, mainline, awaitStep, persistenceSteps, objectIngestStep, objectPublishStep);
  const cinematic = {
    sourceReplay: path
      .relative(path.resolve(__dirname, "../.."), path.resolve(input))
      .split(path.sep)
      .join("/"),
    pipeline: replay.pipeline,
    status: replay.status,
    clipDurationSeconds: CLIP_DURATION_SECONDS,
    fps: FPS,
    viewport: { width: 1600, height: 900 },
    phases: SEGMENTS,
    nodes,
    edges,
    pulses: buildPulses(replay, edges, mainline, awaitStep, persistenceSteps, objectIngestStep, objectPublishStep, branchLayout),
    highlights: buildHighlights(nodes)
  };
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, JSON.stringify(cinematic, null, 2) + "\n");
}

main();
