import * as THREE from "./vendor/three.module.min.js";
import { BUILT_IN_REPLAYS_CONFIG } from "./built-in-replays.js";

const mount = document.getElementById("threeMount");
const appShell = document.querySelector(".app-shell");
const viewport = document.querySelector(".viewport");
const playerSurface = document.getElementById("playerSurface");
const playerChrome = document.getElementById("playerChrome");
const backToDocsLink = document.getElementById("backToDocsLink");
const loadReplayButton = document.getElementById("loadReplayButton");
const playPauseButton = document.getElementById("playPauseButton");
const stopButton = document.getElementById("stopButton");
const restartButton = document.getElementById("restartButton");
const stepBackButton = document.getElementById("stepBackButton");
const stepButton = document.getElementById("stepButton");
const infoButton = document.getElementById("infoButton");
const infoModal = document.getElementById("infoModal");
const infoCloseButton = document.getElementById("infoCloseButton");
const sourceModal = document.getElementById("sourceModal");
const sourceCloseButton = document.getElementById("sourceCloseButton");
const sourceCancelButton = document.getElementById("sourceCancelButton");
const sourceModalTitle = document.getElementById("sourceModalTitle");
const sourceModalDescription = document.getElementById("sourceModalDescription");
const sourceModalStatus = document.getElementById("sourceModalStatus");
const fullscreenButton = document.getElementById("fullscreenButton");
const datasetSelect = document.getElementById("datasetSelect");
const sourceApplyButton = document.getElementById("sourceApplyButton");
const resetLayoutButton = document.getElementById("resetLayoutButton");
const customReplayInputWrap = document.getElementById("customReplayInputWrap");
const replayFileInput = document.getElementById("replayFileInput");
const speedInputs = [...document.querySelectorAll('input[name="speed"]')];
const timelineSlider = document.getElementById("timelineSlider");
const playerTitle = document.getElementById("playerTitle");
const playbackText = document.getElementById("playbackText");
const summaryDatasetName = document.getElementById("summaryDatasetName");
const summaryPipelineName = document.getElementById("summaryPipelineName");
const summaryDurationText = document.getElementById("summaryDurationText");
const summaryTopologyText = document.getElementById("summaryTopologyText");
const summaryEventCountText = document.getElementById("summaryEventCountText");
const rejectLegendItems = [...document.querySelectorAll('[data-support-kind="reject"]')];
const loadProgress = document.getElementById("loadProgress");
const loadProgressFill = document.getElementById("loadProgressFill");
const loadProgressText = document.getElementById("loadProgressText");
const runParametersContent = document.getElementById("runParametersContent");
const runtimeStatus = document.getElementById("runtimeStatus");
const CAMERA_PADDING = 1.2;
const BASE_NODE_RADIUS = 0.68;
const BRANCH_NODE_RADIUS = 0.6;
const BASE_LABEL_HEIGHT = 0.8;
const SUPPORT_LABEL_HEIGHT = 0.5;
const LABEL_OFFSET_Y = 1.12;
const COUNTER_LABEL_HEIGHT = 0.44;
const PRESSURE_RING_START_ANGLE = (Math.PI * 5) / 4;
const PRESSURE_RING_SWEEP_ANGLE = -(Math.PI * 3) / 2;
const PRIMARY_ROW_Y = 2.45;
const BRANCH_ROW_OFFSET_Y = 2.05;
const EMPTY_REPLAY_SOURCE_KEY = "none";
const DEFAULT_REPLAY_SOURCE_KEY = EMPTY_REPLAY_SOURCE_KEY;
const FIRST_VISIT_AUTOLOAD_SOURCE_KEY = "csv-payments";
const FIRST_VISIT_AUTOLOAD_STORAGE_KEY = "tpf-replay-viewer-first-visit-autoloaded:v1";
const COMPLETION_PROMPT_DELAY_MS = 3000;
const LAYOUT_STORAGE_PREFIX = "tpf-replay-viewer-layout";
const BUILT_IN_REPLAYS = new Map(BUILT_IN_REPLAYS_CONFIG.map((entry) => [entry.key, { label: entry.label, path: entry.path }]));
const EFFECT_PRESETS = {
  pulse: {
    start: { duration: 0.72, startScale: 0.92, endScale: 2.7, opacity: 0.96 },
    success: { duration: 0.82, startScale: 1.06, endScale: 2.28, opacity: 0.72 },
    retryPrimary: { duration: 1.55, startScale: 1.05, endScale: 3.4, opacity: 1 },
    retrySecondary: { duration: 1.05, startScale: 0.92, endScale: 2.3, opacity: 0.62 },
    errorPrimary: { duration: 1.18, startScale: 1.08, endScale: 3.95, opacity: 0.96 },
    errorSecondary: { duration: 0.92, startScale: 0.94, endScale: 2.45, opacity: 0.58 }
  },
  burst: {
    duration: 1.18,
    endScale: 3.9,
    opacity: 0.98
  },
  shockwave: {
    duration: 1.4,
    startScale: 1.3,
    endScale: 4.7,
    opacity: 0.54
  },
  retryLoop: {
    duration: 2.2,
    radius: 0.82,
    verticalDrift: 0.42,
    revolutions: 5.6,
    opacity: 1
  },
  background: {
    idleDriftAmplitude: 0.18,
    idleDriftSpeed: 0.35,
    pulseDuration: 1.25
  },
  node: {
    breathScaleAmplitude: 0.038,
    breathEmissiveAmplitude: 0.12,
    highlightScaleBoost: 0.13,
    retryHoldSeconds: 2.2,
    errorHoldSeconds: 1.9,
    defaultHoldSeconds: 1.25
  },
  edge: {
    decay: 0.92,
    shimmerAmplitude: 0.08
  },
  emit: {
    trailDelay: 0.06
  }
};
const END_DRAIN_SECONDS = Math.max(
  1.4,
  ...Object.values(EFFECT_PRESETS)
      .flatMap((preset) => Object.values(preset))
      .flatMap((value) => {
        if (typeof value === "number") {
          return [value];
        }
        if (value && typeof value === "object" && typeof value.duration === "number") {
          return [value.duration];
        }
        return [];
      })
);
const CHROME_HIDE_DELAY_MS = 2000;

const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setSize(mount.clientWidth, mount.clientHeight);
mount.appendChild(renderer.domElement);
renderer.domElement.style.touchAction = "none";

const scene = new THREE.Scene();
scene.background = new THREE.Color(0x07101f);

const camera = new THREE.PerspectiveCamera(40, mount.clientWidth / mount.clientHeight, 0.1, 100);
camera.position.set(0, 0, 24);
camera.lookAt(0, 0, 0);

const ambientLight = new THREE.AmbientLight(0xffffff, 1.2);
const directionalLight = new THREE.DirectionalLight(0x8ab4ff, 1.05);
directionalLight.position.set(4, 8, 10);
scene.add(ambientLight, directionalLight);

const raycaster = new THREE.Raycaster();
const pointer = new THREE.Vector2(2, 2);

const nodeMeshes = new Map();
const nodePositions = new Map();
const automaticNodePositions = new Map();
const nodeLabelSprites = new Map();
const nodeValueSprites = new Map();
const nodeIconSprites = new Map();
const nodePressureRings = new Map();
const edgeLines = new Map();
const particles = new Map();
const pulseEffects = [];
const burstEffects = [];
const retryLoops = [];
const backgroundFlashes = [];
const eventIndexByKey = new Set();
const itemAnchors = new Map();
const highlightExpirations = new Map();
const stepMetadataByName = new Map();
const runtimeStepState = new Map();
const directEventSteps = new Set();
const displayStepAliases = new Map();
const rawStepMetadataByName = new Map();
const supportFlowSampleCounts = new Map();
const STEP_LABELS = {
  PaymentProvider: "Payment Provider",
  Database: "Database"
};
const STEP_ROLE_LABELS = {
  broker: "Kafka Broker",
  "external-provider": "Payment Provider",
  store: "Database",
  "object-ingest": "Object Ingest",
  "object-publish": "Object Publish",
  "query-connector": "JPA Query"
};
const AWAIT_LIFECYCLE_EVENTS = new Set([
  "await_interaction_dispatched",
  "await_unit_dispatch_complete",
  "await_execution_waiting",
  "await_unit_item_completed",
  "await_unit_completed",
  "await_resume_released",
  "await_unit_terminal"
]);
const AWAIT_ATTR = {
  unitId: "tpf.await.unit_id",
  executionId: "tpf.await.execution_id",
  stepId: "tpf.await.step_id",
  stepIndex: "tpf.await.step_index",
  status: "tpf.await.status",
  interactionId: "tpf.await.interaction_id",
  expectedItemCount: "tpf.await.expected_item_count",
  completedItemCount: "tpf.await.completed_item_count"
};
let fallbackAwaitDisplayStep = null;
let activeAnimationPolicy = emptyAnimationPolicy();
let replayHasAwaitLifecycleEvents = false;
let replayBackpressureCapacity = null;

let replayDocument = normalizeReplayDocument(validateReplayDocument({ topology: { steps: [], transitions: [] }, events: [], durationMs: 0, pipeline: "loading" }));
let replayDurationSeconds = 0.1;
let nextEventCursor = 0;
let currentTimeSeconds = 0;
let isPlaying = false;
let isLoadingReplay = false;
let isFinishingEffects = false;
let playbackSpeed = Number(speedInputs.find((input) => input.checked)?.value ?? 0.5);
let previousAnimationFrame = performance.now();
let hoveredStepName = null;
let nodeValueUpdateTick = 0;
let activeReplaySourceKey = DEFAULT_REPLAY_SOURCE_KEY;
let stagedReplaySourceKey = activeReplaySourceKey;
let activeLayoutStorageKey = null;
let chromeHideTimeout = null;
let fatalRenderErrorLatched = false;
let openModal = null;
let modalReturnFocusElement = null;
let isScrubbingTimeline = false;
let dragState = null;
let suppressNextCanvasClick = false;
let prefersTapChrome = window.matchMedia("(hover: none), (pointer: coarse)").matches;
let loadProgressHideTimeout = null;
let completionPromptTimeout = null;
let completionPromptShownForPlayback = false;

function resolveReplayDocsHref() {
  const replayDocsPath = "/operate/observability/replay";
  const currentPath = window.location.pathname;
  if (currentPath.includes("/replay-viewer/") || currentPath.includes("/replay-viewer-app/")) {
    return `${window.location.origin}${replayDocsPath}`;
  }
  return `https://pipelineframework.org${replayDocsPath}`;
}

function reloadIfViewerShellRouteChanged() {
  const currentPath = window.location.pathname;
  if (!currentPath.includes("/replay-viewer/") && !currentPath.includes("/replay-viewer-app/")) {
    window.location.replace(window.location.href);
    return true;
  }
  return false;
}

function clearReplayFileSelection() {
  if (replayFileInput) {
    replayFileInput.value = "";
  }
}

function syncStagedReplaySourceToActive() {
  stagedReplaySourceKey = activeReplaySourceKey;
  datasetSelect.value = stagedReplaySourceKey;
  setCustomReplayVisibility(stagedReplaySourceKey === "custom");
  clearReplayFileSelection();
  updateSourceApplyButton();
}

function setCustomReplayVisibility(visible) {
  customReplayInputWrap.hidden = !visible;
  customReplayInputWrap.setAttribute("aria-hidden", visible ? "false" : "true");
}

function updateSourceApplyButton() {
  if (!sourceApplyButton) {
    return;
  }
  const nextSourceKey = datasetSelect.value;
  const hasPendingCustomReplay = nextSourceKey === "custom" && Boolean(replayFileInput.files?.length);
  sourceApplyButton.disabled = isLoadingReplay
    || nextSourceKey === EMPTY_REPLAY_SOURCE_KEY
    || (nextSourceKey === "custom" && !hasPendingCustomReplay);
}

function isAnyModalOpen() {
  return openModal !== null;
}

function hashString(value) {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function replayLayoutStorageKey(document) {
  const topology = document?.topology ?? {};
  const fingerprint = {
    pipeline: document?.pipeline ?? "unknown",
    steps: (topology.steps ?? []).map((step) => ({
      step: step.step,
      role: step.renderRole,
      actorKind: step.actorKind,
      parentStep: step.parentStep,
      sideEffect: step.sideEffect === true,
      index: step.index
    })),
    transitions: (topology.transitions ?? []).map((transition) => ({
      from: transition.from,
      to: transition.to,
      relationKind: transition.relationKind
    }))
  };
  return `${LAYOUT_STORAGE_PREFIX}:${hashString(JSON.stringify(fingerprint))}`;
}

function readSavedLayoutPositions() {
  if (!activeLayoutStorageKey) {
    return new Map();
  }
  try {
    const raw = localStorage.getItem(activeLayoutStorageKey);
    if (!raw) {
      return new Map();
    }
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") {
      return new Map();
    }
    return new Map(Object.entries(parsed)
      .filter(([, position]) => Number.isFinite(position?.x) && Number.isFinite(position?.y))
      .map(([stepName, position]) => [stepName, new THREE.Vector3(position.x, position.y, 0)]));
  } catch (error) {
    reportViewerIssue(`saved layout ignored (${error.message})`);
    return new Map();
  }
}

function saveCurrentLayoutPositions() {
  if (!activeLayoutStorageKey || nodePositions.size === 0) {
    return;
  }
  const positions = {};
  for (const [stepName, position] of nodePositions.entries()) {
    positions[stepName] = {
      x: Number(position.x.toFixed(4)),
      y: Number(position.y.toFixed(4))
    };
  }
  try {
    localStorage.setItem(activeLayoutStorageKey, JSON.stringify(positions));
  } catch (error) {
    reportViewerIssue(`layout could not be saved (${error.message})`);
  }
}

function clearSavedLayoutPositions() {
  if (!activeLayoutStorageKey) {
    return;
  }
  try {
    localStorage.removeItem(activeLayoutStorageKey);
  } catch (error) {
    reportViewerIssue(`saved layout could not be cleared (${error.message})`);
  }
}

function validateReplayDocument(document) {
  if (Array.isArray(document)) {
    throw new Error("The replay viewer expects a replay document object, not an array.");
  }
  if (!document || typeof document !== "object") {
    throw new Error("Replay JSON must be an object.");
  }
  if (!document.topology || !Array.isArray(document.topology.steps) || !Array.isArray(document.events)) {
    throw new Error("Replay JSON must contain topology.steps and events arrays.");
  }
  return document;
}

function emptyAnimationPolicy() {
  return {
    awaitRequestByTargetStep: new Map(),
    awaitRequestByAwaitStep: new Map(),
    awaitCompletionByResumeStep: new Map(),
    awaitCompletionByAwaitStep: new Map(),
    outputResumeByTargetStep: new Map(),
    storeWriteByRawStep: new Map(),
    connectorIngestByTargetStep: new Map(),
    connectorPublishBySourceStep: new Map(),
    queryConnectorByTargetStep: new Map()
  };
}

function normalizeReplayDocument(document) {
  const events = Array.isArray(document.events)
    ? [...document.events].sort((left, right) => {
        const leftTime = playbackTimeForEvent(left);
        const rightTime = playbackTimeForEvent(right);
        if (leftTime !== rightTime) {
          return leftTime - rightTime;
        }
        return (left.sequence ?? 0) - (right.sequence ?? 0);
      })
    : [];
  const connectorTopology = augmentTopologyWithConnectorNodes(document.topology, document.connectors);
  const topology = augmentTopologyWithDisplayNodes(
    connectorTopology,
    events.some((event) => event.event === "reject")
  );
  const { topology: displayTopology, aliases } = normalizeTopologyForDisplay(topology, events);
  return Object.assign({}, document, {
    rawTopology: topology,
    topology: displayTopology,
    displayAliases: aliases,
    fallbackAwaitDisplayStep: defaultAwaitStepNameForTopology(displayTopology),
    hasAwaitLifecycleEvents: events.some(isAwaitLifecycleEvent),
    events
  });
}

function defaultAwaitStepNameForTopology(topology) {
  if (!topology?.steps) {
    return null;
  }
  return topology.steps.find((step) => resolveDisplayRole(step) === "await")?.step ?? null;
}

function normalizeTopologyForDisplay(topology, events = []) {
  if (!topology || !Array.isArray(topology.steps) || !Array.isArray(topology.transitions)) {
    return { topology, aliases: {} };
  }
  const aliases = {};
  const stepsByName = new Map(topology.steps.map((step) => [step.step, step]));
  const primaryTransitions = topology.transitions.filter((transition) =>
    (transition?.relationKind ?? "primary") === "primary");
  const primaryInboundCounts = new Map();
  const primaryOutboundCounts = new Map();
  for (const transition of primaryTransitions) {
    primaryOutboundCounts.set(transition.from, (primaryOutboundCounts.get(transition.from) ?? 0) + 1);
    primaryInboundCounts.set(transition.to, (primaryInboundCounts.get(transition.to) ?? 0) + 1);
  }
  const directEventSteps = new Set(events.map((event) => event?.step).filter(Boolean));
  const eventBearingPrimarySteps = topology.steps.filter((step) =>
    resolveDisplayRole(step) === "primary" && !step.sideEffect && directEventSteps.has(step.step)
  );
  const visibleAwaitSteps = topology.steps.filter((step) =>
    resolveDisplayRole(step) === "await" && !isInternalAwaitClientStep(step.step)
  );
  const defaultAwaitStep = visibleAwaitSteps[0]?.step ?? null;
  const hasConcretePersistenceNodes = topology.steps.some((step) => resolveDisplayRole(step) === "persistence-plugin");
  const hiddenSteps = new Set();
  for (const step of topology.steps) {
    if (hasConcretePersistenceNodes && resolveDisplayRole(step) === "store") {
      hiddenSteps.add(step.step);
      continue;
    }
    const collapseTarget = collapseDisplayProcessStep(
      step,
      stepsByName,
      primaryTransitions,
      primaryInboundCounts,
      primaryOutboundCounts,
      directEventSteps
    );
    if (collapseTarget) {
      aliases[collapseTarget] = step.step;
      hiddenSteps.add(collapseTarget);
    }
    if (isInternalAwaitClientStep(step.step) && defaultAwaitStep) {
      aliases[step.step] = defaultAwaitStep;
      hiddenSteps.add(step.step);
    }
    if (
      eventBearingPrimarySteps.length > 0
      && resolveDisplayRole(step) === "primary"
      && !step.sideEffect
      && !directEventSteps.has(step.step)
      && (primaryInboundCounts.get(step.step) ?? 0) < 2
      && (primaryOutboundCounts.get(step.step) ?? 0) < 2
      && isGeneratedBaseTopologyStep(step)
    ) {
      hiddenSteps.add(step.step);
    }
  }
  for (const step of topology.steps) {
    if (step.sideEffect && hiddenSteps.has(step.parentStep) && !directEventSteps.has(step.step)) {
      hiddenSteps.add(step.step);
    }
  }
  if (hiddenSteps.size === 0) {
    return { topology, aliases };
  }
  const remappedSteps = topology.steps
    .filter((step) => !hiddenSteps.has(step.step))
    .map((step) => Object.assign({}, step, {
      parentStep: aliasStepName(step.parentStep, aliases)
    }));
  const seenTransitions = new Set();
  const remappedTransitions = [];
  for (const transition of topology.transitions) {
    const remappedFrom = aliasStepName(transition.from, aliases);
    const remappedTo = aliasStepName(transition.to, aliases);
    if (!remappedFrom || !remappedTo || hiddenSteps.has(remappedFrom) || hiddenSteps.has(remappedTo) || remappedFrom === remappedTo) {
      continue;
    }
    const dedupeKey = `${remappedFrom}|${remappedTo}|${transition?.relationKind ?? "primary"}|${transition?.cardinality ?? ""}`;
    if (seenTransitions.has(dedupeKey)) {
      continue;
    }
    seenTransitions.add(dedupeKey);
    remappedTransitions.push(Object.assign({}, transition, {
      from: remappedFrom,
      to: remappedTo
    }));
  }
  return {
    topology: Object.assign({}, topology, {
      steps: remappedSteps,
      transitions: remappedTransitions
    }),
    aliases
  };
}

function collapseDisplayProcessStep(
  step,
  stepsByName,
  primaryTransitions,
  primaryInboundCounts,
  primaryOutboundCounts,
  directEventSteps
) {
  if (!isGeneratedBaseTopologyStep(step) || directEventSteps.has(step.step)) {
    return null;
  }
  const inboundCount = primaryInboundCounts.get(step.step) ?? 0;
  const outboundCount = primaryOutboundCounts.get(step.step) ?? 0;
  if (inboundCount < 2 && outboundCount < 2) {
    return null;
  }
  const outgoingPrimary = primaryTransitions.filter((transition) => transition.from === step.step);
  if (outgoingPrimary.length !== 1) {
    return null;
  }
  const targetStepName = outgoingPrimary[0]?.to;
  const targetStep = stepsByName.get(targetStepName);
  if (!targetStep || targetStep.sideEffect || resolveDisplayRole(targetStep) !== "primary" || isGeneratedBaseTopologyStep(targetStep)) {
    return null;
  }
  if (!directEventSteps.has(targetStep.step)) {
    return null;
  }
  return (primaryInboundCounts.get(targetStep.step) ?? 0) === 1
    ? targetStep.step
    : null;
}

function aliasStepName(stepName, aliases) {
  if (!stepName) {
    return stepName;
  }
  return aliases[stepName] ?? stepName;
}

function uniqueStepName(baseName, existingStepNames) {
  let candidate = baseName;
  let suffix = 2;
  while (existingStepNames.has(candidate)) {
    candidate = `${baseName}${suffix}`;
    suffix += 1;
  }
  existingStepNames.add(candidate);
  return candidate;
}

function isGeneratedBaseTopologyStep(step) {
  return typeof step?.runtimeStepClass === "string"
    && /^runtime::.+\$BaseStep$/.test(step.runtimeStepClass);
}

function isInternalAwaitClientStep(stepName) {
  return /AwaitClientStep$/.test(stepName ?? "");
}

function aliasStepNameForDisplay(stepName) {
  if (!stepName) {
    return stepName;
  }
  if (isInternalAwaitClientStep(stepName) && fallbackAwaitDisplayStep) {
    return fallbackAwaitDisplayStep;
  }
  return displayStepAliases.get(stepName) ?? stepName;
}

function mapEventForDisplay(event) {
  if (!event) {
    return event;
  }
  return Object.assign({}, event, {
    step: aliasStepNameForDisplay(event.step),
    from: aliasStepNameForDisplay(event.from),
    to: aliasStepNameForDisplay(event.to)
  });
}

function isAwaitResumableError(event) {
  return event?.event === "error"
    && event?.errorType === "org.pipelineframework.awaitable.AwaitSuspendedException";
}

function isAwaitSuspensionEvent(event) {
  return isInternalAwaitClientStep(event?.step) && isAwaitResumableError(event);
}

function sameReplayTarget(left, right) {
  return (left?.step ?? "") === (right?.step ?? "")
    && (left?.from ?? "") === (right?.from ?? "")
    && (left?.to ?? "") === (right?.to ?? "")
    && (left?.itemId ?? "") === (right?.itemId ?? "");
}

function isAwaitSuspensionStartEvent(event, nextEvent) {
  if (event?.event !== "start" || !isAwaitResumableError(nextEvent) || !sameReplayTarget(event, nextEvent)) {
    return false;
  }
  return Math.abs(playbackTimeForEvent(nextEvent) - playbackTimeForEvent(event)) < 0.05;
}

function firstDisplayedStepByRole(renderRole) {
  for (const step of stepMetadataByName.values()) {
    if (step?.renderRole === renderRole && nodePositions.has(step.step)) {
      return step.step;
    }
  }
  return null;
}

function awaitDisplayStepForEvent(event) {
  const eventStep = resolveStepDefinition(event?.step);
  if (eventStep?.renderRole === "await" && nodePositions.has(event.step)) {
    return event.step;
  }
  return fallbackAwaitDisplayStep || firstDisplayedStepByRole("await") || event?.step || event?.from || event?.to;
}

function nextAwaitResumeTimeAfter(timeSeconds) {
  for (let index = nextEventCursor; index < replayDocument.events.length; index += 1) {
    const rawCandidate = replayDocument.events[index];
    if (replayHasAwaitLifecycleEvents && rawCandidate?.event === "await_resume_released") {
      const candidateTime = playbackTimeForEvent(rawCandidate);
      if (candidateTime > timeSeconds) {
        return candidateTime;
      }
    }
    const candidate = mapEventForDisplay(rawCandidate);
    const completionFlow = candidate?.step ? activeAnimationPolicy.awaitCompletionByResumeStep.get(candidate.step) : null;
    if (rawCandidate.event === "start" && completionFlow && candidate.from === completionFlow.awaitStep) {
      const candidateTime = playbackTimeForEvent(rawCandidate);
      if (candidateTime > timeSeconds) {
        return candidateTime;
      }
    }
  }
  return null;
}

function resolveRawStepDefinition(stepName) {
  return rawStepMetadataByName.get(stepName) ?? resolveStepDefinition(stepName);
}

function isPersistenceSideEffectStep(stepOrName) {
  const step = typeof stepOrName === "string"
    ? resolveRawStepDefinition(stepOrName)
    : stepOrName;
  return step?.sideEffect === true && step?.pluginKind === "persistence";
}

function playbackTimeForEvent(event) {
  if (!event) {
    return 0;
  }
  return event.event === "success" || event.event === "error"
    ? (event.endTime ?? event.startTime)
    : event.startTime;
}

function isAwaitLifecycleEvent(event) {
  return AWAIT_LIFECYCLE_EVENTS.has(event?.event);
}

function awaitLifecycleAttribute(event, key) {
  return event?.attributes?.[key] ?? null;
}

function awaitLifecycleNumber(event, key) {
  const value = awaitLifecycleAttribute(event, key);
  if (value == null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function awaitStepForLifecycleEvent(rawEvent, displayEvent = mapEventForDisplay(rawEvent)) {
  const stepId = awaitLifecycleAttribute(rawEvent, AWAIT_ATTR.stepId);
  const aliasedStep = aliasStepNameForDisplay(stepId);
  if (stepHasRenderRole(aliasedStep, "await")) {
    return aliasedStep;
  }
  if (stepHasRenderRole(displayEvent?.step, "await")) {
    return displayEvent.step;
  }
  return awaitDisplayStepForEvent(displayEvent);
}

function computeReplayDurationSeconds(document) {
  const declaredDurationSeconds = Math.max(0.1, (document.durationMs || 0) / 1000);
  const maxEventTimeSeconds = Array.isArray(document.events) && document.events.length > 0
    ? document.events.reduce((latest, event) => Math.max(latest, playbackTimeForEvent(event)), 0)
    : 0;
  return Math.max(declaredDurationSeconds, maxEventTimeSeconds);
}

function augmentTopologyWithConnectorNodes(topology, connectors = []) {
  if (!topology || !Array.isArray(topology.steps) || !Array.isArray(topology.transitions)) {
    return topology;
  }
  const steps = topology.steps.map((step) => Object.assign({}, step));
  const transitions = [...topology.transitions];
  const stepNames = new Set(steps.map((step) => step.step));
  const transitionIds = new Set(transitions.map((transition) => transition.id));
  const transitionEndpoints = new Set(
    transitions
      .filter((transition) => transition.from && transition.to)
      .map((transition) => `${transition.from}->${transition.to}`)
  );
  const runtimeStepClasses = new Set(steps.map((step) => step.runtimeStepClass).filter(Boolean));
  const businessSteps = steps.filter((step) => !step.sideEffect && !connectorRenderRole(step));
  const firstBusinessStep = businessSteps[0]?.step ?? null;
  const lastBusinessStep = [...businessSteps].reverse().find((step) => resolveDisplayRole(step) === "primary")?.step
    ?? businessSteps[businessSteps.length - 1]?.step
    ?? null;

  const ensureConnectorTransition = (connectorStepName, role, targetStep, connector = {}) => {
    const relationKind = connector.relationKind ?? role;
    const from = role === "object-ingest" || role === "query-connector" ? connectorStepName : targetStep;
    const to = role === "object-ingest" || role === "query-connector" ? targetStep : connectorStepName;
    const transitionId = connector.transitionId ?? `${from}->${to}`;
    const transitionEndpoint = `${from}->${to}`;
    if (transitionIds.has(transitionId) || transitionEndpoints.has(transitionEndpoint)) {
      return;
    }
    transitions.push({
      id: transitionId,
      fromRuntimeStepClass: steps.find((step) => step.step === from)?.runtimeStepClass,
      toRuntimeStepClass: steps.find((step) => step.step === to)?.runtimeStepClass,
      from,
      to,
      fromService: steps.find((step) => step.step === from)?.service,
      toService: steps.find((step) => step.step === to)?.service,
      cardinality: connector.cardinality ?? "one-to-one",
      relationKind
    });
    transitionIds.add(transitionId);
    transitionEndpoints.add(transitionEndpoint);
  };

  for (const step of steps) {
    const role = connectorRenderRole(step);
    if (!role) {
      continue;
    }
    const targetStep = step.targetStep
      ?? step.attachesTo
      ?? (role === "object-ingest" || role === "query-connector" ? firstBusinessStep : lastBusinessStep);
    if (!targetStep) {
      continue;
    }
    step.sideEffect = true;
    step.parentStep = targetStep;
    step.pluginKind = step.pluginKind ?? null;
    step.connectorKind = step.connectorKind ?? role;
    step.connectorProvider = step.connectorProvider ?? step.provider ?? null;
    step.renderRole = role;
    step.actorKind = step.actorKind ?? connectorActorKind(role, step.connectorProvider);
    ensureConnectorTransition(step.step, role, targetStep, step);
  }

  for (const connector of Array.isArray(connectors) ? connectors : []) {
    const role = connectorRenderRole(connector);
    if (!role) {
      continue;
    }
    const connectorStepName = connector.step
      ?? connector.name
      ?? (role === "object-ingest" ? "ObjectIngest" : role === "object-publish" ? "ObjectPublish" : "JpaQuery");
    const targetStep = connector.targetStep
      ?? connector.attachesTo
      ?? (role === "object-ingest" ? firstBusinessStep : lastBusinessStep);
    if (!targetStep) {
      continue;
    }
    let connectorRuntimeStepClass = steps.find((step) => step.step === connectorStepName)?.runtimeStepClass;
    if (!stepNames.has(connectorStepName)) {
      connectorRuntimeStepClass = uniqueSyntheticRuntimeClass(
        connector.runtimeStepClass ?? `runtime::${connectorStepName}`,
        "ConnectorDisplayStep",
        runtimeStepClasses);
      steps.push({
        runtimeStepClass: connectorRuntimeStepClass,
        step: connectorStepName,
        service: connector.service ?? connector.label ?? connectorStepName,
        cardinality: connector.cardinality ?? "one-to-one",
        index: Number.isFinite(connector.index) ? connector.index : connectorDisplayIndex(role, steps),
        sideEffect: true,
        parentStep: targetStep,
        pluginKind: connector.pluginKind ?? null,
        connectorKind: connector.kind ?? role,
        connectorProvider: connector.provider ?? null,
        connectorTarget: connector.target ?? connector.source ?? null,
        renderRole: role,
        actorKind: connector.actorKind ?? connectorActorKind(role, connector.provider)
      });
      stepNames.add(connectorStepName);
      runtimeStepClasses.add(connectorRuntimeStepClass);
    }
    ensureConnectorTransition(connectorStepName, role, targetStep, connector);
  }

  return Object.assign({}, topology, {
    steps,
    transitions
  });
}

function connectorRenderRole(connector) {
  const kind = String(connector?.kind ?? connector?.connectorKind ?? "").toLowerCase();
  const role = String(connector?.renderRole ?? "").toLowerCase();
  const runtimeStepClass = String(connector?.runtimeStepClass ?? "").toLowerCase();
  const service = String(connector?.service ?? "").toLowerCase();
  const step = String(connector?.step ?? connector?.name ?? "").toLowerCase();
  if (role === "object-ingest"
      || kind === "object-ingest"
      || kind === "object-ingest-connector"
      || runtimeStepClass === "runtime::objectingest"
      || service === "objectingestconnector"
      || step === "objectingest") {
    return "object-ingest";
  }
  if (role === "object-publish"
      || kind === "object-publish"
      || kind === "object-publish-connector"
      || runtimeStepClass === "runtime::objectpublish"
      || service === "objectpublishconnector"
      || step === "objectpublish") {
    return "object-publish";
  }
  if (role === "query-connector" || kind === "jpa-query" || kind === "jpa-query-connector" || kind === "query") {
    return "query-connector";
  }
  return null;
}

function connectorDisplayIndex(role, steps) {
  if (role === "object-ingest" || role === "query-connector") {
    return -10 - steps.length;
  }
  return 1000 + steps.length;
}

function connectorActorKind(role, provider) {
  if (role === "query-connector") {
    return "database";
  }
  const normalizedProvider = String(provider ?? "").toLowerCase();
  if (normalizedProvider.includes("s3")) {
    return "object-storage";
  }
  if (normalizedProvider.includes("file")) {
    return "filesystem";
  }
  return "object";
}

function augmentTopologyWithDisplayNodes(topology, includeRejectNodes = true) {
  if (!topology || !Array.isArray(topology.steps) || !Array.isArray(topology.transitions)) {
    return topology;
  }
  if (!includeRejectNodes) {
    return topology;
  }
  const steps = [...topology.steps];
  const transitions = [...topology.transitions];
  const stepNames = new Set(steps.map((step) => step.step));
  const transitionIds = new Set(transitions.map((transition) => transition.id));
  const runtimeStepClasses = new Set(steps.map((step) => step.runtimeStepClass).filter(Boolean));

  for (const step of topology.steps) {
    if (step.sideEffect) {
      continue;
    }
    const rejectStepName = `Rejects ${step.step}`;
    let rejectRuntimeStepClass = steps.find((candidate) => candidate.step === rejectStepName)?.runtimeStepClass;
    if (!stepNames.has(rejectStepName)) {
      rejectRuntimeStepClass = uniqueSyntheticRuntimeClass(step.runtimeStepClass, "RejectQueueDisplayStep", runtimeStepClasses);
      steps.push({
        runtimeStepClass: rejectRuntimeStepClass,
        step: rejectStepName,
        service: "RejectQueue",
        cardinality: "one-to-one",
        index: step.index,
        sideEffect: true,
        parentStep: step.step,
        pluginKind: "reject"
      });
      stepNames.add(rejectStepName);
      runtimeStepClasses.add(rejectRuntimeStepClass);
    }
    const transitionId = `${step.step}->${rejectStepName}`;
    if (!transitionIds.has(transitionId)) {
      transitions.push({
        id: transitionId,
        fromRuntimeStepClass: step.runtimeStepClass,
        toRuntimeStepClass: rejectRuntimeStepClass,
        from: step.step,
        to: rejectStepName,
        fromService: step.service,
        toService: "RejectQueue",
        cardinality: step.cardinality
      });
      transitionIds.add(transitionId);
    }
  }

  return Object.assign({}, topology, {
    steps,
    transitions
  });
}

function uniqueSyntheticRuntimeClass(baseRuntimeClass, suffix, existingRuntimeClasses) {
  const normalizedBase = baseRuntimeClass || "org.pipelineframework.synthetic.Replay";
  let candidate = `${normalizedBase}$${suffix}`;
  let counter = 2;
  while (existingRuntimeClasses.has(candidate)) {
    candidate = `${normalizedBase}$${suffix}${counter}`;
    counter += 1;
  }
  return candidate;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function setSourceModalStatus(text) {
  if (sourceModalStatus) {
    sourceModalStatus.textContent = text;
  }
}

function setLoadProgress(visible, ratio = 0, text = "Loading replay...") {
  isLoadingReplay = visible;
  if (loadProgressHideTimeout) {
    window.clearTimeout(loadProgressHideTimeout);
    loadProgressHideTimeout = null;
  }
  loadProgress.hidden = !visible;
  loadProgressFill.style.width = `${Math.round(clamp(ratio, 0, 1) * 100)}%`;
  loadProgressText.textContent = text;
  setSourceModalStatus(text);
  if (visible) {
    revealPlayerChrome(true);
  } else if (!isAnyModalOpen()) {
    scheduleChromeHide();
  }
  updateSourceApplyButton();
}

function finishLoadProgress(text = "Replay loaded") {
  if (loadProgressHideTimeout) {
    window.clearTimeout(loadProgressHideTimeout);
  }
  isLoadingReplay = false;
  loadProgress.hidden = false;
  loadProgressFill.style.width = "100%";
  loadProgressText.textContent = text;
  setSourceModalStatus(text);
  loadProgressHideTimeout = window.setTimeout(() => {
    loadProgress.hidden = true;
    loadProgressHideTimeout = null;
    if (!isAnyModalOpen()) {
      scheduleChromeHide();
    }
  }, 1100);
  updateSourceApplyButton();
}

function nextAnimationFrame() {
  return new Promise((resolve) => requestAnimationFrame(resolve));
}

async function readResponseTextWithProgress(response, label) {
  const totalBytes = Number(response.headers.get("Content-Length"));
  if (!response.body || !Number.isFinite(totalBytes) || totalBytes <= 0) {
    setLoadProgress(true, 0.45, `Downloading ${label}...`);
    await nextAnimationFrame();
    return response.text();
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const chunks = [];
  let loadedBytes = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    chunks.push(decoder.decode(value, { stream: true }));
    loadedBytes += value.byteLength;
    const downloadRatio = clamp(loadedBytes / totalBytes, 0, 1);
    setLoadProgress(true, 0.15 + downloadRatio * 0.65, `Downloading ${label}... ${Math.round(downloadRatio * 100)}%`);
  }
  chunks.push(decoder.decode());
  return chunks.join("");
}

function hexToRgbTriplet(hexColor) {
  const value = hexColor.replace("#", "");
  const normalized = value.length === 3
    ? value.split("").map((part) => part + part).join("")
    : value;
  const intValue = Number.parseInt(normalized, 16);
  const red = (intValue >> 16) & 255;
  const green = (intValue >> 8) & 255;
  const blue = intValue & 255;
  return `${red}, ${green}, ${blue}`;
}

function setViewportBackground(accentStrength, accentColor, driftPhase) {
  if (!viewport) {
    return;
  }
  const strength = clamp(accentStrength, 0, 1);
  viewport.style.setProperty("--bg-accent-strength", strength.toFixed(3));
  viewport.style.setProperty("--bg-accent-rgb", hexToRgbTriplet(accentColor));
  viewport.style.setProperty("--bg-drift-x", `${(Math.sin(driftPhase) * 16).toFixed(1)}px`);
  viewport.style.setProperty("--bg-drift-y", `${(Math.cos(driftPhase * 0.7) * 14).toFixed(1)}px`);
}

function resolveStepDefinition(stepName) {
  return stepMetadataByName.get(stepName) ?? null;
}

function stepHasRenderRole(stepName, renderRole) {
  return resolveStepDefinition(stepName)?.renderRole === renderRole;
}

function resolveDisplayRole(step) {
  if (!step) {
    return "primary";
  }
  const connectorRole = connectorRenderRole(step);
  if (connectorRole) {
    return connectorRole;
  }
  if (step.renderRole) {
    return step.renderRole;
  }
  if (step.pluginKind === "persistence") {
    return "persistence-plugin";
  }
  return step.sideEffect ? "plugin" : "primary";
}

function resolveDisplayIconKind(step) {
  const role = resolveDisplayRole(step);
  if (step?.pluginKind) {
    if (step.pluginKind === "persistence" && step.actorKind === "database") {
      return "store";
    }
    return step.pluginKind;
  }
  switch (role) {
    case "store":
      return "store";
    case "broker":
      return "broker";
    case "external-provider":
      return "provider";
    case "object-ingest":
      return "object-ingest";
    case "object-publish":
      return "object-publish";
    case "query-connector":
      return "query";
    default:
      return "plugin";
  }
}

function isNamedSupportActor(step) {
  const role = resolveDisplayRole(step);
  return role === "broker"
    || role === "external-provider"
    || role === "store"
    || role === "object-ingest"
    || role === "object-publish"
    || role === "query-connector";
}

function showsThroughputCounter(step) {
  if (!step) {
    return false;
  }
  if (!step.sideEffect) {
    return true;
  }
  if (step.pluginKind === "reject") {
    return false;
  }
  const role = resolveDisplayRole(step);
  return role === "object-ingest"
    || role === "object-publish"
    || role === "query-connector";
}

function resolveDisplayStepName(event) {
  if (!event || (event.event !== "retry" && event.event !== "error")) {
    return aliasStepNameForDisplay(event?.step ?? null);
  }
  const step = resolveStepDefinition(aliasStepNameForDisplay(event.step));
  if (step?.sideEffect && step.parentStep) {
    return step.parentStep;
  }
  return aliasStepNameForDisplay(event.step);
}

function resolveDisplayTargets(event) {
  const primaryStep = resolveDisplayStepName(event);
  return {
    primaryStep,
    secondaryStep: primaryStep && primaryStep !== event.step ? event.step : null
  };
}

function indexStepsByRole(topology) {
  const stepsByRole = new Map();
  for (const step of topology?.steps ?? []) {
    const role = resolveDisplayRole(step);
    if (!stepsByRole.has(role)) {
      stepsByRole.set(role, []);
    }
    stepsByRole.get(role).push(step);
  }
  return stepsByRole;
}

function indexTransitionsByRelation(topology) {
  const transitionsByRelation = new Map();
  for (const transition of topology?.transitions ?? []) {
    const relationKind = transition?.relationKind ?? "primary";
    if (!transitionsByRelation.has(relationKind)) {
      transitionsByRelation.set(relationKind, []);
    }
    transitionsByRelation.get(relationKind).push(transition);
  }
  return transitionsByRelation;
}

function findTransitionByFrom(transitions, fromStep) {
  return transitions.find((transition) => transition.from === fromStep) ?? null;
}

function findTransitionByTo(transitions, toStep) {
  return transitions.find((transition) => transition.to === toStep) ?? null;
}

function buildSupportAnimationPolicy(displayTopology, rawTopology = displayTopology) {
  const policy = emptyAnimationPolicy();
  const displayTransitions = Array.isArray(displayTopology?.transitions) ? displayTopology.transitions : [];
  const rawSteps = Array.isArray(rawTopology?.steps) ? rawTopology.steps : [];
  const stepsByRole = indexStepsByRole(displayTopology);
  const transitionsByRelation = indexTransitionsByRelation(displayTopology);
  const awaitRequestTransitions = transitionsByRelation.get("await-request") ?? [];
  const awaitCompletionTransitions = transitionsByRelation.get("await-completion") ?? [];
  const primaryTransitions = transitionsByRelation.get("primary") ?? [];
  const storeTransitions = transitionsByRelation.get("store") ?? [];
  const objectIngestTransitions = transitionsByRelation.get("object-ingest") ?? [];
  const objectPublishTransitions = transitionsByRelation.get("object-publish") ?? [];
  const queryTransitions = [
    ...(transitionsByRelation.get("query-connector") ?? []),
    ...(transitionsByRelation.get("query") ?? [])
  ];

  for (const awaitStep of stepsByRole.get("await") ?? []) {
    const firstRequest = findTransitionByFrom(awaitRequestTransitions, awaitStep.step);
    const secondRequest = firstRequest ? findTransitionByFrom(awaitRequestTransitions, firstRequest.to) : null;
    if (firstRequest) {
      const requestFlow = {
        awaitStep: awaitStep.step,
        requestEdges: [firstRequest, secondRequest].filter(Boolean)
      };
      policy.awaitRequestByTargetStep.set(awaitStep.step, requestFlow);
      policy.awaitRequestByAwaitStep.set(awaitStep.step, requestFlow);
    }

    const resumeEdge = findTransitionByFrom(primaryTransitions, awaitStep.step);
    const downstreamEdge = resumeEdge ? findTransitionByFrom(primaryTransitions, resumeEdge.to) : null;
    const completionToAwait = findTransitionByTo(awaitCompletionTransitions, awaitStep.step);
    const completionFromExternal = completionToAwait
      ? findTransitionByTo(awaitCompletionTransitions, completionToAwait.from)
      : null;
    if (resumeEdge) {
      const completionFlow = {
        awaitStep: awaitStep.step,
        completionEdges: [completionFromExternal, completionToAwait].filter(Boolean),
        resumeEdge
      };
      policy.awaitCompletionByResumeStep.set(resumeEdge.to, completionFlow);
      policy.awaitCompletionByAwaitStep.set(awaitStep.step, completionFlow);
      if (downstreamEdge) {
        policy.outputResumeByTargetStep.set(downstreamEdge.to, downstreamEdge);
      }
    }
  }

  const displayStepsByName = new Map((displayTopology?.steps ?? []).map((step) => [step.step, step]));
  const rawTransitionsById = new Map((rawTopology?.transitions ?? []).map((transition) => [transition.id, transition]));
  for (const rawStep of rawSteps) {
    if (!isPersistenceSideEffectStep(rawStep)) {
      continue;
    }
    const displayTarget = aliasStepNameForDisplay(rawStep.step);
    const displaySource = aliasStepNameForDisplay(rawStep.parentStep);
    const displayTargetStep = displayTarget ? displayStepsByName.get(displayTarget) : null;
    const explicitStoreTransition = rawStep.parentStep
      ? rawTransitionsById.get(`${rawStep.parentStep}->${displayTarget}`)
      : null;
    const storeTransition = explicitStoreTransition
      ?? storeTransitions.find((transition) => transition.from === displaySource && transition.to === displayTarget)
      ?? displayTransitions.find((transition) => transition.from === displaySource && transition.to === displayTarget);
    policy.storeWriteByRawStep.set(rawStep.step, {
      fromStep: displaySource,
      toStep: displayTarget,
      aggregate: displayTarget !== rawStep.step
        || (displayTargetStep?.cardinality?.toLowerCase()?.includes("many") ?? false)
        || storeTransition?.cardinality?.toLowerCase()?.includes("many") === true
    });
  }

  for (const transition of objectIngestTransitions) {
    policy.connectorIngestByTargetStep.set(transition.to, transition);
  }
  for (const transition of objectPublishTransitions) {
    policy.connectorPublishBySourceStep.set(transition.from, transition);
  }
  for (const transition of queryTransitions) {
    policy.queryConnectorByTargetStep.set(transition.to, transition);
  }

  return policy;
}

function resolvePluginStepForDisplay(parentStep, pluginKind) {
  for (const step of stepMetadataByName.values()) {
    if (step.sideEffect && step.parentStep === parentStep && step.pluginKind === pluginKind) {
      return step.step;
    }
  }
  return null;
}

function disposeMaterial(material) {
  if (!material) {
    return;
  }
  if (Array.isArray(material)) {
    material.forEach(disposeMaterial);
    return;
  }
  if (material.map) {
    material.map.dispose();
  }
  material.dispose?.();
}

function disposeThreeObject(object) {
  if (!object) {
    return;
  }
  object.geometry?.dispose?.();
  disposeMaterial(object.material);
}

function removeAndDispose(object) {
  if (!object) {
    return;
  }
  scene.remove(object);
  disposeThreeObject(object);
}

function clearScene() {
  for (const { line } of edgeLines.values()) {
    removeAndDispose(line);
  }
  edgeLines.clear();
  stepMetadataByName.clear();
  for (const sprite of nodeLabelSprites.values()) {
    removeAndDispose(sprite);
  }
  nodeLabelSprites.clear();
  for (const sprite of nodeValueSprites.values()) {
    removeAndDispose(sprite);
  }
  nodeValueSprites.clear();
  for (const ring of nodePressureRings.values()) {
    removeAndDispose(ring.track);
    removeAndDispose(ring.arc);
  }
  nodePressureRings.clear();
  for (const sprite of nodeIconSprites.values()) {
    removeAndDispose(sprite);
  }
  nodeIconSprites.clear();
  for (const mesh of nodeMeshes.values()) {
    removeAndDispose(mesh);
  }
  nodeMeshes.clear();
  nodePositions.clear();
  automaticNodePositions.clear();
  for (const particle of particles.values()) {
    removeAndDispose(particle.mesh);
  }
  particles.clear();
  pulseEffects.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  burstEffects.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  retryLoops.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  backgroundFlashes.splice(0);
  runtimeStepState.clear();
  directEventSteps.clear();
  displayStepAliases.clear();
  rawStepMetadataByName.clear();
  activeAnimationPolicy = emptyAnimationPolicy();
  fallbackAwaitDisplayStep = null;
  replayHasAwaitLifecycleEvents = false;
  nodeValueUpdateTick += 1;
}

function clearReplayDynamics() {
  eventIndexByKey.clear();
  itemAnchors.clear();
  highlightExpirations.clear();
  supportFlowSampleCounts.clear();
  hoveredStepName = null;
  for (const particle of particles.values()) {
    removeAndDispose(particle.mesh);
  }
  particles.clear();
  pulseEffects.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  burstEffects.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  retryLoops.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  backgroundFlashes.splice(0);
  runtimeStepState.clear();
  nodeValueUpdateTick += 1;
  for (const edge of edgeLines.values()) {
    edge.intensity = 0;
    edge.line.material.opacity = edge.baseOpacity;
    edge.line.material.color.setHex(edge.baseColor);
  }
  isFinishingEffects = false;
}

function resetPlaybackState() {
  nextEventCursor = 0;
  clearReplayDynamics();
}

function cancelChromeHide() {
  if (chromeHideTimeout != null) {
    window.clearTimeout(chromeHideTimeout);
    chromeHideTimeout = null;
  }
}

function setPlayerChromeVisible(visible) {
  playerChrome.dataset.visible = visible ? "true" : "false";
  playerChrome.setAttribute("aria-hidden", visible ? "false" : "true");
  playerChrome.inert = !visible;
  if (!visible && playerChrome.contains(document.activeElement)) {
    document.activeElement.blur();
  }
}

function scheduleChromeHide(delayMs = CHROME_HIDE_DELAY_MS) {
  cancelChromeHide();
  if (isAnyModalOpen() || isScrubbingTimeline || isLoadingReplay) {
    setPlayerChromeVisible(true);
    return;
  }
  chromeHideTimeout = window.setTimeout(() => {
    if (isAnyModalOpen() || isScrubbingTimeline || isLoadingReplay) {
      setPlayerChromeVisible(true);
      return;
    }
    setPlayerChromeVisible(false);
  }, delayMs);
}

function revealPlayerChrome(sticky = false) {
  setPlayerChromeVisible(true);
  if (sticky || isAnyModalOpen() || isScrubbingTimeline || isLoadingReplay) {
    cancelChromeHide();
    return;
  }
  scheduleChromeHide();
}

function setSourceModalMode(mode = "load") {
  if (!sourceModalTitle || !sourceModalDescription) {
    return;
  }
  if (mode === "completion") {
    sourceModalTitle.textContent = "Replay finished";
    sourceModalDescription.textContent = "Load another built-in capture or try your own replay JSON.";
    setSourceModalStatus("Choose what to inspect next.");
    return;
  }
  sourceModalTitle.textContent = "Load replay";
  sourceModalDescription.textContent = "Choose a captured built-in replay or upload a replay JSON. The viewer plays real event timing, counters, and topology without synthetic flows.";
  if (isLoadingReplay) {
    setSourceModalStatus(loadProgressText.textContent);
  } else if (activeReplaySourceKey === EMPTY_REPLAY_SOURCE_KEY) {
    setSourceModalStatus("CSV Payments will load automatically on your first visit.");
  } else {
    setSourceModalStatus("Choose a different replay or upload a custom JSON file.");
  }
}

function openSourceModal(mode = "load") {
  setSourceModalMode(mode);
  openModalElement("source", sourceModal);
}

function cancelCompletionPrompt() {
  if (completionPromptTimeout != null) {
    window.clearTimeout(completionPromptTimeout);
    completionPromptTimeout = null;
  }
}

function resetCompletionPromptForPlayback() {
  cancelCompletionPrompt();
  completionPromptShownForPlayback = false;
}

function scheduleCompletionPrompt() {
  if (
    completionPromptShownForPlayback ||
    replayDocument.events.length === 0 ||
    isLoadingReplay ||
    isAnyModalOpen()
  ) {
    return;
  }
  completionPromptShownForPlayback = true;
  cancelCompletionPrompt();
  completionPromptTimeout = window.setTimeout(() => {
    completionPromptTimeout = null;
    if (
      isPlaying ||
      isLoadingReplay ||
      isAnyModalOpen() ||
      currentTimeSeconds < replayDurationSeconds
    ) {
      return;
    }
    openSourceModal("completion");
  }, COMPLETION_PROMPT_DELAY_MS);
}

function openModalElement(name, element) {
  if (name === "source") {
    syncStagedReplaySourceToActive();
  }
  if (!openModal) {
    modalReturnFocusElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  }
  openModal = name;
  if (appShell) {
    appShell.inert = true;
  }
  element.hidden = false;
  element.setAttribute("aria-hidden", "false");
  revealPlayerChrome(true);
  window.requestAnimationFrame(() => {
    if (openModal !== name || element.hidden) {
      return;
    }
    const focusTarget = name === "source"
      ? (datasetSelect ?? sourceCloseButton)
      : infoCloseButton;
    focusTarget?.focus({ preventScroll: true });
  });
}

function closeModalElement(name, element) {
  if (openModal !== name) {
    return;
  }
  if (name === "source") {
    syncStagedReplaySourceToActive();
  }
  openModal = null;
  element.hidden = true;
  element.setAttribute("aria-hidden", "true");
  if (appShell) {
    appShell.inert = false;
  }
  revealPlayerChrome(!prefersTapChrome);
  if (modalReturnFocusElement?.isConnected) {
    modalReturnFocusElement.focus({ preventScroll: true });
  }
  modalReturnFocusElement = null;
}

function clearElement(element) {
  while (element.firstChild) {
    element.removeChild(element.firstChild);
  }
}

function renderRunParameters(runParameters) {
  clearElement(runParametersContent);
  const sections = Array.isArray(runParameters?.sections)
    ? runParameters.sections.filter((section) => section?.id !== "telemetry")
    : [];

  for (const section of sections) {
    const entries = Array.isArray(section?.entries) ? section.entries : [];
    if (entries.length === 0) {
      continue;
    }

    const sectionElement = document.createElement("section");
    sectionElement.className = "run-parameters-section";

    const title = document.createElement("div");
    title.className = "run-parameters-section-title";
    title.textContent = section.label || section.id || "Parameters";
    sectionElement.appendChild(title);

    const list = document.createElement("div");
    list.className = "run-parameters-list";

    for (const entry of entries) {
      const row = document.createElement("div");
      row.className = "run-parameters-entry";

      const label = document.createElement("div");
      label.className = "run-parameters-label";
      label.textContent = entry.label || entry.key || "Parameter";

      const value = document.createElement("div");
      value.className = "run-parameters-value";
      value.textContent = entry.value ?? "";

      row.append(label, value);
      list.appendChild(row);
    }

    sectionElement.appendChild(list);
    runParametersContent.appendChild(sectionElement);
  }

  renderAwaitUnitSummary();

  if (!runParametersContent.childElementCount) {
    const empty = document.createElement("div");
    empty.className = "run-parameters-empty";
    empty.textContent = "Run parameters unavailable";
    runParametersContent.appendChild(empty);
  }
}

function runParameterValue(runParameters, key) {
  for (const section of runParameters?.sections ?? []) {
    for (const entry of section?.entries ?? []) {
      if (entry?.key === key) {
        return entry.value ?? null;
      }
    }
  }
  return null;
}

function numericRunParameter(runParameters, key) {
  const value = runParameterValue(runParameters, key);
  if (value == null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function renderAwaitUnitSummary() {
  const units = awaitUnitSummaries(replayDocument.events);
  if (units.length === 0) {
    return;
  }
  const sectionElement = document.createElement("section");
  sectionElement.className = "run-parameters-section await-unit-section";

  const title = document.createElement("div");
  title.className = "run-parameters-section-title";
  title.textContent = "Await units";
  sectionElement.appendChild(title);

  const list = document.createElement("div");
  list.className = "run-parameters-list";

  for (const unit of units) {
    const row = document.createElement("div");
    row.className = "run-parameters-entry await-unit-entry";

    const label = document.createElement("div");
    label.className = "run-parameters-label";
    label.textContent = unit.label;

    const value = document.createElement("div");
    value.className = "run-parameters-value";
    value.textContent = unit.value;

    row.append(label, value);
    list.appendChild(row);
  }

  sectionElement.appendChild(list);
  runParametersContent.appendChild(sectionElement);
}

function awaitUnitSummaries(events) {
  const summariesByUnit = new Map();
  for (const event of events ?? []) {
    if (!isAwaitLifecycleEvent(event)) {
      continue;
    }
    const unitId = awaitLifecycleAttribute(event, AWAIT_ATTR.unitId) ?? event.itemId;
    if (!unitId) {
      continue;
    }
    const summary = summariesByUnit.get(unitId) ?? {
      unitId,
      stepId: null,
      status: null,
      expected: null,
      completed: null,
      lastTime: 0
    };
    summary.stepId = awaitLifecycleAttribute(event, AWAIT_ATTR.stepId) ?? summary.stepId;
    summary.status = awaitLifecycleAttribute(event, AWAIT_ATTR.status) ?? summary.status;
    summary.expected = awaitLifecycleNumber(event, AWAIT_ATTR.expectedItemCount) ?? summary.expected;
    summary.completed = awaitLifecycleNumber(event, AWAIT_ATTR.completedItemCount) ?? summary.completed;
    summary.lastTime = Math.max(summary.lastTime, playbackTimeForEvent(event));
    summariesByUnit.set(unitId, summary);
  }
  return [...summariesByUnit.values()]
    .sort((left, right) => left.lastTime - right.lastTime)
    .map((summary) => {
      const shortUnit = summary.unitId.length > 8 ? `${summary.unitId.slice(0, 8)}...` : summary.unitId;
      const countText = summary.expected == null || summary.completed == null
        ? "count unknown"
        : `${summary.completed}/${summary.expected}`;
      return {
        label: `${normalizeStepLabel(summary.stepId ?? "Await")} ${shortUnit}`,
        value: `${summary.status ?? "UNKNOWN"} | ${countText} | ${summary.lastTime.toFixed(2)}s`
      };
    });
}

function updateLegendForReplay(document) {
  const hasReject = (document.topology?.steps ?? []).some((step) => step.pluginKind === "reject")
    || (document.events ?? []).some((event) => event.event === "reject");
  for (const item of rejectLegendItems) {
    item.hidden = !hasReject;
  }
}

function reportViewerIssue(message) {
  revealPlayerChrome(true);
  loadProgress.hidden = false;
  loadProgressFill.style.width = "100%";
  loadProgressText.textContent = `Status: ${message}`;
  setSourceModalStatus(`Status: ${message}`);
  if (runtimeStatus) {
    runtimeStatus.textContent = message;
  }
  console.error(`[replay-viewer] ${message}`);
}

function reportRuntimeError(context, error) {
  const message = error?.message ?? String(error);
  isPlaying = false;
  isFinishingEffects = false;
  reportViewerIssue(`${context} (${message})`);
  updateUi();
}

function reportNonFatalRuntimeIssue(context, error) {
  const message = error?.message ?? String(error);
  console.error(`[replay-viewer] ${context} (${message})`, error);
}

function loadReplay(document, label, sourceKey = activeReplaySourceKey) {
  fatalRenderErrorLatched = false;
  if (runtimeStatus) {
    runtimeStatus.textContent = "";
  }
  clearScene();
  replayDocument = normalizeReplayDocument(validateReplayDocument(document));
  activeLayoutStorageKey = replayLayoutStorageKey(replayDocument);
  replayDurationSeconds = computeReplayDurationSeconds(replayDocument);
  replayBackpressureCapacity = numericRunParameter(replayDocument.runParameters, "pipeline.defaults.backpressure-buffer-capacity");
  currentTimeSeconds = 0;
  isPlaying = false;
  isFinishingEffects = false;
  resetCompletionPromptForPlayback();
  fallbackAwaitDisplayStep = replayDocument.fallbackAwaitDisplayStep ?? null;
  replayHasAwaitLifecycleEvents = replayDocument.hasAwaitLifecycleEvents === true;
  for (const [rawStepName, displayStepName] of Object.entries(replayDocument.displayAliases ?? {})) {
    displayStepAliases.set(rawStepName, displayStepName);
  }
  for (const step of replayDocument.rawTopology?.steps ?? replayDocument.topology.steps) {
    rawStepMetadataByName.set(step.step, step);
  }
  activeAnimationPolicy = buildSupportAnimationPolicy(
    replayDocument.topology,
    replayDocument.rawTopology ?? replayDocument.topology
  );
  try {
    buildTopology(replayDocument.topology);
  } catch (error) {
    clearScene();
    replayDocument = normalizeReplayDocument(validateReplayDocument({
      topology: { steps: [], transitions: [] },
      events: [],
      durationMs: 0,
      pipeline: "error"
    }));
    replayDurationSeconds = 0.1;
    currentTimeSeconds = 0;
    isPlaying = false;
    isFinishingEffects = false;
    resetPlaybackState();
    reportViewerIssue(`failed to render replay topology (${error.message ?? error})`);
    throw error;
  }
  for (const event of replayDocument.events) {
    const displayStepName = aliasStepNameForDisplay(event?.step);
    const displayFromName = aliasStepNameForDisplay(event?.from);
    const displayToName = aliasStepNameForDisplay(event?.to);
    if (displayStepName) {
      directEventSteps.add(displayStepName);
    }
    if (isAwaitLifecycleEvent(event)) {
      const awaitStepName = awaitStepForLifecycleEvent(event, mapEventForDisplay(event));
      if (awaitStepName) {
        directEventSteps.add(awaitStepName);
      }
    }
    for (const transitionStepName of [displayFromName, displayToName]) {
      if (stepHasRenderRole(transitionStepName, "await")) {
        directEventSteps.add(transitionStepName);
      }
    }
  }
  activeReplaySourceKey = sourceKey;
  stagedReplaySourceKey = sourceKey;
  datasetSelect.value = sourceKey;
  setCustomReplayVisibility(sourceKey === "custom");
  updateSourceApplyButton();
  playerTitle.textContent = label;
  summaryDatasetName.textContent = label;
  summaryPipelineName.textContent = replayDocument.pipeline;
  summaryDurationText.textContent = `${replayDurationSeconds.toFixed(2)}s`;
  summaryTopologyText.textContent = `${replayDocument.topology.steps.length} nodes / ${replayDocument.topology.transitions.length} edges`;
  summaryEventCountText.textContent = String(replayDocument.events.length);
  updateLegendForReplay(replayDocument);
  renderRunParameters(replayDocument.runParameters);
  resetPlaybackState();
  revealPlayerChrome();
  updateUi();
}

async function loadBuiltInReplay(datasetKey) {
  const dataset = BUILT_IN_REPLAYS.get(datasetKey);
  if (!dataset) {
    return;
  }
  isPlaying = false;
  setLoadProgress(true, 0.12, `Loading ${dataset.label}...`);
  updateUi();
  await nextAnimationFrame();
  const response = await fetch(dataset.path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to fetch built-in dataset (${response.status})`);
  }
  const text = await readResponseTextWithProgress(response, dataset.label);
  setLoadProgress(true, 0.88, `Parsing ${dataset.label}...`);
  await nextAnimationFrame();
  let document;
  try {
    document = JSON.parse(text);
  } catch (error) {
    throw new Error(`${dataset.label} is not valid replay JSON: ${error.message}`);
  }
  setLoadProgress(true, 0.96, `Rendering ${dataset.label}...`);
  await nextAnimationFrame();
  try {
    loadReplay(document, dataset.label, datasetKey);
  } catch (error) {
    throw new Error(`${dataset.label} could not be rendered: ${error.message}`);
  }
  finishLoadProgress(`${dataset.label} loaded`);
}

async function applySelectedReplaySource({ closeOnSuccess = true } = {}) {
  if (isLoadingReplay) {
    return false;
  }
  cancelCompletionPrompt();
  const nextSource = datasetSelect.value;
  if (nextSource === EMPTY_REPLAY_SOURCE_KEY) {
    return false;
  }
  try {
    if (nextSource === "custom") {
      const [file] = replayFileInput.files || [];
      if (!file) {
        return false;
      }
      isPlaying = false;
      setLoadProgress(true, 0, `Loading ${file.name}...`);
      updateUi();
      await nextAnimationFrame();
      const text = await readReplayFile(file);
      setLoadProgress(true, 1, `Parsing ${file.name}...`);
      await nextAnimationFrame();
      const parsed = JSON.parse(text);
      loadReplay(parsed, file.name, "custom");
      finishLoadProgress(`${file.name} loaded`);
    } else {
      await loadBuiltInReplay(nextSource);
    }
    resetCompletionPromptForPlayback();
    if (closeOnSuccess && openModal === "source") {
      closeModalElement("source", sourceModal);
    }
    scheduleChromeHide();
    updateUi();
    return true;
  } catch (error) {
    setLoadProgress(false);
    reportViewerIssue(`failed to load replay (${error.message})`);
    setSourceModalStatus(`Could not load replay: ${error.message}`);
    playPauseButton.disabled = false;
    restartButton.disabled = false;
    stepBackButton.disabled = false;
    stepButton.disabled = false;
    return false;
  }
}

async function loadFirstVisitReplayAfterPaint() {
  if (DEFAULT_REPLAY_SOURCE_KEY !== EMPTY_REPLAY_SOURCE_KEY) {
    return;
  }
  try {
    if (localStorage.getItem(FIRST_VISIT_AUTOLOAD_STORAGE_KEY)) {
      return;
    }
  } catch (_error) {
    // Keep the first-run experience useful when storage is unavailable.
  }
  await nextAnimationFrame();
  await nextAnimationFrame();
  await new Promise((resolve) => window.setTimeout(resolve, 2000));
  if (isLoadingReplay || replayDocument.events.length > 0 || isAnyModalOpen()) {
    return;
  }
  try {
    await loadBuiltInReplay(FIRST_VISIT_AUTOLOAD_SOURCE_KEY);
    try {
      localStorage.setItem(FIRST_VISIT_AUTOLOAD_STORAGE_KEY, "true");
    } catch (_error) {
      // Private browsing or locked-down storage should not block the loaded replay.
    }
  } catch (error) {
    setLoadProgress(false);
    reportViewerIssue(`failed to load first-visit replay (${error.message})`);
  }
}

function readReplayFile(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onprogress = (event) => {
      if (!event.lengthComputable) {
        setLoadProgress(true, 0, `Loading ${file.name}...`);
        return;
      }
      setLoadProgress(
        true,
        event.loaded / event.total,
        `Loading ${file.name} (${Math.round((event.loaded / event.total) * 100)}%)`
      );
    };
    reader.onerror = () => reject(reader.error ?? new Error(`Failed to read ${file.name}.`));
    reader.onload = () => resolve(typeof reader.result === "string" ? reader.result : "");
    reader.readAsText(file);
  });
}

function computeAutomaticLayoutPositions(topology) {
  const steps = Array.isArray(topology.steps) ? topology.steps : [];
  const transitions = Array.isArray(topology.transitions) ? topology.transitions : [];
  const positions = new Map();
  const baseSteps = steps.filter((step) => !step.sideEffect);
  const sideEffects = steps.filter((step) => step.sideEffect);
  const sideEffectsByParent = new Map();
  const primaryBranchLayout = analyzePrimaryBranchLayout(baseSteps, transitions);
  const mainlineSteps = baseSteps.filter((step) => !primaryBranchLayout.branchNodes.has(step.step));

  for (const step of sideEffects) {
    const parentStep = step.parentStep || "__unattached__";
    if (!sideEffectsByParent.has(parentStep)) {
      sideEffectsByParent.set(parentStep, []);
    }
    sideEffectsByParent.get(parentStep).push(step);
  }

  const spacing = mainlineSteps.length > 4 ? 3.5 : 4.35;
  const totalWidth = Math.max(0, (mainlineSteps.length - 1) * spacing);
  mainlineSteps.forEach((step, index) => {
    const x = index * spacing - totalWidth / 2;
    const stagger = mainlineSteps.length > 4
      ? (index % 2 === 0 ? 0.34 : -0.34)
      : (index % 2 === 0 ? 0.18 : -0.18);
    positions.set(step.step, new THREE.Vector3(x, PRIMARY_ROW_Y + stagger, 0));
  });
  applyPrimaryBranchLayout(positions, primaryBranchLayout);

  for (const step of sideEffects) {
    const role = resolveDisplayRole(step);
    if (role === "store") {
      continue;
    }
    const parentPosition = step.parentStep ? positions.get(step.parentStep) : null;
    if (!parentPosition) {
      continue;
    }
    let offsetX = 0;
    let offsetY = BRANCH_ROW_OFFSET_Y;
    if (role === "object-ingest") {
      offsetX = -2.05;
      offsetY = 0.02;
    } else if (role === "object-publish") {
      offsetX = 2.05;
      offsetY = 0.02;
    } else if (role === "query-connector") {
      offsetX = -1.42;
      offsetY = -1.18;
    } else if (role === "broker") {
      offsetX = -1.52;
      offsetY = -1.45;
    } else if (role === "external-provider") {
      offsetX = 1.52;
      offsetY = -1.45;
    } else {
      const siblings = sideEffectsByParent.get(step.parentStep) || [];
      const siblingIndex = siblings.findIndex((candidate) => candidate.step === step.step);
      const band = Math.floor(siblingIndex / 2);
      const centeredIndex = siblingIndex - (siblings.length - 1) / 2;
      offsetX = centeredIndex * 1.28;
      offsetY = BRANCH_ROW_OFFSET_Y + band * 1;
    }
    positions.set(step.step, new THREE.Vector3(parentPosition.x + offsetX, parentPosition.y - offsetY, 0));
  }

  const storeActors = sideEffects.filter((step) => resolveDisplayRole(step) === "store");
  const topologyPositions = [...positions.values()];
  const topologyMinX = topologyPositions.length > 0
    ? Math.min(...topologyPositions.map((position) => position.x))
    : -1;
  const topologyMaxX = topologyPositions.length > 0
    ? Math.max(...topologyPositions.map((position) => position.x))
    : 1;
  const topologyCenterX = (topologyMinX + topologyMaxX) / 2;
  const topologyWidth = Math.max(1.6, topologyMaxX - topologyMinX);
  storeActors.forEach((step, index) => {
    const inboundStoreTransitions = transitions.filter((transition) => transition.to === step.step && transition.relationKind === "store");
    const sourcePositions = inboundStoreTransitions
      .map((transition) => positions.get(transition.from))
      .filter(Boolean);
    const averageX = sourcePositions.length > 0
      ? sourcePositions.reduce((sum, position) => sum + position.x, 0) / sourcePositions.length
      : topologyCenterX;
    const spreadOffset = storeActors.length > 1
      ? ((index / (storeActors.length - 1)) - 0.5) * Math.min(2.4, topologyWidth * 0.38)
      : 0;
    const x = averageX + spreadOffset;
    const y = PRIMARY_ROW_Y - BRANCH_ROW_OFFSET_Y - 0.28 - index * 0.3;
    positions.set(step.step, new THREE.Vector3(x, y, 0));
  });

  return positions;
}

function analyzePrimaryBranchLayout(baseSteps, transitions) {
  const baseStepNames = new Set(baseSteps.map((step) => step.step));
  const orderedBaseSteps = [...baseSteps].sort((left, right) =>
    (left.index ?? Number.MAX_SAFE_INTEGER) - (right.index ?? Number.MAX_SAFE_INTEGER));
  const primaryTransitions = transitions.filter((transition) =>
    (transition?.relationKind ?? "primary") === "primary"
      && baseStepNames.has(transition.from)
      && baseStepNames.has(transition.to));
  const outbound = new Map();
  const inbound = new Map();
  const stepIndex = new Map(orderedBaseSteps.map((step, index) => [step.step, index]));
  for (const step of orderedBaseSteps) {
    outbound.set(step.step, []);
    inbound.set(step.step, []);
  }
  for (const transition of primaryTransitions) {
    outbound.get(transition.from)?.push(transition.to);
    inbound.get(transition.to)?.push(transition.from);
  }

  const branchNodes = new Set();
  const branchGroups = [];
  const consumedSplits = new Set();
  for (const step of orderedBaseSteps) {
    const branchStarts = outbound.get(step.step) ?? [];
    if (branchStarts.length < 2 || consumedSplits.has(step.step)) {
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
    consumedSplits.add(step.step);
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
  if (!Array.isArray(branchStarts) || branchStarts.length < 2) {
    return null;
  }
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

function applyPrimaryBranchLayout(positions, branchLayout) {
  for (const group of branchLayout.branchGroups) {
    const splitPosition = positions.get(group.split);
    const mergePosition = positions.get(group.merge);
    if (!splitPosition || !mergePosition) {
      continue;
    }
    const span = Math.max(mergePosition.x - splitPosition.x, 2.4);
    const branchY = Math.min(splitPosition.y, mergePosition.y) - BRANCH_ROW_OFFSET_Y - 0.12;
    group.paths.forEach((path, branchIndex) => {
      const laneCenter = splitPosition.x + (span * (branchIndex + 1) / (group.paths.length + 1));
      path.forEach((stepName, stepIndex) => {
        const offsetX = path.length === 1
          ? 0
          : ((stepIndex / (path.length - 1)) - 0.5) * Math.min(1.15, span * 0.22);
        positions.set(stepName, new THREE.Vector3(
          laneCenter + offsetX,
          branchY - Math.floor(stepIndex / 3) * 0.62,
          0));
      });
    });
  }
}

function buildTopology(topology) {
  const steps = Array.isArray(topology.steps) ? topology.steps : [];
  const transitions = Array.isArray(topology.transitions) ? topology.transitions : [];
  steps.forEach((step) => {
    stepMetadataByName.set(step.step, step);
  });

  const savedPositions = readSavedLayoutPositions();
  const autoPositions = computeAutomaticLayoutPositions(topology);
  automaticNodePositions.clear();
  for (const [stepName, position] of autoPositions.entries()) {
    automaticNodePositions.set(stepName, position.clone());
  }

  for (const step of steps) {
    const autoPosition = autoPositions.get(step.step);
    if (!autoPosition) {
      continue;
    }
    const savedPosition = savedPositions.get(step.step);
    registerNode(step, savedPosition ?? autoPosition);
  }

  transitions.forEach((transition) => {
    const source = nodePositions.get(transition.from);
    const target = nodePositions.get(transition.to);
    if (!source || !target) {
      return;
    }
    const edgeStyle = edgeStyleForTransition(transition, steps);
    const material = edgeStyle.dashed
      ? new THREE.LineDashedMaterial({
          color: edgeStyle.baseColor,
          transparent: true,
          opacity: edgeStyle.baseOpacity,
          dashSize: edgeStyle.dashSize,
          gapSize: edgeStyle.gapSize
        })
      : new THREE.LineBasicMaterial({ color: edgeStyle.baseColor, transparent: true, opacity: edgeStyle.baseOpacity });
    const geometry = new THREE.BufferGeometry().setFromPoints([source.clone(), target.clone()]);
    const line = new THREE.Line(geometry, material);
    if (edgeStyle.dashed) {
      line.computeLineDistances();
    }
    scene.add(line);
    edgeLines.set(edgeKey(transition.from, transition.to), {
      line,
      from: transition.from,
      to: transition.to,
      baseColor: edgeStyle.baseColor,
      accentColor: edgeStyle.accentColor,
      baseOpacity: edgeStyle.baseOpacity,
      intensity: 0,
      shimmerPhase: Math.random() * Math.PI * 2,
      branch: edgeStyle.dashed
    });
  });

  fitCameraToTopology(steps);
}

function edgeStyleForTransition(transition, steps) {
  const relationKind = transition?.relationKind ?? "primary";
  const targetStep = steps.find((step) => step.step === transition.to);
  if (relationKind === "object-ingest" || relationKind === "object-publish") {
    return {
      dashed: true,
      baseColor: 0x7bdcb5,
      accentColor: 0x9fffd2,
      baseOpacity: 0.72,
      dashSize: 0.28,
      gapSize: 0.14
    };
  }
  if (relationKind === "query" || relationKind === "query-connector") {
    return {
      dashed: true,
      baseColor: 0x67b2f3,
      accentColor: 0x9bd6ff,
      baseOpacity: 0.68,
      dashSize: 0.22,
      gapSize: 0.12
    };
  }
  const isBranch = targetStep?.sideEffect === true;
  return {
    dashed: isBranch,
    baseColor: isBranch ? 0x8f7aea : 0x34527f,
    accentColor: isBranch ? 0xcf9cff : 0x7ad7ff,
    baseOpacity: isBranch ? 0.55 : 0.8,
    dashSize: 0.32,
    gapSize: 0.18
  };
}

function registerNode(step, position) {
  const isSideEffect = Boolean(step.sideEffect);
  const role = resolveDisplayRole(step);
  const geometry = new THREE.SphereGeometry(isSideEffect ? BRANCH_NODE_RADIUS : BASE_NODE_RADIUS, 24, 24);
  const material = new THREE.MeshStandardMaterial({
    color: nodeColorForStep(step),
    emissive: isSideEffect ? 0x1d2742 : 0x17355b,
    emissiveIntensity: 0.34,
    roughness: 0.35,
    metalness: 0.15
  });
  const mesh = new THREE.Mesh(geometry, material);
  mesh.position.copy(position);
  mesh.userData.step = step;
  mesh.userData.phase = step.index * 0.73 + (isSideEffect ? 0.4 : 0);
  mesh.userData.isSideEffect = isSideEffect;
  scene.add(mesh);
  nodeMeshes.set(step.step, mesh);
  nodePositions.set(step.step, position.clone());
  const namedSupportActor = isNamedSupportActor(step);
  if (!isSideEffect) {
    registerPressureRing(step, position);
  }
  if (!isSideEffect || namedSupportActor) {
    const sprite = buildStepLabelSprite(step.step);
    const labelOffset = isSideEffect ? LABEL_OFFSET_Y - 0.08 : LABEL_OFFSET_Y;
    sprite.scale.multiplyScalar(isSideEffect ? SUPPORT_LABEL_HEIGHT / BASE_LABEL_HEIGHT : 1);
    sprite.userData.baseHeight = isSideEffect ? SUPPORT_LABEL_HEIGHT : BASE_LABEL_HEIGHT;
    sprite.position.copy(position).add(new THREE.Vector3(0, -labelOffset, 0));
    scene.add(sprite);
    nodeLabelSprites.set(step.step, sprite);
  }
  if (showsThroughputCounter(step)) {
    const valueSprite = buildThroughputCounterSprite("—|—");
    valueSprite.position.copy(position).add(isSideEffect ? new THREE.Vector3(0, -0.3, 0) : new THREE.Vector3(0, 0, 0));
    scene.add(valueSprite);
    nodeValueSprites.set(step.step, valueSprite);
  } else {
    const iconSprite = buildPluginIconSprite(step);
    iconSprite.position.copy(position);
    scene.add(iconSprite);
    nodeIconSprites.set(step.step, iconSprite);
    if (step.pluginKind === "reject") {
      const rejectSprite = buildTextSprite("0", {
        fontSize: 26,
        fontWeight: 700,
        fillStyle: "#f8fbff",
        paddingX: 7,
        paddingY: 5,
        backgroundStyle: "rgba(7, 16, 31, 0.0)",
        borderStyle: null,
        height: 0.24
      });
      rejectSprite.position.copy(position).add(new THREE.Vector3(0, -0.3, 0));
      scene.add(rejectSprite);
      nodeValueSprites.set(step.step, rejectSprite);
    }
  }
}

function labelOffsetForStep(step) {
  const isSideEffect = Boolean(step?.sideEffect);
  return isSideEffect ? LABEL_OFFSET_Y - 0.08 : LABEL_OFFSET_Y;
}

function updateNodeDecorations(stepName) {
  const position = nodePositions.get(stepName);
  const step = resolveStepDefinition(stepName);
  if (!position || !step) {
    return;
  }
  const label = nodeLabelSprites.get(stepName);
  if (label) {
    label.position.copy(position).add(new THREE.Vector3(0, -labelOffsetForStep(step), 0));
  }
  const value = nodeValueSprites.get(stepName);
  if (value) {
    const valueOffset = step.sideEffect && step.pluginKind === "reject"
      ? new THREE.Vector3(0, -0.3, 0)
      : new THREE.Vector3(0, 0, 0);
    value.position.copy(position).add(valueOffset);
  }
  updatePressureRingPosition(stepName, position);
  const icon = nodeIconSprites.get(stepName);
  if (icon) {
    icon.position.copy(position);
  }
}

function updateEdgeGeometry(edge) {
  const source = nodePositions.get(edge.from);
  const target = nodePositions.get(edge.to);
  if (!source || !target) {
    return;
  }
  edge.line.geometry.dispose();
  edge.line.geometry = new THREE.BufferGeometry().setFromPoints([source.clone(), target.clone()]);
  if (edge.branch) {
    edge.line.computeLineDistances();
  }
}

function updateConnectedEdges(stepName) {
  for (const edge of edgeLines.values()) {
    if (edge.from === stepName || edge.to === stepName) {
      updateEdgeGeometry(edge);
    }
  }
}

function setNodePosition(stepName, position) {
  const mesh = nodeMeshes.get(stepName);
  if (!mesh) {
    return;
  }
  const nextPosition = position.clone();
  mesh.position.copy(nextPosition);
  nodePositions.set(stepName, nextPosition);
  updateNodeDecorations(stepName);
  updateConnectedEdges(stepName);
}

function resetCurrentLayout() {
  if (automaticNodePositions.size === 0) {
    return;
  }
  clearSavedLayoutPositions();
  for (const [stepName, position] of automaticNodePositions.entries()) {
    setNodePosition(stepName, position);
  }
  fitCameraToTopology(replayDocument.topology.steps);
  updateLabels();
}

function buildStepLabelSprite(stepName) {
  const labelLines = layoutStepLabel(stepName);
  const paddingX = 14;
  const paddingY = 10;
  const lineGap = 6;
  return buildTextSprite(labelLines.join("\n"), {
    fontSize: 30,
    fontWeight: 600,
    fillStyle: "#f8fbff",
    paddingX,
    paddingY,
    lineGap,
    backgroundStyle: "rgba(7, 16, 31, 0.0)",
    borderStyle: null,
    shadowColor: "rgba(7, 16, 31, 0.92)",
    shadowBlur: 10,
    height: BASE_LABEL_HEIGHT
  });
}

function buildPluginIconSprite(step) {
  const iconKind = resolveDisplayIconKind(step);
  return buildIconSprite(iconKind, iconKind === "reject" ? 0.54 : 0.72);
}

function buildThroughputCounterSprite(text) {
  return buildTextSprite(text, {
    fontSize: 28,
    fontWeight: 700,
    fillStyle: "#f8fbff",
    paddingX: 12,
    paddingY: 8,
    backgroundStyle: "rgba(7, 16, 31, 0.0)",
    borderStyle: null,
    height: COUNTER_LABEL_HEIGHT
  });
}

function replaceThroughputCounterSprite(sprite, text) {
  replaceSpriteText(sprite, text, sprite.userData.options ?? {
    fontSize: 28,
    fontWeight: 700,
    fillStyle: "#f8fbff",
    paddingX: 12,
    paddingY: 8,
    backgroundStyle: "rgba(7, 16, 31, 0.0)",
    borderStyle: null,
    height: COUNTER_LABEL_HEIGHT
  });
  sprite.userData.text = text;
}

function registerPressureRing(step, position) {
  const radius = BASE_NODE_RADIUS + 0.14;
  const width = 0.065;
  const trackGeometry = new THREE.RingGeometry(
    radius,
    radius + width,
    80,
    1,
    PRESSURE_RING_START_ANGLE,
    PRESSURE_RING_SWEEP_ANGLE
  );
  const trackMaterial = new THREE.MeshBasicMaterial({
    color: 0x8da2c9,
    transparent: true,
    opacity: 0.22,
    side: THREE.DoubleSide,
    depthTest: false
  });
  const track = new THREE.Mesh(trackGeometry, trackMaterial);
  track.position.copy(position).setZ(0.05);
  track.renderOrder = 8;
  scene.add(track);

  const arcMaterial = new THREE.MeshBasicMaterial({
    color: 0x8bffa5,
    transparent: true,
    opacity: 0,
    side: THREE.DoubleSide,
    depthTest: false
  });
  const arc = new THREE.Mesh(
    new THREE.RingGeometry(radius, radius + width, 80, 1, PRESSURE_RING_START_ANGLE, -0.001),
    arcMaterial
  );
  arc.position.copy(position).setZ(0.06);
  arc.renderOrder = 9;
  scene.add(arc);

  nodePressureRings.set(step.step, {
    track,
    arc,
    radius,
    width,
    ratio: null
  });
}

function updatePressureRingPosition(stepName, position) {
  const ring = nodePressureRings.get(stepName);
  if (!ring) {
    return;
  }
  ring.track.position.copy(position).setZ(0.05);
  ring.arc.position.copy(position).setZ(0.06);
}

function updatePressureRing(stepName, pressureRatio) {
  const ring = nodePressureRings.get(stepName);
  if (!ring) {
    return;
  }
  const normalizedRatio = Number.isFinite(pressureRatio) ? clamp(pressureRatio, 0, 1) : null;
  if (ring.ratio === normalizedRatio) {
    return;
  }
  ring.ratio = normalizedRatio;
  ring.arc.geometry.dispose();
  const thetaLength = normalizedRatio == null || normalizedRatio <= 0
    ? -0.001
    : PRESSURE_RING_SWEEP_ANGLE * normalizedRatio;
  ring.arc.geometry = new THREE.RingGeometry(
    ring.radius,
    ring.radius + ring.width,
    80,
    1,
    PRESSURE_RING_START_ANGLE,
    thetaLength
  );
  ring.arc.material.opacity = normalizedRatio == null || normalizedRatio <= 0 ? 0 : 0.96;
  if (normalizedRatio != null) {
    ring.arc.material.color.setHex(pressureColorForRatio(normalizedRatio));
  }
}

function pressureColorForRatio(ratio) {
  if (ratio >= 0.8) {
    return 0xff7c8f;
  }
  if (ratio >= 0.45) {
    return 0xffd166;
  }
  return 0x8bffa5;
}

function buildTextSprite(text, options = {}) {
  const {
    fontSize = 20,
    fontWeight = 600,
    paddingX = 10,
    paddingY = 8,
    lineGap = 4,
    fillStyle = "#f8fbff",
    backgroundStyle = "rgba(7, 16, 31, 0.0)",
    borderStyle = null,
    shadowColor = "rgba(7, 16, 31, 0.0)",
    shadowBlur = 0,
    height = 0.4
  } = options;
  const lines = String(text).split("\n");
  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d");
  context.font = `${fontWeight} ${fontSize}px Inter, system-ui, sans-serif`;
  const lineWidths = lines.map((line) => Math.ceil(context.measureText(line).width));
  const textWidth = Math.max(...lineWidths, 1);
  const lineHeight = fontSize + lineGap;
  canvas.width = textWidth + paddingX * 2;
  canvas.height = lines.length * lineHeight + paddingY * 2 - lineGap;
  const draw = canvas.getContext("2d");
  draw.clearRect(0, 0, canvas.width, canvas.height);
  if (backgroundStyle && backgroundStyle !== "transparent") {
    draw.fillStyle = backgroundStyle;
    draw.fillRect(0, 0, canvas.width, canvas.height);
  }
  if (borderStyle) {
    draw.strokeStyle = borderStyle;
    draw.lineWidth = 2;
    draw.strokeRect(1, 1, canvas.width - 2, canvas.height - 2);
  }
  draw.font = `${fontWeight} ${fontSize}px Inter, system-ui, sans-serif`;
  draw.fillStyle = fillStyle;
  draw.textAlign = "center";
  draw.textBaseline = "top";
  draw.shadowColor = shadowColor;
  draw.shadowBlur = shadowBlur;
  lines.forEach((line, index) => {
    draw.fillText(line, canvas.width / 2, paddingY + index * lineHeight);
  });
  const texture = new THREE.CanvasTexture(canvas);
  texture.needsUpdate = true;
  texture.minFilter = THREE.LinearFilter;
  const material = new THREE.SpriteMaterial({
    map: texture,
    transparent: true,
    depthTest: false
  });
  const sprite = new THREE.Sprite(material);
  const aspect = canvas.width / canvas.height;
  sprite.scale.set(height * aspect, height, 1);
  sprite.userData.aspect = aspect;
  sprite.userData.baseHeight = height;
  sprite.userData.text = text;
  sprite.userData.options = options;
  sprite.renderOrder = 10;
  return sprite;
}

function replaceSpriteText(sprite, text, options = sprite.userData.options ?? {}) {
  const replacement = buildTextSprite(text, options);
  sprite.material.map?.dispose();
  sprite.material.dispose();
  sprite.material = replacement.material;
  sprite.scale.copy(replacement.scale);
  sprite.userData.aspect = replacement.userData.aspect;
  sprite.userData.baseHeight = replacement.userData.baseHeight;
  sprite.userData.text = text;
  sprite.userData.options = options;
}

function buildIconSprite(pluginKind, height = 0.28) {
  const canvas = document.createElement("canvas");
  canvas.width = 160;
  canvas.height = 160;
  const context = canvas.getContext("2d");
  context.clearRect(0, 0, canvas.width, canvas.height);
  context.strokeStyle = "#f8fbff";
  context.fillStyle = "#f8fbff";
  context.lineWidth = 9;
  context.lineCap = "round";
  context.lineJoin = "round";

  switch (pluginKind) {
    case "persistence":
    case "store":
      drawDatabaseIcon(context);
      break;
    case "broker":
      drawBrokerIcon(context);
      break;
    case "provider":
      drawProviderIcon(context);
      break;
    case "object-ingest":
      drawObjectIngestIcon(context);
      break;
    case "object-publish":
      drawObjectPublishIcon(context);
      break;
    case "query":
      drawQueryIcon(context);
      break;
    case "cache":
      drawCacheIcon(context);
      break;
    case "cache-invalidate":
      drawCacheIcon(context);
      drawInvalidateAccent(context, "!");
      break;
    case "cache-invalidate-all":
      drawCacheIcon(context);
      drawInvalidateAccent(context, "x");
      break;
    case "reject":
      drawRejectQueueIcon(context);
      break;
    default:
      drawPluginDefaultIcon(context);
      break;
  }

  const texture = new THREE.CanvasTexture(canvas);
  texture.needsUpdate = true;
  texture.minFilter = THREE.LinearFilter;
  const material = new THREE.SpriteMaterial({
    map: texture,
    transparent: true,
    depthTest: false
  });
  const sprite = new THREE.Sprite(material);
  sprite.scale.set(height, height, 1);
  sprite.userData.aspect = 1;
  sprite.userData.baseHeight = height;
  sprite.renderOrder = 10;
  return sprite;
}

function drawDatabaseIcon(context) {
  context.beginPath();
  context.ellipse(80, 38, 34, 15, 0, 0, Math.PI * 2);
  context.stroke();
  context.beginPath();
  context.moveTo(46, 38);
  context.lineTo(46, 102);
  context.moveTo(114, 38);
  context.lineTo(114, 102);
  context.stroke();
  context.beginPath();
  context.ellipse(80, 102, 34, 15, 0, 0, Math.PI);
  context.stroke();
  context.beginPath();
  context.ellipse(80, 70, 34, 15, 0, 0, Math.PI);
  context.stroke();
}

function drawBrokerIcon(context) {
  roundRectPath(context, 28, 34, 22, 72, 8);
  context.stroke();
  roundRectPath(context, 57, 46, 22, 60, 8);
  context.stroke();
  roundRectPath(context, 86, 28, 22, 78, 8);
  context.stroke();
}

function drawProviderIcon(context) {
  roundRectPath(context, 38, 38, 60, 64, 12);
  context.stroke();
  context.beginPath();
  context.moveTo(98, 56);
  context.lineTo(122, 56);
  context.moveTo(98, 84);
  context.lineTo(122, 84);
  context.stroke();
  context.beginPath();
  context.moveTo(58, 58);
  context.lineTo(78, 58);
  context.moveTo(58, 80);
  context.lineTo(78, 80);
  context.stroke();
}

function drawObjectIngestIcon(context) {
  drawObjectBucketIcon(context);
  context.beginPath();
  context.moveTo(50, 82);
  context.lineTo(92, 82);
  context.moveTo(78, 66);
  context.lineTo(94, 82);
  context.lineTo(78, 98);
  context.stroke();
}

function drawObjectPublishIcon(context) {
  drawObjectBucketIcon(context);
  context.beginPath();
  context.moveTo(110, 82);
  context.lineTo(68, 82);
  context.moveTo(82, 66);
  context.lineTo(66, 82);
  context.lineTo(82, 98);
  context.stroke();
}

function drawObjectBucketIcon(context) {
  roundRectPath(context, 42, 48, 76, 68, 12);
  context.stroke();
  context.beginPath();
  context.moveTo(52, 66);
  context.lineTo(108, 66);
  context.moveTo(56, 100);
  context.lineTo(104, 100);
  context.stroke();
}

function drawQueryIcon(context) {
  drawDatabaseIcon(context);
  context.beginPath();
  context.moveTo(58, 120);
  context.lineTo(104, 120);
  context.moveTo(90, 106);
  context.lineTo(106, 120);
  context.lineTo(90, 134);
  context.stroke();
}

function drawCacheIcon(context) {
  roundRectPath(context, 40, 38, 64, 24, 10);
  context.stroke();
  roundRectPath(context, 34, 62, 76, 24, 10);
  context.stroke();
  roundRectPath(context, 28, 86, 88, 24, 10);
  context.stroke();
}

function drawInvalidateAccent(context, glyph) {
  context.save();
  context.fillStyle = "#ffdf8f";
  context.strokeStyle = "#ffdf8f";
  context.font = glyph === "!" ? "700 40px Inter, system-ui, sans-serif" : "700 36px Inter, system-ui, sans-serif";
  context.textAlign = "center";
  context.textBaseline = "middle";
  context.beginPath();
  context.arc(124, 40, 20, 0, Math.PI * 2);
  context.fillStyle = "rgba(7, 16, 31, 0.72)";
  context.fill();
  context.lineWidth = 4;
  context.strokeStyle = "#ffdf8f";
  context.stroke();
  context.fillStyle = "#ffdf8f";
  context.fillText(glyph === "!" ? "!" : "×", 124, 41);
  context.restore();
}

function drawRejectQueueIcon(context) {
  roundRectPath(context, 38, 46, 84, 38, 10);
  context.stroke();
  context.beginPath();
  context.moveTo(50, 60);
  context.lineTo(110, 60);
  context.moveTo(50, 72);
  context.lineTo(110, 72);
  context.stroke();
  context.beginPath();
  context.moveTo(38, 84);
  context.lineTo(58, 110);
  context.lineTo(102, 110);
  context.lineTo(122, 84);
  context.stroke();
}

function drawPluginDefaultIcon(context) {
  roundRectPath(context, 42, 42, 76, 76, 14);
  context.stroke();
}

function roundRectPath(context, x, y, width, height, radius) {
  context.beginPath();
  context.moveTo(x + radius, y);
  context.lineTo(x + width - radius, y);
  context.quadraticCurveTo(x + width, y, x + width, y + radius);
  context.lineTo(x + width, y + height - radius);
  context.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
  context.lineTo(x + radius, y + height);
  context.quadraticCurveTo(x, y + height, x, y + height - radius);
  context.lineTo(x, y + radius);
  context.quadraticCurveTo(x, y, x + radius, y);
}

function layoutStepLabel(stepName) {
  const normalized = normalizeStepLabel(stepName);
  const words = normalized.split(/\s+/).filter(Boolean);
  if (words.length <= 2) {
    return [normalized];
  }

  const lines = [];
  let current = "";
  for (const word of words) {
    const candidate = current ? `${current} ${word}` : word;
    if (candidate.length <= 14) {
      current = candidate;
      continue;
    }
    if (current) {
      lines.push(current);
    }
    current = word;
  }
  if (current) {
    lines.push(current);
  }
  return lines.slice(0, 3);
}

function normalizeStepLabel(stepName) {
  const step = resolveStepDefinition(stepName);
  const roleLabel = STEP_ROLE_LABELS[resolveDisplayRole(step)];
  if (roleLabel) {
    return roleLabel;
  }
  const explicitLabel = STEP_LABELS[stepName];
  if (explicitLabel) {
    return explicitLabel;
  }
  const withoutPrefix = stepName.replace(/^Process/, "");
  return withoutPrefix
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim();
}

function nodeColorForStep(step) {
  const role = resolveDisplayRole(step);
  if (!step?.sideEffect) {
    if (role === "await") {
      return 0x78c8ff;
    }
    if (role === "command") {
      return 0xffd166;
    }
    return 0x5cc8ff;
  }
  switch (resolveDisplayIconKind(step)) {
    case "store":
      return 0x67b2f3;
    case "broker":
      return 0x8f7aea;
    case "provider":
      return 0xb08cff;
    case "object-ingest":
      return 0x8bffa5;
    case "object-publish":
      return 0x86f0cf;
    case "query":
      return 0x67b2f3;
    case "persistence":
      return 0x7ed0ff;
    case "cache":
      return 0x8bffa5;
    case "cache-invalidate":
      return 0xffd166;
    case "cache-invalidate-all":
      return 0xff9f68;
    case "reject":
      return 0xff7c8f;
    default:
      return 0xcaa6ff;
  }
}

function fitCameraToTopology(steps) {
  if (!steps || steps.length === 0 || nodePositions.size === 0) {
    camera.position.set(0, 0, 24);
    camera.lookAt(0, 0, 0);
    return;
  }

  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;

  for (const step of steps) {
    const position = nodePositions.get(step.step);
    if (!position) {
      continue;
    }
    const radius = step.sideEffect ? BRANCH_NODE_RADIUS : BASE_NODE_RADIUS;
    const labelHeight = !step.sideEffect ? BASE_LABEL_HEIGHT : (isNamedSupportActor(step) ? SUPPORT_LABEL_HEIGHT : 0);
    const valueHeight = step.sideEffect ? (step.pluginKind === "reject" ? 0.24 : 0.08) : COUNTER_LABEL_HEIGHT * 0.5;
    const labelAspect = nodeLabelSprites.get(step.step)?.userData?.aspect ?? 1;
    const labelWidth = !step.sideEffect
      ? labelAspect * BASE_LABEL_HEIGHT * 0.5
      : (isNamedSupportActor(step) ? labelAspect * SUPPORT_LABEL_HEIGHT * 0.5 : 0);
    minX = Math.min(minX, position.x - radius - labelWidth);
    maxX = Math.max(maxX, position.x + radius + labelWidth);
    minY = Math.min(minY, position.y - radius - labelHeight - valueHeight - 0.42);
    maxY = Math.max(maxY, position.y + radius + 0.32);
  }

  if (!Number.isFinite(minX) || !Number.isFinite(maxX) || !Number.isFinite(minY) || !Number.isFinite(maxY)) {
    return;
  }

  const width = Math.max(1, maxX - minX);
  const height = Math.max(1, maxY - minY);
  const centerX = (minX + maxX) / 2;
  const verticalBias = camera.aspect < 0.9 ? -0.08 : 0.08;
  const centerY = (minY + maxY) / 2 + height * verticalBias;
  const halfFovRadians = THREE.MathUtils.degToRad(camera.fov / 2);
  const fitHeightDistance = height * 0.5 / Math.tan(halfFovRadians);
  const fitWidthDistance = width * 0.5 / (Math.tan(halfFovRadians) * camera.aspect);
  const distance = Math.max(fitHeightDistance, fitWidthDistance) * CAMERA_PADDING;

  camera.position.set(centerX, centerY, clamp(distance, 8, 34));
  camera.lookAt(centerX, centerY, 0);
}

function highlightStep(stepName, holdSeconds = EFFECT_PRESETS.node.defaultHoldSeconds, atTime = currentTimeSeconds) {
  if (!stepName || !nodeMeshes.has(stepName)) {
    return;
  }
  const nextExpiry = atTime + holdSeconds;
  highlightExpirations.set(stepName, Math.max(highlightExpirations.get(stepName) ?? 0, nextExpiry));
}

function particleColorForEvent(eventName) {
  if (eventName === "error") {
    return 0xff647c;
  }
  if (eventName === "retry") {
    return 0xffb454;
  }
  if (eventName === "emit") {
    return 0x8bffa5;
  }
  return 0x7ad7ff;
}

function edgeKey(from, to) {
  return `${from ?? "entry"}->${to ?? "exit"}`;
}

function boostEdge(from, to, amount = 0.65) {
  const edge = edgeLines.get(edgeKey(from, to));
  if (!edge) {
    return;
  }
  edge.intensity = Math.min(1.4, edge.intensity + amount);
}

function resolveSourceAnchors(event) {
  if (event.cardinality === "many-to-one" && Array.isArray(event.parentItemIds) && event.parentItemIds.length > 1) {
    const sources = event.parentItemIds
      .map((itemId) => itemAnchors.get(itemId))
      .map((stepName) => ({ stepName, position: nodePositions.get(stepName) }))
      .filter((source) => source.position);
    if (sources.length > 0) {
      return sources;
    }
  }
  const anchoredSource = event.from ? nodePositions.get(event.from) : null;
  return anchoredSource ? [{ stepName: event.from, position: anchoredSource }] : [];
}

function spawnTransit(event) {
  const toNode = nodePositions.get(event.step);
  if (!toNode) {
    return;
  }
  const eventEndTime = event.endTime ?? event.startTime;
  const duration = Math.max(0.12, eventEndTime - event.startTime);
  const color = particleColorForEvent(event.event);
  if (event.from && event.step) {
    boostEdge(event.from, event.step, event.event === "error" ? 1.1 : 0.78);
  }
  const sources = resolveSourceAnchors(event);
  (sources.length > 0 ? sources : [{ stepName: event.step, position: toNode }]).forEach((source, index) => {
    const geometry = new THREE.SphereGeometry(0.13, 16, 16);
    const material = new THREE.MeshBasicMaterial({
      color,
      transparent: true,
      opacity: 0.95
    });
    const mesh = new THREE.Mesh(geometry, material);
    scene.add(mesh);
    particles.set(`${event.sequence}:${event.event}:${index}`, {
      mesh,
      source: source.position.clone(),
      target: toNode.clone(),
      sourceStep: source.stepName,
      targetStep: event.step,
      start: event.startTime,
      end: event.startTime + duration
    });
  });
}

function spawnEmitSpark(event) {
  const sourceStep = event.from || event.step;
  const source = nodePositions.get(sourceStep);
  const targetStep = event.to || sourceStep;
  const target = event.to
    ? nodePositions.get(event.to) || source
    : source;
  if (!source || !target) {
    return;
  }
  if (event.from && event.to) {
    boostEdge(event.from, event.to, 0.88);
  }
  const trailCount = event.cardinality === "one-to-many" ? 2 : 1;
  for (let index = 0; index < trailCount; index += 1) {
    const geometry = new THREE.SphereGeometry(index === 0 ? 0.1 : 0.075, 12, 12);
    const material = new THREE.MeshBasicMaterial({
      color: 0x8bffa5,
      transparent: true,
      opacity: index === 0 ? 0.95 : 0.78
    });
    const mesh = new THREE.Mesh(geometry, material);
    scene.add(mesh);
    particles.set(`${event.sequence}:emit:${index}`, {
      mesh,
      source: source.clone(),
      target: target.clone(),
      sourceStep,
      targetStep,
      start: event.startTime + index * EFFECT_PRESETS.emit.trailDelay,
      end: event.startTime + 0.35 + index * EFFECT_PRESETS.emit.trailDelay
    });
  }
  if (!event.to && event.from) {
    spawnPulse(event.from, 0x8bffa5, EFFECT_PRESETS.pulse.success, event.startTime + 0.2);
  }
}

function spawnPulse(stepName, color, preset, startTime = currentTimeSeconds) {
  const node = nodePositions.get(stepName);
  if (!node) {
    return;
  }
  const geometry = new THREE.RingGeometry(0.48, 0.58, 32);
  const material = new THREE.MeshBasicMaterial({
    color,
    transparent: true,
    opacity: 0.88,
    side: THREE.DoubleSide
  });
  const ring = new THREE.Mesh(geometry, material);
  ring.position.copy(node);
  scene.add(ring);
  pulseEffects.push({
    mesh: ring,
    anchorStep: stepName,
    start: startTime,
    end: startTime + preset.duration,
    startScale: preset.startScale,
    endScale: preset.endScale,
    startOpacity: preset.opacity
  });
}

function spawnBurst(stepName, startTime = currentTimeSeconds, strong = false) {
  const node = nodePositions.get(stepName);
  if (!node) {
    return;
  }
  const geometry = new THREE.SphereGeometry(0.22, 18, 18);
  const material = new THREE.MeshBasicMaterial({
    color: 0xff647c,
    transparent: true,
    opacity: 0.95
  });
  const burst = new THREE.Mesh(geometry, material);
  burst.position.copy(node);
  scene.add(burst);
  burstEffects.push({
    mesh: burst,
    anchorStep: stepName,
    start: startTime,
    end: startTime + EFFECT_PRESETS.burst.duration,
    endScale: EFFECT_PRESETS.burst.endScale,
    startOpacity: EFFECT_PRESETS.burst.opacity
  });
  if (strong) {
    const ringGeometry = new THREE.RingGeometry(0.72, 0.9, 40);
    const ringMaterial = new THREE.MeshBasicMaterial({
      color: 0xff7c8f,
      transparent: true,
      opacity: EFFECT_PRESETS.shockwave.opacity,
      side: THREE.DoubleSide
    });
    const ring = new THREE.Mesh(ringGeometry, ringMaterial);
    ring.position.copy(node);
    scene.add(ring);
    pulseEffects.push({
      mesh: ring,
      anchorStep: stepName,
      start: startTime + 0.08,
      end: startTime + EFFECT_PRESETS.shockwave.duration,
      startScale: EFFECT_PRESETS.shockwave.startScale,
      endScale: EFFECT_PRESETS.shockwave.endScale,
      startOpacity: EFFECT_PRESETS.shockwave.opacity
    });
  }
}

function spawnRetryLoop(stepName, startTime = currentTimeSeconds) {
  const node = nodePositions.get(stepName);
  if (!node) {
    return;
  }
  const geometry = new THREE.SphereGeometry(0.08, 12, 12);
  const material = new THREE.MeshBasicMaterial({
    color: 0xffb454,
    transparent: true,
    opacity: 1
  });
  const orb = new THREE.Mesh(geometry, material);
  scene.add(orb);
  retryLoops.push({
    mesh: orb,
    center: node.clone(),
    anchorStep: stepName,
    radius: EFFECT_PRESETS.retryLoop.radius,
    start: startTime,
    end: startTime + EFFECT_PRESETS.retryLoop.duration,
    revolutions: EFFECT_PRESETS.retryLoop.revolutions,
    verticalDrift: EFFECT_PRESETS.retryLoop.verticalDrift,
    startOpacity: EFFECT_PRESETS.retryLoop.opacity
  });
}

function queueBackgroundFlash(color, startTime = currentTimeSeconds, duration = EFFECT_PRESETS.background.pulseDuration, intensity = 1) {
  backgroundFlashes.push({
    color,
    start: startTime,
    end: startTime + duration,
    intensity
  });
}

function resolveEventKey(event) {
  if (event.sequence != null) {
    return `seq:${event.sequence}`;
  }
  return `${event.spanId}:${event.event}:${event.itemId ?? "no-item"}:${event.startTime}`;
}

function stateForStep(stepName) {
  if (!runtimeStepState.has(stepName)) {
    runtimeStepState.set(stepName, {
      received: 0,
      sent: 0,
      inFlight: 0,
      skips: 0,
      rejects: 0,
      known: false,
      receivedKnown: true,
      sentKnown: true,
      peakPressure: 0,
      peakInFlight: 0,
      peakCounterBacklog: 0,
      activeInputKeys: new Set()
    });
  }
  return runtimeStepState.get(stepName);
}

function inputItemKeys(event) {
  if (Array.isArray(event?.parentItemIds) && event.parentItemIds.length > 0) {
    return event.parentItemIds.map((itemId) => String(itemId));
  }
  return event?.itemId ? [String(event.itemId)] : [];
}

function inputItemCount(event) {
  if (Array.isArray(event?.parentItemIds) && event.parentItemIds.length > 0) {
    return event.parentItemIds.length;
  }
  return event?.itemId ? 1 : 0;
}

function outputItemCount(event) {
  return event?.itemId ? 1 : 0;
}

function numericEventAttribute(event, key) {
  const value = event?.attributes?.[key];
  if (value == null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function displayStateForStep(stepName) {
  const directState = stateForStep(stepName);
  const step = resolveStepDefinition(stepName);
  if (!step || step.pluginKind === "reject") {
    return {
      state: directState,
      unknown: false
    };
  }
  if (step.renderRole === "await" && replayHasAwaitLifecycleEvents) {
    return {
      state: directState,
      unknown: !directState.known
    };
  }
  return {
    state: directState,
    unknown: !directState.known
  };
}

function markCounterEvidence(state, itemCount) {
  if (itemCount > 0) {
    state.known = true;
    state.receivedKnown = true;
    state.sentKnown = true;
  }
}

function markCountersKnown(state) {
  state.known = true;
  state.receivedKnown = true;
  state.sentKnown = true;
}

function formatCounterValue(value, known = true) {
  return known ? `${value}` : "—";
}

function recordReceived(state, itemCount) {
  markCounterEvidence(state, itemCount);
  if (itemCount > 0) {
    state.received += itemCount;
    updateCounterBacklogPeak(state);
  }
}

function recordSent(state, itemCount) {
  markCounterEvidence(state, itemCount);
  if (itemCount > 0) {
    state.sent += itemCount;
    updateCounterBacklogPeak(state);
  }
}

function recordInFlight(state, itemKeys, fallbackCount = 0) {
  if (itemKeys.length > 0) {
    let added = 0;
    for (const itemKey of itemKeys) {
      if (!state.activeInputKeys.has(itemKey)) {
        state.activeInputKeys.add(itemKey);
        added += 1;
      }
    }
    if (added > 0) {
      state.inFlight += added;
      updatePeakPressure(state);
    }
    return;
  }
  if (fallbackCount > 0) {
    state.inFlight += fallbackCount;
    updatePeakPressure(state);
  }
}

function releaseInFlight(state, itemKeys, fallbackCount = 0) {
  if (itemKeys.length > 0) {
    let released = 0;
    for (const itemKey of itemKeys) {
      if (state.activeInputKeys.delete(itemKey)) {
        released += 1;
      }
    }
    if (released > 0) {
      state.inFlight = Math.max(0, state.inFlight - released);
    }
    return;
  }
  if (fallbackCount > 0) {
    state.inFlight = Math.max(0, state.inFlight - fallbackCount);
  }
}

function setInFlight(state, itemCount) {
  state.inFlight = Math.max(0, itemCount);
  state.peakInFlight = Math.max(state.peakInFlight ?? 0, state.inFlight);
  state.peakPressure = Math.max(state.peakPressure ?? 0, state.inFlight);
}

function isCountedThroughputStep(stepName) {
  const step = resolveStepDefinition(stepName);
  return Boolean(step) && !step.sideEffect && step.pluginKind !== "reject";
}

function hasPrimaryInboundTransition(stepName) {
  return (replayDocument.topology?.transitions ?? []).some((transition) =>
    transition.to === stepName
      && (transition.relationKind ?? "primary") === "primary"
      && isCountedThroughputStep(transition.from)
  );
}

function shouldCountStartAsReceived(event) {
  if (!event?.step || !isCountedThroughputStep(event.step)) {
    return false;
  }
  if (!hasPrimaryInboundTransition(event.step)) {
    return true;
  }
  return stepHasRenderRole(event.from, "await");
}

function updatePeakPressure(state) {
  state.peakInFlight = Math.max(state.peakInFlight ?? 0, state.inFlight ?? 0);
  state.peakPressure = Math.max(state.peakPressure ?? 0, state.inFlight ?? 0);
}

function updateCounterBacklogPeak(state) {
  state.peakCounterBacklog = Math.max(
    state.peakCounterBacklog ?? 0,
    Math.max(0, (state.received ?? 0) - (state.sent ?? 0))
  );
}

function usesCounterDeltaPressure(stepName) {
  if (!stepName || isInternalAwaitClientStep(stepName)) {
    return false;
  }
  const displayStep = resolveStepDefinition(stepName);
  if (!displayStep || displayStep.sideEffect || displayStep.pluginKind === "reject") {
    return false;
  }
  for (const [rawStepName, displayStepName] of displayStepAliases.entries()) {
    if (displayStepName !== stepName || rawStepName === stepName || isInternalAwaitClientStep(rawStepName)) {
      continue;
    }
    const rawStep = resolveRawStepDefinition(rawStepName);
    if (rawStep && !rawStep.sideEffect && resolveDisplayRole(rawStep) === "primary") {
      return true;
    }
  }
  return false;
}

function pressureForStep(stepName, state) {
  if (!state?.receivedKnown || !state?.sentKnown) {
    return null;
  }
  if (usesCounterDeltaPressure(stepName)) {
    return Math.max(0, (state.received ?? 0) - (state.sent ?? 0));
  }
  return Math.max(0, state.inFlight ?? 0);
}

function pressureRatioForStep(stepName, state) {
  const pressure = pressureForStep(stepName, state);
  if (pressure == null) {
    return null;
  }
  const capacity = usesCounterDeltaPressure(stepName)
    ? (replayBackpressureCapacity ?? state.peakCounterBacklog)
    : (replayBackpressureCapacity ?? state.peakInFlight ?? state.peakPressure);
  if (!Number.isFinite(capacity) || capacity <= 0) {
    return pressure > 0 ? 1 : 0;
  }
  return clamp(pressure / capacity, 0, 1);
}

function pressureCapacityForStep(stepName, state) {
  const pressure = pressureForStep(stepName, state);
  if (pressure == null) {
    return null;
  }
  const capacity = usesCounterDeltaPressure(stepName)
    ? (replayBackpressureCapacity ?? state.peakCounterBacklog)
    : (replayBackpressureCapacity ?? state.peakInFlight ?? state.peakPressure);
  return Number.isFinite(capacity) && capacity > 0 ? capacity : null;
}

function tooltipForStep(stepName) {
  const step = resolveStepDefinition(stepName);
  if (!step) {
    return "";
  }
  const display = displayStateForStep(stepName);
  const state = display.state;
  const label = normalizeStepLabel(stepName);
  if (step.pluginKind === "reject") {
    return `${label}: rejected ${state.rejects}`;
  }
  if (state.skips > 0 && display.unknown) {
    return `${label}: skipped ${state.skips}; received/sent counters unknown for this replay.`;
  }
  if (display.unknown) {
    return `${label}: received/sent counters unknown for this replay.`;
  }
  const pressure = pressureForStep(stepName, state) ?? 0;
  const capacity = pressureCapacityForStep(stepName, state);
  const capacityText = capacity == null ? "observed peak" : `${capacity} capacity`;
  const skipText = state.skips > 0 ? `, skipped ${state.skips}` : "";
  return `${label}: received ${formatCounterValue(state.received, state.receivedKnown)}, sent ${formatCounterValue(state.sent, state.sentKnown)}${skipText}, queued pressure ${pressure} (${capacityText}).`;
}

function rejectStepNameFor(stepName) {
  return `Rejects ${stepName}`;
}

function recordReplayCounters(rawEvent) {
  const event = mapEventForDisplay(rawEvent);
  if (isAwaitLifecycleEvent(rawEvent)) {
    recordAwaitLifecycleCounters(rawEvent, event);
    return;
  }
  if (recordConnectorCounters(rawEvent, event)) {
    return;
  }
  if (!event?.step) {
    return;
  }
  const state = stateForStep(event.step);
  const inputCount = inputItemCount(rawEvent);
  const inputKeys = inputItemKeys(rawEvent);
  const outputCount = outputItemCount(rawEvent);
  if (isAwaitResumableError(rawEvent)) {
    return;
  }
  if (event.event === "start") {
    if (shouldCountStartAsReceived(event)) {
      recordReceived(state, inputCount);
    }
    releaseInFlight(state, inputKeys, inputCount);
    if (!replayHasAwaitLifecycleEvents && stepHasRenderRole(event.from, "await")) {
      const awaitState = stateForStep(event.from);
      recordSent(awaitState, inputCount);
      releaseInFlight(awaitState, inputKeys, inputCount);
    }
    return;
  }
  if (event.event === "emit") {
    recordSent(state, outputCount);
    if (event.to && isCountedThroughputStep(event.to)) {
      const targetState = stateForStep(event.to);
      recordReceived(targetState, outputCount);
      recordInFlight(targetState, rawEvent?.itemId ? [String(rawEvent.itemId)] : [], outputCount);
    }
    return;
  }
  if (event.event === "success" || event.event === "error") {
    releaseInFlight(state, inputKeys, inputCount);
    return;
  }
  if (event.event === "cache_hit") {
    return;
  }
  if (event.event === "skip") {
    const skipCount = Math.max(inputCount, 1);
    state.skips += skipCount;
    return;
  }
  if (event.event === "reject") {
    const rejectStepName = event.to || rejectStepNameFor(event.step);
    const rejectState = stateForStep(rejectStepName);
    markCounterEvidence(rejectState, inputCount);
    rejectState.rejects += inputCount;
    return;
  }
}

function recordConnectorCounters(rawEvent, event) {
  if (!event?.step) {
    return false;
  }
  const step = resolveStepDefinition(event.step);
  const role = resolveDisplayRole(step);
  if (role === "object-ingest") {
    const state = stateForStep(event.step);
    markCountersKnown(state);
    if (rawEvent.event === "object_ingest_listed") {
      state.received = Math.max(state.received, numericEventAttribute(rawEvent, "count") ?? 0);
      return true;
    }
    if (rawEvent.event === "object_ingest_submitted") {
      state.sent += 1;
      state.received = Math.max(state.received, state.sent);
      return true;
    }
  }
  if (role === "object-publish") {
    const state = stateForStep(event.step);
    markCountersKnown(state);
    if (rawEvent.event === "object_publish_grouped") {
      const itemCount = numericEventAttribute(rawEvent, "itemCount");
      const groupCount = numericEventAttribute(rawEvent, "groupCount");
      if (itemCount != null) {
        state.received = Math.max(state.received, itemCount);
      }
      if (groupCount != null) {
        state.sent = Math.max(state.sent, groupCount);
      }
      return true;
    }
    if (rawEvent.event === "object_publish_published") {
      state.sent = Math.max(state.sent, 1);
      return true;
    }
  }
  return false;
}

function recordAwaitLifecycleCounters(rawEvent, event) {
  const awaitStepName = awaitStepForLifecycleEvent(rawEvent, event);
  if (!awaitStepName) {
    return;
  }
  const expected = awaitLifecycleNumber(rawEvent, AWAIT_ATTR.expectedItemCount);
  const completed = awaitLifecycleNumber(rawEvent, AWAIT_ATTR.completedItemCount);
  if (expected == null && completed == null) {
    return;
  }
  const state = stateForStep(awaitStepName);
  state.known = true;
  state.receivedKnown = true;
  state.sentKnown = true;
  if (rawEvent.event === "await_interaction_dispatched") {
    if (expected != null) {
      state.received = Math.max(state.received, expected);
      setInFlight(state, Math.max(0, expected - state.sent));
    }
    return;
  }
  if (completed != null) {
    state.sent = completed;
    state.sentKnown = true;
  } else {
    state.sentKnown = false;
  }
  if (expected != null) {
    state.received = Math.max(state.received, expected);
    state.receivedKnown = true;
  }
  const receivedBaseline = expected ?? Math.max(state.received, completed ?? 0);
  const completedBaseline = completed ?? state.sent;
  setInFlight(state, Math.max(0, receivedBaseline - completedBaseline));
}

function spawnSupportTransit(fromStep, toStep, startTime, color, duration = 0.36, size = 0.085) {
  const source = nodePositions.get(fromStep);
  const target = nodePositions.get(toStep);
  if (!source || !target) {
    return;
  }
  boostEdge(fromStep, toStep, 0.82);
  const geometry = new THREE.SphereGeometry(size, 12, 12);
  const material = new THREE.MeshBasicMaterial({
    color,
    transparent: true,
    opacity: 0.92
  });
  const mesh = new THREE.Mesh(geometry, material);
  scene.add(mesh);
  particles.set(`support:${fromStep}:${toStep}:${startTime}:${Math.random()}`, {
    mesh,
    source: source.clone(),
    target: target.clone(),
    sourceStep: fromStep,
    targetStep: toStep,
    start: startTime,
    end: startTime + duration
  });
}

function shouldSampleSupportFlow(sampleKey, interval = 18) {
  const nextCount = (supportFlowSampleCounts.get(sampleKey) ?? 0) + 1;
  supportFlowSampleCounts.set(sampleKey, nextCount);
  return nextCount === 1 || nextCount % interval === 0;
}

function animateAwaitRequest(flow, timeSeconds) {
  if (!flow?.awaitStep) {
    return;
  }
  highlightStep(flow.awaitStep, 1.1, timeSeconds);
  const [firstEdge, secondEdge] = flow.requestEdges ?? [];
  if (firstEdge && nodePositions.has(firstEdge.to)) {
    highlightStep(firstEdge.to, 0.95, timeSeconds);
    spawnSupportTransit(firstEdge.from, firstEdge.to, timeSeconds, 0x8f7aea, 0.72, 0.11);
  }
  if (secondEdge && nodePositions.has(secondEdge.to)) {
    highlightStep(secondEdge.to, 0.95, timeSeconds);
    spawnSupportTransit(secondEdge.from, secondEdge.to, timeSeconds + 0.1, 0xb08cff, 0.84, 0.11);
  }
}

function animateAwaitCompletion(flow, timeSeconds, includeResume = true) {
  if (!flow?.awaitStep || !flow?.resumeEdge) {
    return;
  }
  const completionEdges = flow.completionEdges ?? [];
  if (completionEdges[0] && nodePositions.has(completionEdges[0].from)) {
    highlightStep(completionEdges[0].from, 0.85, timeSeconds);
    spawnSupportTransit(completionEdges[0].from, completionEdges[0].to, timeSeconds, 0xb08cff, 0.78, 0.11);
  }
  if (completionEdges[1] && nodePositions.has(completionEdges[1].from)) {
    highlightStep(completionEdges[1].from, 0.92, timeSeconds);
    spawnSupportTransit(completionEdges[1].from, completionEdges[1].to, timeSeconds + 0.1, 0x8f7aea, 0.86, 0.11);
  }
  highlightStep(flow.awaitStep, 1.05, timeSeconds);
  if (includeResume) {
    animateAwaitResume(flow, timeSeconds + 0.16);
  }
}

function animateAwaitResume(flow, timeSeconds) {
  if (!flow?.awaitStep || !flow?.resumeEdge) {
    return;
  }
  highlightStep(flow.awaitStep, 1.05, timeSeconds);
  highlightStep(flow.resumeEdge.to, 1.05, timeSeconds + 0.1);
  spawnSupportTransit(flow.resumeEdge.from, flow.resumeEdge.to, timeSeconds + 0.12, 0x7ad7ff, 0.96, 0.12);
}

function animateStoreWrite(flow, timeSeconds) {
  if (!flow?.fromStep || !flow?.toStep) {
    return;
  }
  highlightStep(flow.toStep, 1.05, timeSeconds);
  highlightStep(flow.fromStep, 0.9, timeSeconds);
  const burstCount = flow.aggregate ? 3 : 2;
  for (let index = 0; index < burstCount; index += 1) {
    spawnSupportTransit(
      flow.fromStep,
      flow.toStep,
      timeSeconds + index * 0.07,
      0x79dfff,
      flow.aggregate ? 1.18 + index * 0.12 : 0.94 + index * 0.1,
      flow.aggregate ? 0.13 : 0.11
    );
  }
}

function animateOutputResume(edge, timeSeconds, aggregate = false) {
  if (!edge?.from || !edge?.to) {
    return;
  }
  highlightStep(edge.from, 1.05, timeSeconds);
  highlightStep(edge.to, 1.15, timeSeconds);
  const burstCount = aggregate ? 3 : 1;
  for (let index = 0; index < burstCount; index += 1) {
    spawnSupportTransit(
      edge.from,
      edge.to,
      timeSeconds + index * 0.08,
      0x86f0cf,
      aggregate ? 1.18 + index * 0.1 : 0.72,
      aggregate ? 0.13 : 0.1
    );
  }
}

function animateDataBackedStartEdge(event) {
  if (!event?.from || !event?.step || event.from === event.step) {
    return;
  }
  if (!nodePositions.has(event.from) || !nodePositions.has(event.step)) {
    return;
  }
  if (!shouldSampleSupportFlow(`start:${event.from}->${event.step}`, 18)) {
    return;
  }
  spawnSupportTransit(event.from, event.step, event.startTime, 0x79dfff, 0.72, 0.1);
}

function animateConnectorFlow(edge, timeSeconds, color) {
  if (!edge?.from || !edge?.to) {
    return;
  }
  highlightStep(edge.from, 0.95, timeSeconds);
  highlightStep(edge.to, 0.95, timeSeconds + 0.06);
  spawnSupportTransit(edge.from, edge.to, timeSeconds + 0.04, color, 0.82, 0.11);
}

function processEvent(rawEvent) {
  const event = mapEventForDisplay(rawEvent);
  const key = resolveEventKey(rawEvent);
  if (eventIndexByKey.has(key)) {
    return;
  }
  eventIndexByKey.add(key);
  recordReplayCounters(rawEvent);
  if (isAwaitLifecycleEvent(rawEvent)) {
    processAwaitLifecycleEvent(rawEvent, event);
    return;
  }
  const awaitRequestFlow = event?.to ? activeAnimationPolicy.awaitRequestByTargetStep.get(event.to) : null;
  if (!replayHasAwaitLifecycleEvents && rawEvent.event === "emit" && awaitRequestFlow
      && shouldSampleSupportFlow(`await-request:${awaitRequestFlow.awaitStep}`)) {
    animateAwaitRequest(awaitRequestFlow, rawEvent.startTime);
  }
  const awaitCompletionFlow = event?.step ? activeAnimationPolicy.awaitCompletionByResumeStep.get(event.step) : null;
  if (!replayHasAwaitLifecycleEvents && rawEvent.event === "start" && awaitCompletionFlow && event.from === awaitCompletionFlow.awaitStep
      && shouldSampleSupportFlow(`await-completion:${awaitCompletionFlow.awaitStep}`)) {
    animateAwaitCompletion(awaitCompletionFlow, rawEvent.startTime);
  }
  const connectorIngestEdge = event?.step ? activeAnimationPolicy.connectorIngestByTargetStep.get(event.step) : null;
  if (rawEvent.event === "start" && connectorIngestEdge
      && shouldSampleSupportFlow(`object-ingest:${connectorIngestEdge.from}->${connectorIngestEdge.to}`, 12)) {
    animateConnectorFlow(connectorIngestEdge, rawEvent.startTime, 0x8bffa5);
  }
  const queryConnectorEdge = event?.step ? activeAnimationPolicy.queryConnectorByTargetStep.get(event.step) : null;
  if (rawEvent.event === "start" && queryConnectorEdge
      && shouldSampleSupportFlow(`query:${queryConnectorEdge.from}->${queryConnectorEdge.to}`, 16)) {
    animateConnectorFlow(queryConnectorEdge, rawEvent.startTime + 0.04, 0x9bd6ff);
  }
  if (rawEvent.event === "object_ingest_listed" || rawEvent.event === "object_ingest_submitted") {
    const ingestEdge = activeAnimationPolicy.connectorIngestByTargetStep.get(event.step)
      ?? [...activeAnimationPolicy.connectorIngestByTargetStep.values()].find((edge) => edge.from === event.step);
    const sampleKey = ingestEdge
      ? `object-ingest:${ingestEdge.from}->${ingestEdge.to}`
      : `object-ingest:${event.step}`;
    if (!shouldSampleSupportFlow(sampleKey, 12)) {
      return;
    }
    if (ingestEdge) {
      animateConnectorFlow(ingestEdge, rawEvent.startTime, 0x8bffa5);
    } else {
      highlightStep(event.step, 1.1, rawEvent.startTime);
      spawnPulse(event.step, 0x8bffa5, EFFECT_PRESETS.pulse.success, rawEvent.startTime);
    }
    return;
  }
  const objectPublishEdge = event?.step ? activeAnimationPolicy.connectorPublishBySourceStep.get(event.step) : null;
  if (rawEvent.event === "success" && objectPublishEdge
      && shouldSampleSupportFlow(`object-publish:${objectPublishEdge.from}->${objectPublishEdge.to}`, 12)) {
    animateConnectorFlow(objectPublishEdge, rawEvent.endTime ?? rawEvent.startTime, 0x86f0cf);
  }
  if (rawEvent.event === "object_publish_grouped" || rawEvent.event === "object_publish_published") {
    const publishEdge = [...activeAnimationPolicy.connectorPublishBySourceStep.values()]
      .find((edge) => edge.to === event.step);
    const sampleKey = publishEdge
      ? `object-publish:${publishEdge.from}->${publishEdge.to}`
      : `object-publish:${event.step}`;
    if (!shouldSampleSupportFlow(sampleKey, 12)) {
      return;
    }
    if (publishEdge) {
      animateConnectorFlow(publishEdge, rawEvent.startTime, 0x86f0cf);
    } else {
      highlightStep(event.step, 1.1, rawEvent.startTime);
      spawnPulse(event.step, 0x86f0cf, EFFECT_PRESETS.pulse.success, rawEvent.startTime);
    }
    return;
  }
  const storeWriteFlow = activeAnimationPolicy.storeWriteByRawStep.get(rawEvent.step);
  if (rawEvent.event === "success" && storeWriteFlow && isPersistenceSideEffectStep(rawEvent.step)
      && (!storeWriteFlow.aggregate || shouldSampleSupportFlow(`store:${storeWriteFlow.fromStep}->${storeWriteFlow.toStep}`, 24))) {
    animateStoreWrite(storeWriteFlow, rawEvent.endTime ?? rawEvent.startTime);
  }
  const outputResumeEdge = event?.step ? activeAnimationPolicy.outputResumeByTargetStep.get(event.step) : null;
  if (rawEvent.event === "start" && outputResumeEdge && event.from === outputResumeEdge.from) {
    animateOutputResume(outputResumeEdge, rawEvent.startTime, inputItemCount(rawEvent) > 1);
  }
  const displayTargets = resolveDisplayTargets(event);
  if (isAwaitResumableError(rawEvent)) {
    const suspensionTime = Number.isFinite(event.endTime)
      ? event.endTime
      : (Number.isFinite(event.startTime) ? event.startTime : 0);
    const awaitStepName = awaitDisplayStepForEvent(event);
    if (awaitStepName) {
      const resumeTime = nextAwaitResumeTimeAfter(suspensionTime);
      const holdSeconds = resumeTime
        ? Math.max(EFFECT_PRESETS.node.defaultHoldSeconds + 0.5, resumeTime - suspensionTime)
        : EFFECT_PRESETS.node.defaultHoldSeconds + 0.5;
      highlightStep(awaitStepName, holdSeconds, suspensionTime);
      spawnPulse(awaitStepName, 0x8f7aea, {
        duration: Math.min(holdSeconds, 7),
        startScale: 1,
        endScale: 1.95,
        opacity: 0.58
      }, suspensionTime);
      queueBackgroundFlash("#8f7aea", suspensionTime, 0.8, 0.24);
      if (rawEvent.itemId) {
        itemAnchors.set(rawEvent.itemId, awaitStepName);
      }
    }
    return;
  }
  highlightStep(event.step, EFFECT_PRESETS.node.defaultHoldSeconds, event.startTime);
  highlightStep(event.from, 0.9, event.startTime);
  highlightStep(event.to, 0.9, event.startTime);
  if (event.event === "start") {
    animateDataBackedStartEdge(event);
    spawnPulse(event.step, 0x6ce2ff, EFFECT_PRESETS.pulse.start, event.startTime);
    itemAnchors.set(event.itemId, event.step);
  } else if (event.event === "retry") {
    highlightStep(displayTargets.primaryStep, EFFECT_PRESETS.node.retryHoldSeconds, event.startTime);
    spawnPulse(displayTargets.primaryStep, 0xffc164, EFFECT_PRESETS.pulse.retryPrimary, event.startTime);
    spawnRetryLoop(displayTargets.primaryStep, event.startTime);
    queueBackgroundFlash("#ffb454", event.startTime, 1.05, 0.68);
    if (displayTargets.secondaryStep) {
      highlightStep(displayTargets.secondaryStep, 1.2, event.startTime);
      spawnPulse(displayTargets.secondaryStep, 0xffb454, EFFECT_PRESETS.pulse.retrySecondary, event.startTime + 0.04);
      boostEdge(event.from, displayTargets.secondaryStep, 0.58);
    }
    if (event.from) {
      boostEdge(event.from, displayTargets.primaryStep, 1.12);
    }
  } else if (event.event === "error") {
    spawnTransit(event);
    highlightStep(displayTargets.primaryStep, EFFECT_PRESETS.node.errorHoldSeconds, event.startTime);
    spawnPulse(displayTargets.primaryStep, 0xff7488, EFFECT_PRESETS.pulse.errorPrimary, event.startTime);
    spawnBurst(displayTargets.primaryStep, event.startTime, true);
    queueBackgroundFlash("#ff647c", event.startTime, 1.35, 1);
    if (displayTargets.secondaryStep) {
      highlightStep(displayTargets.secondaryStep, 1.2, event.startTime);
      spawnPulse(displayTargets.secondaryStep, 0xff8ca3, EFFECT_PRESETS.pulse.errorSecondary, event.startTime + 0.05);
      boostEdge(event.from, displayTargets.secondaryStep, 0.62);
    }
    itemAnchors.set(event.itemId, event.step);
  } else if (event.event === "success") {
    spawnTransit(event);
    spawnPulse(event.step, 0x79f2c6, EFFECT_PRESETS.pulse.success, event.endTime);
    itemAnchors.set(event.itemId, event.step);
  } else if (event.event === "cache_hit") {
    spawnTransit(Object.assign({}, event, {
      from: event.from || resolvePluginStepForDisplay(event.step, "cache"),
      step: event.to || event.step
    }));
    highlightStep(event.step, EFFECT_PRESETS.node.defaultHoldSeconds + 0.4, event.startTime);
    spawnPulse(event.step, 0x8bffa5, EFFECT_PRESETS.pulse.success, event.endTime);
    queueBackgroundFlash("#8bffa5", event.startTime, 0.85, 0.42);
    itemAnchors.set(event.itemId, event.step);
  } else if (event.event === "reject") {
    spawnTransit(event);
    highlightStep(displayTargets.primaryStep, EFFECT_PRESETS.node.errorHoldSeconds, event.startTime);
    if (event.to) {
      highlightStep(event.to, EFFECT_PRESETS.node.errorHoldSeconds, event.startTime);
      spawnPulse(event.to, 0xff7c8f, EFFECT_PRESETS.pulse.errorSecondary, event.startTime);
    }
    spawnPulse(displayTargets.primaryStep, 0xff7c8f, EFFECT_PRESETS.pulse.errorPrimary, event.startTime);
    queueBackgroundFlash("#ff647c", event.startTime, 0.95, 0.58);
    itemAnchors.set(event.itemId, event.to || event.step);
  } else if (event.event === "skip") {
    highlightStep(event.step, EFFECT_PRESETS.node.defaultHoldSeconds + 0.35, event.startTime);
    spawnPulse(event.step, 0x9fb3ff, {
      duration: 0.78,
      startScale: 0.98,
      endScale: 1.92,
      opacity: 0.6
    }, event.startTime);
    queueBackgroundFlash("#9fb3ff", event.startTime, 0.72, 0.28);
    itemAnchors.set(event.itemId, event.from || event.step);
  } else if (event.event === "emit") {
    spawnEmitSpark(event);
    itemAnchors.set(event.itemId, event.to || event.step);
  }
}

function processAwaitLifecycleEvent(rawEvent, event) {
  const timeSeconds = playbackTimeForEvent(rawEvent);
  const awaitStepName = awaitStepForLifecycleEvent(rawEvent, event);
  if (!awaitStepName) {
    return;
  }
  const requestFlow = activeAnimationPolicy.awaitRequestByAwaitStep.get(awaitStepName);
  const completionFlow = activeAnimationPolicy.awaitCompletionByAwaitStep.get(awaitStepName);
  if (rawEvent.event === "await_interaction_dispatched") {
    if (requestFlow && shouldSampleSupportFlow(`await-request:${awaitStepName}`)) {
      animateAwaitRequest(requestFlow, timeSeconds);
    } else {
      highlightStep(awaitStepName, 0.9, timeSeconds);
    }
    itemAnchors.set(rawEvent.itemId ?? awaitLifecycleAttribute(rawEvent, AWAIT_ATTR.unitId), awaitStepName);
    return;
  }
  if (rawEvent.event === "await_unit_item_completed" || rawEvent.event === "await_unit_completed") {
    if (completionFlow && shouldSampleSupportFlow(`await-completion:${awaitStepName}`)) {
      animateAwaitCompletion(completionFlow, timeSeconds, false);
    } else {
      highlightStep(awaitStepName, 0.9, timeSeconds);
    }
    itemAnchors.set(rawEvent.itemId ?? awaitLifecycleAttribute(rawEvent, AWAIT_ATTR.unitId), awaitStepName);
    return;
  }
  if (rawEvent.event === "await_resume_released") {
    if (completionFlow) {
      animateAwaitResume(completionFlow, timeSeconds);
    } else {
      highlightStep(awaitStepName, 1.05, timeSeconds);
    }
    itemAnchors.set(rawEvent.itemId ?? awaitLifecycleAttribute(rawEvent, AWAIT_ATTR.unitId), awaitStepName);
    return;
  }
  if (rawEvent.event === "await_unit_terminal") {
    highlightStep(awaitStepName, EFFECT_PRESETS.node.errorHoldSeconds, timeSeconds);
    spawnPulse(awaitStepName, 0xff7488, EFFECT_PRESETS.pulse.errorPrimary, timeSeconds);
    queueBackgroundFlash("#ff647c", timeSeconds, 0.95, 0.58);
    return;
  }
  highlightStep(awaitStepName, EFFECT_PRESETS.node.defaultHoldSeconds + 0.5, timeSeconds);
  spawnPulse(awaitStepName, 0x8f7aea, EFFECT_PRESETS.pulse.start, timeSeconds);
}

function processEventsUntil(timeSeconds) {
  while (nextEventCursor < replayDocument.events.length) {
    const event = replayDocument.events[nextEventCursor];
    if (playbackTimeForEvent(event) > timeSeconds) {
      break;
    }
    if (isAwaitSuspensionStartEvent(event, replayDocument.events[nextEventCursor + 1])) {
      nextEventCursor += 1;
      continue;
    }
    processEvent(event);
    nextEventCursor += 1;
  }
}

function rebuildPlaybackTo(timeSeconds) {
  currentTimeSeconds = clamp(timeSeconds, 0, replayDurationSeconds);
  resetPlaybackState();
  processEventsUntil(currentTimeSeconds);
  updateParticles(currentTimeSeconds);
  updateEffects(currentTimeSeconds);
  updateEdges(currentTimeSeconds);
  updateNodes(currentTimeSeconds);
  updateBackgroundEffects(currentTimeSeconds);
  updateLabels();
  updateUi();
}

function rebuildPlaybackToCursor(targetCursor) {
  const safeCursor = clamp(targetCursor, 0, replayDocument.events.length);
  resetPlaybackState();
  currentTimeSeconds = 0;
  for (let index = 0; index < safeCursor; index += 1) {
    const event = replayDocument.events[index];
    nextEventCursor = index + 1;
    if (isAwaitSuspensionStartEvent(event, replayDocument.events[index + 1])) {
      continue;
    }
    currentTimeSeconds = Math.max(currentTimeSeconds, playbackTimeForEvent(event));
    processEvent(event);
  }
  updateParticles(currentTimeSeconds);
  updateEffects(currentTimeSeconds);
  updateEdges(currentTimeSeconds);
  updateNodes(currentTimeSeconds);
  updateBackgroundEffects(currentTimeSeconds);
  updateLabels();
  updateUi();
}

function updateParticles(timeSeconds) {
  for (const [key, particle] of [...particles.entries()]) {
    const duration = Math.max(0.001, particle.end - particle.start);
    const progress = Math.min(1, Math.max(0, (timeSeconds - particle.start) / duration));
    const source = particle.sourceStep ? nodePositions.get(particle.sourceStep) ?? particle.source : particle.source;
    const target = particle.targetStep ? nodePositions.get(particle.targetStep) ?? particle.target : particle.target;
    particle.mesh.position.lerpVectors(source, target, progress);
    if (timeSeconds >= particle.end) {
      removeAndDispose(particle.mesh);
      particles.delete(key);
    }
  }
}

function updateEffects(timeSeconds) {
  for (const effect of [...pulseEffects]) {
    const anchor = effect.anchorStep ? nodePositions.get(effect.anchorStep) : null;
    if (anchor) {
      effect.mesh.position.copy(anchor);
    }
    const progress = Math.min(1, Math.max(0, (timeSeconds - effect.start) / (effect.end - effect.start)));
    const scale = effect.startScale + (effect.endScale - effect.startScale) * progress;
    effect.mesh.scale.setScalar(scale);
    effect.mesh.material.opacity = effect.startOpacity * (1 - progress);
    if (timeSeconds >= effect.end) {
      removeAndDispose(effect.mesh);
      pulseEffects.splice(pulseEffects.indexOf(effect), 1);
    }
  }
  for (const effect of [...burstEffects]) {
    const anchor = effect.anchorStep ? nodePositions.get(effect.anchorStep) : null;
    if (anchor) {
      effect.mesh.position.copy(anchor);
    }
    const progress = Math.min(1, Math.max(0, (timeSeconds - effect.start) / (effect.end - effect.start)));
    effect.mesh.scale.setScalar(1 + progress * effect.endScale);
    effect.mesh.material.opacity = effect.startOpacity * Math.max(0, 1 - progress * 1.25);
    if (timeSeconds >= effect.end) {
      removeAndDispose(effect.mesh);
      burstEffects.splice(burstEffects.indexOf(effect), 1);
    }
  }
  for (const effect of [...retryLoops]) {
    const anchor = effect.anchorStep ? nodePositions.get(effect.anchorStep) : null;
    if (anchor) {
      effect.center.copy(anchor);
    }
    const progress = Math.min(1, Math.max(0, (timeSeconds - effect.start) / (effect.end - effect.start)));
    const angle = progress * Math.PI * effect.revolutions;
    effect.mesh.position.set(
      effect.center.x + Math.cos(angle) * effect.radius,
      effect.center.y + Math.sin(angle) * effect.radius,
      effect.center.z + Math.sin(angle * 0.5) * effect.verticalDrift
    );
    effect.mesh.material.opacity = Math.max(0.28, effect.startOpacity - progress * 0.32);
    if (timeSeconds >= effect.end) {
      removeAndDispose(effect.mesh);
      retryLoops.splice(retryLoops.indexOf(effect), 1);
    }
  }
}

function updateEdges(timeSeconds) {
  for (const edge of edgeLines.values()) {
    edge.intensity *= EFFECT_PRESETS.edge.decay;
    const shimmer = (Math.sin(timeSeconds * 1.35 + edge.shimmerPhase) + 1) * 0.5 * EFFECT_PRESETS.edge.shimmerAmplitude;
    edge.line.material.opacity = edge.baseOpacity + shimmer + edge.intensity * 0.38;
    edge.line.material.color.setHex(edge.intensity > 0.02 ? edge.accentColor : edge.baseColor);
  }
}

function updateNodes(timeSeconds) {
  nodeMeshes.forEach((mesh, stepName) => {
    const highlightedUntil = highlightExpirations.get(stepName) ?? 0;
    const highlighted = highlightedUntil > timeSeconds;
    const pulse = mesh.userData.isSideEffect ? 0.012 : EFFECT_PRESETS.node.breathScaleAmplitude;
    const phase = mesh.userData.phase ?? 0;
    const breath = Math.sin(timeSeconds * 2.2 + phase);
    const baseScale = 1 + breath * pulse;
    const highlightScale = highlighted ? EFFECT_PRESETS.node.highlightScaleBoost : 0;
    mesh.scale.setScalar(baseScale + highlightScale);
    const baseEmissive = mesh.userData.isSideEffect ? 0.34 : 0.34 + ((breath + 1) * 0.5) * EFFECT_PRESETS.node.breathEmissiveAmplitude;
    mesh.material.emissiveIntensity = baseEmissive + (highlighted ? 0.3 : 0);
  });

  for (const [stepName, sprite] of nodeValueSprites.entries()) {
    const step = resolveStepDefinition(stepName);
    const display = displayStateForStep(stepName);
    const state = display.state;
    const expectedText = step?.pluginKind === "reject"
      ? `${state.rejects}`
      : display.unknown
        ? "—|—"
        : `${formatCounterValue(state.received, state.receivedKnown)}|${formatCounterValue(state.sent, state.sentKnown)}`;
    const pressureRatio = step?.pluginKind === "reject" || display.unknown ? null : pressureRatioForStep(stepName, state);
    if (sprite.userData.text !== expectedText) {
      if (step?.pluginKind === "reject") {
        replaceSpriteText(sprite, expectedText, {
          fontSize: 26,
          fontWeight: 700,
          fillStyle: "#f8fbff",
          paddingX: 6,
          paddingY: 4,
          backgroundStyle: "rgba(7, 16, 31, 0.0)",
          borderStyle: null,
          height: 0.22
        });
      } else {
        replaceThroughputCounterSprite(sprite, expectedText);
      }
      sprite.userData.text = expectedText;
    }
    updatePressureRing(stepName, pressureRatio);
  }
}

function updateBackgroundEffects(timeSeconds) {
  let strongest = null;
  for (const flash of [...backgroundFlashes]) {
    if (timeSeconds >= flash.end) {
      backgroundFlashes.splice(backgroundFlashes.indexOf(flash), 1);
      continue;
    }
    const progress = Math.min(1, Math.max(0, (timeSeconds - flash.start) / (flash.end - flash.start)));
    const intensity = Math.sin(progress * Math.PI) * flash.intensity;
    if (!strongest || intensity > strongest.intensity) {
      strongest = { color: flash.color, intensity };
    }
  }
  const driftPhase = timeSeconds * EFFECT_PRESETS.background.idleDriftSpeed;
  const idle = 0.18 + (Math.sin(driftPhase) + 1) * 0.5 * EFFECT_PRESETS.background.idleDriftAmplitude;
  setViewportBackground(idle + (strongest?.intensity ?? 0) * 0.32, strongest?.color ?? "#5cc8ff", driftPhase);
}

function updateLabels() {
  nodeLabelSprites.forEach((sprite, stepName) => {
    const highlighted = (highlightExpirations.get(stepName) ?? 0) > currentTimeSeconds;
    sprite.material.opacity = highlighted || hoveredStepName === stepName ? 1 : 0.92;
  });
}

function hasActiveTransientEffects() {
  return (
    particles.size > 0 ||
    pulseEffects.length > 0 ||
    burstEffects.length > 0 ||
    retryLoops.length > 0 ||
    backgroundFlashes.length > 0
  );
}

function updateUi() {
  const playbackSeconds = Math.min(currentTimeSeconds, replayDurationSeconds);
  const ratio = replayDurationSeconds <= 0 ? 0 : playbackSeconds / replayDurationSeconds;
  timelineSlider.value = `${Math.round(ratio * 10000)}`;
  playbackText.textContent = `${playbackSeconds.toFixed(2)}s / ${replayDurationSeconds.toFixed(2)}s`;
  playPauseButton.dataset.playing = isPlaying ? "true" : "false";
  playPauseButton.setAttribute("aria-label", isPlaying ? "Pause replay" : "Play replay");
  playPauseButton.title = isPlaying ? "Pause replay" : "Play replay";
  stopButton.disabled = isLoadingReplay || (nextEventCursor === 0 && currentTimeSeconds <= 0);
  stepBackButton.disabled = isLoadingReplay || nextEventCursor <= 0;
  stepButton.disabled = isLoadingReplay || nextEventCursor >= replayDocument.events.length;
  playPauseButton.disabled = isLoadingReplay;
  restartButton.disabled = isLoadingReplay;
  if (resetLayoutButton) {
    resetLayoutButton.disabled = isLoadingReplay || automaticNodePositions.size === 0;
  }
  speedInputs.forEach((input) => {
    input.checked = Number(input.value) === playbackSpeed;
    input.disabled = isLoadingReplay;
  });
}

function restartPlayback() {
  isFinishingEffects = false;
  rebuildPlaybackTo(0);
}

function stepBackwardOneEvent() {
  isFinishingEffects = false;
  if (nextEventCursor <= 0) {
    rebuildPlaybackToCursor(0);
    return;
  }
  isPlaying = false;
  rebuildPlaybackToCursor(nextEventCursor - 1);
}

function stepForwardOneEvent() {
  if (nextEventCursor >= replayDocument.events.length) {
    return;
  }
  isPlaying = false;
  isFinishingEffects = false;
  while (nextEventCursor < replayDocument.events.length
      && isAwaitSuspensionStartEvent(replayDocument.events[nextEventCursor], replayDocument.events[nextEventCursor + 1])) {
    nextEventCursor += 1;
  }
  if (nextEventCursor >= replayDocument.events.length) {
    updateUi();
    return;
  }
  const nextEvent = replayDocument.events[nextEventCursor];
  currentTimeSeconds = playbackTimeForEvent(nextEvent);
  processEvent(nextEvent);
  nextEventCursor += 1;
  updateParticles(currentTimeSeconds);
  updateEffects(currentTimeSeconds);
  updateEdges(currentTimeSeconds);
  updateNodes(currentTimeSeconds);
  updateBackgroundEffects(currentTimeSeconds);
  updateLabels();
  updateUi();
}

function updateHoveredStep(clientX, clientY) {
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x = ((clientX - rect.left) / rect.width) * 2 - 1;
  pointer.y = -((clientY - rect.top) / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  const intersections = raycaster.intersectObjects([...nodeMeshes.values()], false);
  hoveredStepName = intersections[0]?.object?.userData?.step?.step ?? null;
  renderer.domElement.style.cursor = hoveredStepName ? "grab" : "default";
  renderer.domElement.title = hoveredStepName ? tooltipForStep(hoveredStepName) : "";
}

function worldPointForPointer(clientX, clientY) {
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x = ((clientX - rect.left) / rect.width) * 2 - 1;
  pointer.y = -((clientY - rect.top) / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  const plane = new THREE.Plane(new THREE.Vector3(0, 0, 1), 0);
  const point = new THREE.Vector3();
  return raycaster.ray.intersectPlane(plane, point) ? point : null;
}

function nodeAtPointer(clientX, clientY) {
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x = ((clientX - rect.left) / rect.width) * 2 - 1;
  pointer.y = -((clientY - rect.top) / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  return raycaster.intersectObjects([...nodeMeshes.values()], false)[0]?.object ?? null;
}

function beginNodeDrag(event) {
  if (event.button !== 0 || isLoadingReplay || isAnyModalOpen()) {
    return false;
  }
  const mesh = nodeAtPointer(event.clientX, event.clientY);
  const stepName = mesh?.userData?.step?.step;
  const hitPoint = worldPointForPointer(event.clientX, event.clientY);
  const position = stepName ? nodePositions.get(stepName) : null;
  if (!stepName || !hitPoint || !position) {
    return false;
  }
  dragState = {
    stepName,
    offset: position.clone().sub(hitPoint),
    moved: false
  };
  hoveredStepName = stepName;
  cancelChromeHide();
  renderer.domElement.setPointerCapture(event.pointerId);
  renderer.domElement.style.cursor = "grabbing";
  event.preventDefault();
  return true;
}

function updateNodeDrag(event) {
  if (!dragState) {
    return false;
  }
  const hitPoint = worldPointForPointer(event.clientX, event.clientY);
  if (!hitPoint) {
    return true;
  }
  const nextPosition = hitPoint.add(dragState.offset);
  nextPosition.z = 0;
  setNodePosition(dragState.stepName, nextPosition);
  dragState.moved = true;
  event.preventDefault();
  return true;
}

function endNodeDrag(event) {
  if (!dragState) {
    return false;
  }
  const moved = dragState.moved;
  saveCurrentLayoutPositions();
  dragState = null;
  suppressNextCanvasClick = moved;
  try {
    renderer.domElement.releasePointerCapture(event.pointerId);
  } catch (_error) {
    // Pointer capture may already be gone after browser-level cancellation.
  }
  renderer.domElement.style.cursor = hoveredStepName ? "grab" : "default";
  if (!prefersTapChrome) {
    scheduleChromeHide();
  }
  event.preventDefault();
  return moved;
}

function tick(now) {
  try {
    const deltaSeconds = (now - previousAnimationFrame) / 1000;
    previousAnimationFrame = now;

    if (isPlaying) {
      if (isFinishingEffects) {
        currentTimeSeconds += deltaSeconds;
      } else {
        const nextPlaybackTime = currentTimeSeconds + deltaSeconds * playbackSpeed;
        if (nextPlaybackTime >= replayDurationSeconds) {
          currentTimeSeconds = replayDurationSeconds;
          isFinishingEffects = true;
        } else {
          currentTimeSeconds = nextPlaybackTime;
        }
      }
      if (currentTimeSeconds >= replayDurationSeconds) {
        isFinishingEffects = true;
      }
    }

    processEventsUntil(Math.min(currentTimeSeconds, replayDurationSeconds));
    updateParticles(currentTimeSeconds);
    updateEffects(currentTimeSeconds);
    updateEdges(currentTimeSeconds);
    updateNodes(currentTimeSeconds);
    updateBackgroundEffects(currentTimeSeconds);
    updateLabels();
    if (isPlaying && isFinishingEffects
        && (!hasActiveTransientEffects() || currentTimeSeconds >= replayDurationSeconds + END_DRAIN_SECONDS)) {
      isPlaying = false;
      isFinishingEffects = false;
      currentTimeSeconds = replayDurationSeconds;
      scheduleCompletionPrompt();
    }
    updateUi();
    renderer.render(scene, camera);
  } catch (error) {
    reportRuntimeError("playback paused after a rendering error", error);
    fatalRenderErrorLatched = true;
  }
  if (!fatalRenderErrorLatched) {
    requestAnimationFrame(tick);
  }
}

playPauseButton.addEventListener("click", () => {
  try {
    if (isLoadingReplay) {
      return;
    }
    cancelCompletionPrompt();
    revealPlayerChrome();
    const shouldStart = !isPlaying;
    if (shouldStart && currentTimeSeconds >= replayDurationSeconds) {
      rebuildPlaybackTo(0);
      completionPromptShownForPlayback = false;
    }
    previousAnimationFrame = performance.now();
    isFinishingEffects = false;
    isPlaying = shouldStart;
    scheduleChromeHide();
    updateUi();
  } catch (error) {
    reportRuntimeError("playback could not start", error);
  }
});

loadReplayButton.addEventListener("click", () => {
  cancelCompletionPrompt();
  openSourceModal("load");
});

restartButton.addEventListener("click", () => {
  resetCompletionPromptForPlayback();
  revealPlayerChrome(true);
  restartPlayback();
  scheduleChromeHide();
});

stopButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  resetCompletionPromptForPlayback();
  revealPlayerChrome(true);
  isPlaying = false;
  restartPlayback();
  scheduleChromeHide();
});

stepBackButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  resetCompletionPromptForPlayback();
  revealPlayerChrome(true);
  stepBackwardOneEvent();
  scheduleChromeHide();
});

stepButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  resetCompletionPromptForPlayback();
  revealPlayerChrome(true);
  stepForwardOneEvent();
  scheduleChromeHide();
});

timelineSlider.addEventListener("input", () => {
  resetCompletionPromptForPlayback();
  revealPlayerChrome(true);
  isScrubbingTimeline = true;
  const nextTime = (Number(timelineSlider.value) / 10000) * replayDurationSeconds;
  isPlaying = false;
  isFinishingEffects = false;
  rebuildPlaybackTo(nextTime);
});

timelineSlider.addEventListener("pointerdown", () => {
  cancelCompletionPrompt();
  revealPlayerChrome(true);
  isScrubbingTimeline = true;
});

timelineSlider.addEventListener("pointerup", () => {
  isScrubbingTimeline = false;
  scheduleChromeHide();
});

timelineSlider.addEventListener("change", () => {
  isScrubbingTimeline = false;
  scheduleChromeHide();
});

speedInputs.forEach((input) => {
  input.addEventListener("change", () => {
    if (!input.checked) {
      return;
    }
    playbackSpeed = Number(input.value);
    revealPlayerChrome(true);
    scheduleChromeHide();
    updateUi();
  });
});

datasetSelect.addEventListener("change", () => {
  stagedReplaySourceKey = datasetSelect.value;
  setCustomReplayVisibility(stagedReplaySourceKey === "custom");
  updateSourceApplyButton();
  revealPlayerChrome(true);
});

replayFileInput.addEventListener("change", async (event) => {
  stagedReplaySourceKey = datasetSelect.value;
  updateSourceApplyButton();
  revealPlayerChrome(true);
});

infoButton.addEventListener("click", () => {
  openModalElement("info", infoModal);
});

infoCloseButton.addEventListener("click", () => {
  closeModalElement("info", infoModal);
});

sourceCloseButton.addEventListener("click", () => {
  closeModalElement("source", sourceModal);
});

sourceCancelButton.addEventListener("click", () => {
  closeModalElement("source", sourceModal);
});

sourceApplyButton.addEventListener("click", async () => {
  await applySelectedReplaySource();
});

fullscreenButton.addEventListener("click", async () => {
  try {
    if (document.fullscreenElement === playerSurface) {
      await document.exitFullscreen();
    } else {
      await playerSurface.requestFullscreen();
    }
  } catch (error) {
    reportViewerIssue(`fullscreen unavailable (${error.message})`);
  }
});

document.addEventListener("fullscreenchange", () => {
  const isFullscreen = document.fullscreenElement === playerSurface;
  fullscreenButton.dataset.fullscreen = isFullscreen ? "true" : "false";
  fullscreenButton.setAttribute("aria-label", isFullscreen ? "Exit full screen" : "Full screen");
  fullscreenButton.title = isFullscreen ? "Exit full screen" : "Full screen";
});

renderer.domElement.addEventListener("pointermove", (event) => {
  if (updateNodeDrag(event)) {
    return;
  }
  updateHoveredStep(event.clientX, event.clientY);
  if (!prefersTapChrome) {
    revealPlayerChrome();
  }
});

renderer.domElement.addEventListener("pointerdown", (event) => {
  beginNodeDrag(event);
});

renderer.domElement.addEventListener("pointerup", (event) => {
  endNodeDrag(event);
});

renderer.domElement.addEventListener("pointercancel", (event) => {
  endNodeDrag(event);
});

renderer.domElement.addEventListener("pointerleave", () => {
  if (dragState) {
    return;
  }
  hoveredStepName = null;
  renderer.domElement.style.cursor = "default";
});

renderer.domElement.addEventListener("click", (event) => {
  if (suppressNextCanvasClick) {
    suppressNextCanvasClick = false;
    event.preventDefault();
    return;
  }
  if (dragState) {
    return;
  }
  if (!prefersTapChrome) {
    revealPlayerChrome();
    event.preventDefault();
    return;
  }
  if (playerChrome.dataset.visible === "true") {
    setPlayerChromeVisible(false);
    cancelChromeHide();
  } else {
    revealPlayerChrome();
  }
});

if (resetLayoutButton) {
  resetLayoutButton.addEventListener("click", () => {
    resetCurrentLayout();
    revealPlayerChrome(true);
    scheduleChromeHide();
    updateUi();
  });
}

playerChrome.addEventListener("pointermove", () => {
  revealPlayerChrome();
});

playerChrome.addEventListener("pointerleave", () => {
  if (!isAnyModalOpen()) {
    scheduleChromeHide();
  }
});

playerChrome.addEventListener("click", (event) => {
  event.stopPropagation();
  revealPlayerChrome();
});

window.addEventListener("keydown", (event) => {
  if (event.key === "Tab" && !isAnyModalOpen()) {
    revealPlayerChrome();
  }
});

window.addEventListener("resize", () => {
  renderer.setSize(mount.clientWidth, mount.clientHeight);
  camera.aspect = mount.clientWidth / mount.clientHeight;
  camera.updateProjectionMatrix();
  fitCameraToTopology(replayDocument.topology.steps);
  updateLabels();
  prefersTapChrome = window.matchMedia("(hover: none), (pointer: coarse)").matches;
  if (isAnyModalOpen() || isLoadingReplay || isScrubbingTimeline) {
    setPlayerChromeVisible(true);
    cancelChromeHide();
  } else if (playerChrome.dataset.visible === "true") {
    scheduleChromeHide();
  }
});

for (const modal of [infoModal, sourceModal]) {
  modal.addEventListener("click", (event) => {
    const closeTarget = event.target.closest("[data-close-modal]");
    if (closeTarget) {
      const name = closeTarget.getAttribute("data-close-modal");
      if (name === "info") {
        closeModalElement("info", infoModal);
      } else if (name === "source") {
        closeModalElement("source", sourceModal);
      }
    }
  });
}

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (openModal === "info") {
      closeModalElement("info", infoModal);
    } else if (openModal === "source") {
      closeModalElement("source", sourceModal);
    }
  }
});

window.addEventListener("popstate", () => {
  reloadIfViewerShellRouteChanged();
});

window.addEventListener("pageshow", () => {
  if (reloadIfViewerShellRouteChanged()) {
    return;
  }
  if (!infoModal.hidden) {
    infoModal.hidden = true;
    infoModal.setAttribute("aria-hidden", "true");
  }
  if (!sourceModal.hidden) {
    sourceModal.hidden = true;
    sourceModal.setAttribute("aria-hidden", "true");
  }
  openModal = null;
  modalReturnFocusElement = null;
  if (appShell) {
    appShell.inert = false;
  }
  cancelCompletionPrompt();
  syncStagedReplaySourceToActive();
  if (isLoadingReplay || isScrubbingTimeline) {
    revealPlayerChrome(true);
  } else {
    setPlayerChromeVisible(false);
    cancelChromeHide();
  }
});

window.addEventListener("error", (event) => {
  reportNonFatalRuntimeIssue("viewer error", event.error ?? event.message);
});

window.addEventListener("unhandledrejection", (event) => {
  reportNonFatalRuntimeIssue("viewer promise error", event.reason);
});

setCustomReplayVisibility(false);
updateSourceApplyButton();
renderRunParameters(replayDocument.runParameters);
if (backToDocsLink) {
  backToDocsLink.href = resolveReplayDocsHref();
  backToDocsLink.addEventListener("click", (event) => {
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    ) {
      return;
    }
    event.preventDefault();
    window.location.assign(backToDocsLink.href);
  });
}
setPlayerChromeVisible(false);
updateUi();
requestAnimationFrame(tick);
// Intentionally starts empty so large built-in datasets load only after an explicit picker selection.
if (DEFAULT_REPLAY_SOURCE_KEY !== EMPTY_REPLAY_SOURCE_KEY) {
  loadBuiltInReplay(DEFAULT_REPLAY_SOURCE_KEY).catch((error) => {
    setLoadProgress(false);
    reportViewerIssue(`failed to load built-in dataset (${error.message})`);
  });
} else {
  loadFirstVisitReplayAfterPaint();
}
