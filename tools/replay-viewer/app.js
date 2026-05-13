import * as THREE from "./vendor/three.module.min.js";

const mount = document.getElementById("threeMount");
const viewport = document.querySelector(".viewport");
const playerSurface = document.getElementById("playerSurface");
const playerChrome = document.getElementById("playerChrome");
const backToDocsLink = document.getElementById("backToDocsLink");
const playPauseButton = document.getElementById("playPauseButton");
const stopButton = document.getElementById("stopButton");
const restartButton = document.getElementById("restartButton");
const stepBackButton = document.getElementById("stepBackButton");
const stepButton = document.getElementById("stepButton");
const sourceButton = document.getElementById("sourceButton");
const infoButton = document.getElementById("infoButton");
const sourceModal = document.getElementById("sourceModal");
const infoModal = document.getElementById("infoModal");
const sourceCloseButton = document.getElementById("sourceCloseButton");
const infoCloseButton = document.getElementById("infoCloseButton");
const fullscreenButton = document.getElementById("fullscreenButton");
const datasetSelect = document.getElementById("datasetSelect");
const sourceApplyButton = document.getElementById("sourceApplyButton");
const customReplayInputWrap = document.getElementById("customReplayInputWrap");
const replayFileInput = document.getElementById("replayFileInput");
const speedInputs = [...document.querySelectorAll('input[name="speed"]')];
const timelineSlider = document.getElementById("timelineSlider");
const datasetName = document.getElementById("datasetName");
const pipelineName = document.getElementById("pipelineName");
const durationText = document.getElementById("durationText");
const topologyText = document.getElementById("topologyText");
const playbackText = document.getElementById("playbackText");
const eventCountText = document.getElementById("eventCountText");
const loadProgress = document.getElementById("loadProgress");
const loadProgressFill = document.getElementById("loadProgressFill");
const loadProgressText = document.getElementById("loadProgressText");
const runParametersContent = document.getElementById("runParametersContent");
const CAMERA_PADDING = 1.2;
const BASE_NODE_RADIUS = 0.68;
const BRANCH_NODE_RADIUS = 0.6;
const BASE_LABEL_HEIGHT = 0.8;
const LABEL_OFFSET_Y = 1.12;
const COUNTER_LABEL_HEIGHT = 0.44;
const PRIMARY_ROW_Y = 2.45;
const BRANCH_ROW_OFFSET_Y = 2.05;
const BUILT_IN_REPLAYS = new Map([
  ["csv-payments", { label: "CSV Payments built-in", path: "./datasets/csv-payments-built-in.json" }],
  ["search-warm-cache", { label: "Search built-in pre-warm", path: "./datasets/search-built-in-pre-warm.json" }],
  ["search-cache-hit", { label: "Search built-in", path: "./datasets/search-built-in.json" }]
]);
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
const CHROME_HIDE_DELAY_MS = 1800;

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
const nodeLabelSprites = new Map();
const nodeValueSprites = new Map();
const nodeIconSprites = new Map();
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
const executionsWithEmit = new Set();
const processedResultKeys = new Set();

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
let activeReplaySourceKey = "csv-payments";
let stagedReplaySourceKey = activeReplaySourceKey;
let chromeHideTimeout = null;
let openModal = null;
let isScrubbingTimeline = false;
let prefersTapChrome = window.matchMedia("(hover: none), (pointer: coarse)").matches;

function resolveReplayDocsHref() {
  const replayDocsPath = "/guide/operations/observability/replay";
  const currentPath = window.location.pathname;
  if (currentPath.includes("/replay-viewer/")) {
    return `${window.location.origin}${replayDocsPath}`;
  }
  return `https://pipelineframework.org${replayDocsPath}`;
}

function reloadIfViewerShellRouteChanged() {
  if (!window.location.pathname.includes("/replay-viewer/")) {
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
  sourceApplyButton.disabled = nextSourceKey === "custom" && !hasPendingCustomReplay;
}

function isAnyModalOpen() {
  return openModal !== null;
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
  const topology = augmentTopologyWithDisplayNodes(
    document.topology,
    events.some((event) => event.event === "reject")
  );
  return {
    ...document,
    topology,
    events
  };
}

function playbackTimeForEvent(event) {
  if (!event) {
    return 0;
  }
  return event.event === "success" || event.event === "error"
    ? (event.endTime ?? event.startTime)
    : event.startTime;
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

  return {
    ...topology,
    steps,
    transitions
  };
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

function setLoadProgress(visible, ratio = 0, text = "Loading replay...") {
  isLoadingReplay = visible;
  loadProgress.hidden = !visible;
  loadProgressFill.style.width = `${Math.round(clamp(ratio, 0, 1) * 100)}%`;
  loadProgressText.textContent = text;
  if (visible) {
    revealPlayerChrome(true);
  } else if (!isAnyModalOpen()) {
    scheduleChromeHide();
  }
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

function resolveDisplayStepName(event) {
  if (!event || (event.event !== "retry" && event.event !== "error")) {
    return event?.step ?? null;
  }
  const step = resolveStepDefinition(event.step);
  if (step?.sideEffect && step.parentStep) {
    return step.parentStep;
  }
  return event.step;
}

function resolveDisplayTargets(event) {
  const primaryStep = resolveDisplayStepName(event);
  return {
    primaryStep,
    secondaryStep: primaryStep && primaryStep !== event.step ? event.step : null
  };
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
  for (const sprite of nodeIconSprites.values()) {
    removeAndDispose(sprite);
  }
  nodeIconSprites.clear();
  for (const mesh of nodeMeshes.values()) {
    removeAndDispose(mesh);
  }
  nodeMeshes.clear();
  nodePositions.clear();
  for (const particle of particles.values()) {
    removeAndDispose(particle.mesh);
  }
  particles.clear();
  pulseEffects.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  burstEffects.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  retryLoops.splice(0).forEach((effect) => removeAndDispose(effect.mesh));
  backgroundFlashes.splice(0);
  runtimeStepState.clear();
  executionsWithEmit.clear();
  processedResultKeys.clear();
  nodeValueUpdateTick += 1;
}

function clearReplayDynamics() {
  eventIndexByKey.clear();
  itemAnchors.clear();
  highlightExpirations.clear();
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
  executionsWithEmit.clear();
  processedResultKeys.clear();
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
    if (prefersTapChrome && !isPlaying) {
      return;
    }
    setPlayerChromeVisible(false);
  }, delayMs);
}

function revealPlayerChrome(sticky = false) {
  setPlayerChromeVisible(true);
  if (!sticky) {
    scheduleChromeHide();
  } else {
    cancelChromeHide();
  }
}

function openModalElement(name, element) {
  if (name === "source") {
    syncStagedReplaySourceToActive();
  }
  openModal = name;
  element.hidden = false;
  element.setAttribute("aria-hidden", "false");
  revealPlayerChrome(true);
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
  revealPlayerChrome(!prefersTapChrome);
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
  if (sections.length === 0) {
    const empty = document.createElement("div");
    empty.className = "run-parameters-empty";
    empty.textContent = "Run parameters unavailable";
    runParametersContent.appendChild(empty);
    return;
  }

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

  if (!runParametersContent.childElementCount) {
    const empty = document.createElement("div");
    empty.className = "run-parameters-empty";
    empty.textContent = "Run parameters unavailable";
    runParametersContent.appendChild(empty);
  }
}

function reportViewerIssue(message) {
  eventCountText.textContent = `Status: ${message}`;
}

function loadReplay(document, label, sourceKey = activeReplaySourceKey) {
  replayDocument = normalizeReplayDocument(validateReplayDocument(document));
  replayDurationSeconds = Math.max(0.1, (replayDocument.durationMs || 0) / 1000);
  currentTimeSeconds = 0;
  isPlaying = false;
  isFinishingEffects = false;
  clearScene();
  buildTopology(replayDocument.topology);
  activeReplaySourceKey = sourceKey;
  stagedReplaySourceKey = sourceKey;
  datasetSelect.value = sourceKey;
  setCustomReplayVisibility(sourceKey === "custom");
  updateSourceApplyButton();
  datasetName.textContent = label;
  pipelineName.textContent = `Pipeline: ${replayDocument.pipeline}`;
  durationText.textContent = `Duration: ${replayDurationSeconds.toFixed(2)}s`;
  topologyText.textContent = `Topology: ${replayDocument.topology.steps.length} nodes / ${replayDocument.topology.transitions.length} edges`;
  eventCountText.textContent = `Events: ${replayDocument.events.length}`;
  renderRunParameters(replayDocument.runParameters);
  resetPlaybackState();
  revealPlayerChrome(true);
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
  const response = await fetch(dataset.path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to fetch built-in dataset (${response.status})`);
  }
  setLoadProgress(true, 0.55, `Parsing ${dataset.label}...`);
  const document = await response.json();
  loadReplay(document, dataset.label, datasetKey);
  setLoadProgress(false);
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

function buildTopology(topology) {
  const steps = Array.isArray(topology.steps) ? topology.steps : [];
  const transitions = Array.isArray(topology.transitions) ? topology.transitions : [];
  steps.forEach((step) => {
    stepMetadataByName.set(step.step, step);
  });
  const baseSteps = steps.filter((step) => !step.sideEffect);
  const sideEffects = steps.filter((step) => step.sideEffect);
  const sideEffectsByParent = new Map();

  for (const step of sideEffects) {
    const parentStep = step.parentStep || "__unattached__";
    if (!sideEffectsByParent.has(parentStep)) {
      sideEffectsByParent.set(parentStep, []);
    }
    sideEffectsByParent.get(parentStep).push(step);
  }

  const spacing = baseSteps.length > 4 ? 3.5 : 4.35;
  const totalWidth = Math.max(0, (baseSteps.length - 1) * spacing);
  baseSteps.forEach((step, index) => {
    const x = index * spacing - totalWidth / 2;
    registerNode(step, new THREE.Vector3(x, PRIMARY_ROW_Y, 0));
  });

  const detached = sideEffectsByParent.get("__unattached__") || [];
  for (const step of sideEffects) {
    const parentPosition = step.parentStep ? nodePositions.get(step.parentStep) : null;
    if (!parentPosition) {
      continue;
    }
    const siblings = sideEffectsByParent.get(step.parentStep) || [];
    const siblingIndex = siblings.findIndex((candidate) => candidate.step === step.step);
    const band = Math.floor(siblingIndex / 2);
    const centeredIndex = siblingIndex - (siblings.length - 1) / 2;
    const offsetX = centeredIndex * 1.28;
    const offsetY = BRANCH_ROW_OFFSET_Y + band * 1;
    registerNode(step, new THREE.Vector3(parentPosition.x + offsetX, parentPosition.y - offsetY, 0));
  }

  detached.forEach((step, index) => {
    const x = totalWidth / 2 + 3.2;
    const y = PRIMARY_ROW_Y - BRANCH_ROW_OFFSET_Y - 0.5 - index * 1.1;
    registerNode(step, new THREE.Vector3(x, y, 0));
  });

  transitions.forEach((transition) => {
    const source = nodePositions.get(transition.from);
    const target = nodePositions.get(transition.to);
    if (!source || !target) {
      return;
    }
    const isBranch = steps.find((step) => step.step === transition.to)?.sideEffect === true;
    const material = isBranch
      ? new THREE.LineDashedMaterial({ color: 0x8f7aea, transparent: true, opacity: 0.55, dashSize: 0.32, gapSize: 0.18 })
      : new THREE.LineBasicMaterial({ color: 0x34527f, transparent: true, opacity: 0.8 });
    const geometry = new THREE.BufferGeometry().setFromPoints([source.clone(), target.clone()]);
    const line = new THREE.Line(geometry, material);
    if (isBranch) {
      line.computeLineDistances();
    }
    scene.add(line);
    edgeLines.set(edgeKey(transition.from, transition.to), {
      line,
      baseColor: isBranch ? 0x8f7aea : 0x34527f,
      accentColor: isBranch ? 0xcf9cff : 0x7ad7ff,
      baseOpacity: isBranch ? 0.55 : 0.8,
      intensity: 0,
      shimmerPhase: Math.random() * Math.PI * 2,
      branch: isBranch
    });
  });

  fitCameraToTopology(steps);
}

function registerNode(step, position) {
  const isSideEffect = Boolean(step.sideEffect);
  const geometry = new THREE.SphereGeometry(isSideEffect ? BRANCH_NODE_RADIUS : BASE_NODE_RADIUS, 24, 24);
  const material = new THREE.MeshStandardMaterial({
    color: nodeColorForStep(step),
    emissive: isSideEffect ? 0x35254a : 0x17355b,
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
  if (!isSideEffect) {
    const sprite = buildStepLabelSprite(step.step);
    sprite.position.copy(position).add(new THREE.Vector3(0, -LABEL_OFFSET_Y, 0));
    scene.add(sprite);
    nodeLabelSprites.set(step.step, sprite);
    const valueSprite = buildTextSprite("0|0", {
      fontSize: 28,
      fontWeight: 700,
      fillStyle: "#f8fbff",
      paddingX: 12,
      paddingY: 8,
      backgroundStyle: "rgba(7, 16, 31, 0.0)",
      borderStyle: null,
      height: COUNTER_LABEL_HEIGHT
    });
    valueSprite.position.copy(position);
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
  return buildIconSprite(step.pluginKind, step.pluginKind === "reject" ? 0.54 : 0.72);
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
      drawDatabaseIcon(context);
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
  const withoutPrefix = stepName.replace(/^Process/, "");
  return withoutPrefix
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim();
}

function nodeColorForStep(step) {
  if (!step?.sideEffect) {
    return 0x5cc8ff;
  }
  switch (step.pluginKind) {
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
    const labelHeight = step.sideEffect ? 0 : BASE_LABEL_HEIGHT;
    const valueHeight = step.sideEffect ? (step.pluginKind === "reject" ? 0.24 : 0.08) : COUNTER_LABEL_HEIGHT * 0.5;
    const labelWidth = step.sideEffect ? 0 : (nodeLabelSprites.get(step.step)?.userData?.aspect ?? 1) * BASE_LABEL_HEIGHT * 0.5;
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
  const centerY = (minY + maxY) / 2 + height * 0.16;
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

function resolveSourcePositions(event) {
  if (event.cardinality === "many-to-one" && Array.isArray(event.parentItemIds) && event.parentItemIds.length > 1) {
    const sources = event.parentItemIds
      .map((itemId) => itemAnchors.get(itemId))
      .map((stepName) => nodePositions.get(stepName))
      .filter(Boolean);
    if (sources.length > 0) {
      return sources;
    }
  }
  const anchoredSource = event.from ? nodePositions.get(event.from) : null;
  return anchoredSource ? [anchoredSource] : [];
}

function spawnTransit(event) {
  const toNode = nodePositions.get(event.step);
  if (!toNode) {
    return;
  }
  const sources = resolveSourcePositions(event);
  const eventEndTime = event.endTime ?? event.startTime;
  const duration = Math.max(0.12, eventEndTime - event.startTime);
  const color = particleColorForEvent(event.event);
  if (event.from && event.step) {
    boostEdge(event.from, event.step, event.event === "error" ? 1.1 : 0.78);
  }
  (sources.length > 0 ? sources : [toNode]).forEach((source, index) => {
    const geometry = new THREE.SphereGeometry(0.13, 16, 16);
    const material = new THREE.MeshStandardMaterial({
      color,
      emissive: color,
      emissiveIntensity: 0.45
    });
    const mesh = new THREE.Mesh(geometry, material);
    scene.add(mesh);
    particles.set(`${event.sequence}:${event.event}:${index}`, {
      mesh,
      source: source.clone(),
      target: toNode.clone(),
      start: event.startTime,
      end: event.startTime + duration
    });
  });
}

function spawnEmitSpark(event) {
  const source = nodePositions.get(event.from) || nodePositions.get(event.step);
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
    const material = new THREE.MeshStandardMaterial({
      color: 0x8bffa5,
      emissive: 0x8bffa5,
      emissiveIntensity: index === 0 ? 0.65 : 0.45
    });
    const mesh = new THREE.Mesh(geometry, material);
    scene.add(mesh);
    particles.set(`${event.sequence}:emit:${index}`, {
      mesh,
      source: source.clone(),
      target: target.clone(),
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
  const material = new THREE.MeshStandardMaterial({
    color: 0xffb454,
    emissive: 0xffb454,
    emissiveIntensity: 0.8,
    transparent: true,
    opacity: 1
  });
  const orb = new THREE.Mesh(geometry, material);
  scene.add(orb);
  retryLoops.push({
    mesh: orb,
    center: node.clone(),
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
    runtimeStepState.set(stepName, { inflight: 0, processed: 0, rejects: 0 });
  }
  return runtimeStepState.get(stepName);
}

function executionKey(stepName, spanId) {
  return `${stepName}:${spanId ?? "no-span"}`;
}

function processedResultKey(event) {
  if (event.itemId) {
    return `${event.step}:item:${event.itemId}`;
  }
  if (event.spanId) {
    return `${event.step}:span:${event.spanId}`;
  }
  return `${event.step}:seq:${event.sequence ?? event.startTime}`;
}

function rejectStepNameFor(stepName) {
  return `Rejects ${stepName}`;
}

function recordReplayCounters(event) {
  if (!event?.step) {
    return;
  }
  const state = stateForStep(event.step);
  if (event.event === "start") {
    state.inflight += 1;
    return;
  }
  if (event.event === "emit") {
    executionsWithEmit.add(executionKey(event.step, event.spanId));
    const key = processedResultKey(event);
    if (!processedResultKeys.has(key)) {
      processedResultKeys.add(key);
      state.processed += 1;
    }
    return;
  }
  if (event.event === "cache_hit") {
    const key = processedResultKey(event);
    if (!processedResultKeys.has(key)) {
      processedResultKeys.add(key);
      state.processed += 1;
    }
    return;
  }
  if (event.event === "reject") {
    const rejectStepName = event.to || rejectStepNameFor(event.step);
    const rejectState = stateForStep(rejectStepName);
    rejectState.rejects += 1;
    return;
  }
  if (event.event === "success" || event.event === "error") {
    state.inflight = Math.max(0, state.inflight - 1);
    if (event.event === "success" && !executionsWithEmit.has(executionKey(event.step, event.spanId))) {
      const key = processedResultKey(event);
      if (!processedResultKeys.has(key)) {
        processedResultKeys.add(key);
        state.processed += 1;
      }
    }
  }
}

function processEvent(event) {
  const key = resolveEventKey(event);
  if (eventIndexByKey.has(key)) {
    return;
  }
  eventIndexByKey.add(key);
  recordReplayCounters(event);
  const displayTargets = resolveDisplayTargets(event);
  highlightStep(event.step, EFFECT_PRESETS.node.defaultHoldSeconds, event.startTime);
  highlightStep(event.from, 0.9, event.startTime);
  highlightStep(event.to, 0.9, event.startTime);
  if (event.event === "start") {
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
    spawnTransit({
      ...event,
      from: event.from || resolvePluginStepForDisplay(event.step, "cache"),
      step: event.to || event.step
    });
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
  } else if (event.event === "emit") {
    spawnEmitSpark(event);
    itemAnchors.set(event.itemId, event.to || event.step);
  }
}

function processEventsUntil(timeSeconds) {
  while (nextEventCursor < replayDocument.events.length) {
    const event = replayDocument.events[nextEventCursor];
    if (playbackTimeForEvent(event) > timeSeconds) {
      break;
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
    currentTimeSeconds = Math.max(currentTimeSeconds, playbackTimeForEvent(event));
    processEvent(event);
    nextEventCursor = index + 1;
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
    particle.mesh.position.lerpVectors(particle.source, particle.target, progress);
    if (timeSeconds >= particle.end) {
      removeAndDispose(particle.mesh);
      particles.delete(key);
    }
  }
}

function updateEffects(timeSeconds) {
  for (const effect of [...pulseEffects]) {
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
    const progress = Math.min(1, Math.max(0, (timeSeconds - effect.start) / (effect.end - effect.start)));
    effect.mesh.scale.setScalar(1 + progress * effect.endScale);
    effect.mesh.material.opacity = effect.startOpacity * Math.max(0, 1 - progress * 1.25);
    if (timeSeconds >= effect.end) {
      removeAndDispose(effect.mesh);
      burstEffects.splice(burstEffects.indexOf(effect), 1);
    }
  }
  for (const effect of [...retryLoops]) {
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
    const state = stateForStep(stepName);
    const expectedText = step?.pluginKind === "reject"
      ? `${state.rejects}`
      : `${state.inflight}|${state.processed}`;
    if (sprite.userData.text !== expectedText) {
      replaceSpriteText(sprite, expectedText, step?.pluginKind === "reject" ? {
        fontSize: 26,
        fontWeight: 700,
        fillStyle: "#f8fbff",
        paddingX: 6,
        paddingY: 4,
        backgroundStyle: "rgba(7, 16, 31, 0.0)",
        borderStyle: null,
        height: 0.22
      } : {
        fontSize: 28,
        fontWeight: 700,
        fillStyle: "#f8fbff",
        paddingX: 12,
        paddingY: 8,
        backgroundStyle: "rgba(7, 16, 31, 0.0)",
        borderStyle: null,
        height: COUNTER_LABEL_HEIGHT
      });
      sprite.userData.text = expectedText;
    }
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
  timelineSlider.value = `${Math.round(ratio * 1000)}`;
  playbackText.textContent = `${playbackSeconds.toFixed(2)}s / ${replayDurationSeconds.toFixed(2)}s`;
  playPauseButton.textContent = isPlaying ? "⏸" : "▶";
  stopButton.disabled = isLoadingReplay || (nextEventCursor === 0 && currentTimeSeconds <= 0);
  stepBackButton.disabled = isLoadingReplay || nextEventCursor <= 0;
  stepButton.disabled = isLoadingReplay || nextEventCursor >= replayDocument.events.length;
  playPauseButton.disabled = isLoadingReplay;
  restartButton.disabled = isLoadingReplay;
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
}

function tick(now) {
  const deltaSeconds = (now - previousAnimationFrame) / 1000;
  previousAnimationFrame = now;

  if (isPlaying) {
    currentTimeSeconds += deltaSeconds * playbackSpeed;
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
  if (isPlaying && currentTimeSeconds >= replayDurationSeconds && !hasActiveTransientEffects()) {
    isPlaying = false;
    isFinishingEffects = false;
    currentTimeSeconds = replayDurationSeconds;
  }
  updateUi();
  renderer.render(scene, camera);
  requestAnimationFrame(tick);
}

playPauseButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  revealPlayerChrome(true);
  if (!isPlaying && currentTimeSeconds >= replayDurationSeconds) {
    rebuildPlaybackTo(0);
  }
  if (!isPlaying && nextEventCursor === 0 && replayDocument.events.length > 0) {
    rebuildPlaybackToCursor(1);
  }
  previousAnimationFrame = performance.now();
  isFinishingEffects = false;
  isPlaying = !isPlaying;
  scheduleChromeHide();
  updateUi();
});

restartButton.addEventListener("click", () => {
  revealPlayerChrome(true);
  restartPlayback();
  scheduleChromeHide();
});

stopButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  revealPlayerChrome(true);
  isPlaying = false;
  restartPlayback();
  if (!prefersTapChrome) {
    scheduleChromeHide();
  }
});

stepBackButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  revealPlayerChrome(true);
  stepBackwardOneEvent();
  scheduleChromeHide();
});

stepButton.addEventListener("click", () => {
  if (isLoadingReplay) {
    return;
  }
  revealPlayerChrome(true);
  stepForwardOneEvent();
  scheduleChromeHide();
});

timelineSlider.addEventListener("input", () => {
  revealPlayerChrome(true);
  isScrubbingTimeline = true;
  const nextTime = (Number(timelineSlider.value) / 1000) * replayDurationSeconds;
  isPlaying = false;
  isFinishingEffects = false;
  rebuildPlaybackTo(nextTime);
});

timelineSlider.addEventListener("pointerdown", () => {
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

sourceButton.addEventListener("click", () => {
  openModalElement("source", sourceModal);
});

infoButton.addEventListener("click", () => {
  openModalElement("info", infoModal);
});

sourceCloseButton.addEventListener("click", () => {
  closeModalElement("source", sourceModal);
});

infoCloseButton.addEventListener("click", () => {
  closeModalElement("info", infoModal);
});

sourceApplyButton.addEventListener("click", async () => {
  if (isLoadingReplay) {
    return;
  }
  const nextSource = datasetSelect.value;
  try {
    if (nextSource === "custom") {
      const [file] = replayFileInput.files || [];
      if (!file) {
        return;
      }
      isPlaying = false;
      setLoadProgress(true, 0, `Loading ${file.name}...`);
      updateUi();
      const text = await readReplayFile(file);
      setLoadProgress(true, 1, `Parsing ${file.name}...`);
      await new Promise((resolve) => requestAnimationFrame(resolve));
      const parsed = JSON.parse(text);
      loadReplay(parsed, file.name, "custom");
    } else {
      await loadBuiltInReplay(nextSource);
    }
    setLoadProgress(false);
    closeModalElement("source", sourceModal);
    scheduleChromeHide();
    updateUi();
  } catch (error) {
    setLoadProgress(false);
    reportViewerIssue(`failed to load replay (${error.message})`);
    playPauseButton.disabled = false;
    restartButton.disabled = false;
    stepBackButton.disabled = false;
    stepButton.disabled = false;
  }
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
  fullscreenButton.textContent = document.fullscreenElement === playerSurface
    ? "🡼"
    : "⛶";
});

renderer.domElement.addEventListener("pointermove", (event) => {
  updateHoveredStep(event.clientX, event.clientY);
  if (!prefersTapChrome) {
    revealPlayerChrome();
  }
});

renderer.domElement.addEventListener("pointerleave", () => {
  hoveredStepName = null;
  if (!prefersTapChrome && !isAnyModalOpen()) {
    scheduleChromeHide(900);
  }
});

renderer.domElement.addEventListener("click", () => {
  if (!prefersTapChrome) {
    revealPlayerChrome();
    return;
  }
  if (playerChrome.dataset.visible === "true") {
    setPlayerChromeVisible(false);
    cancelChromeHide();
  } else {
    revealPlayerChrome();
  }
});

playerChrome.addEventListener("pointermove", () => {
  revealPlayerChrome(true);
});

playerChrome.addEventListener("pointerleave", () => {
  if (!prefersTapChrome && !isAnyModalOpen()) {
    scheduleChromeHide(900);
  }
});

playerChrome.addEventListener("click", (event) => {
  event.stopPropagation();
  revealPlayerChrome(true);
});

window.addEventListener("resize", () => {
  renderer.setSize(mount.clientWidth, mount.clientHeight);
  camera.aspect = mount.clientWidth / mount.clientHeight;
  camera.updateProjectionMatrix();
  fitCameraToTopology(replayDocument.topology.steps);
  updateLabels();
  prefersTapChrome = window.matchMedia("(hover: none), (pointer: coarse)").matches;
  setPlayerChromeVisible(prefersTapChrome || isAnyModalOpen());
});

for (const modal of [sourceModal, infoModal]) {
  modal.addEventListener("click", (event) => {
    const closeTarget = event.target.closest("[data-close-modal]");
    if (closeTarget) {
      const name = closeTarget.getAttribute("data-close-modal");
      if (name === "source") {
        closeModalElement("source", sourceModal);
      } else if (name === "info") {
        closeModalElement("info", infoModal);
      }
    }
  });
}

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (openModal === "source") {
      closeModalElement("source", sourceModal);
    } else if (openModal === "info") {
      closeModalElement("info", infoModal);
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
  if (!sourceModal.hidden) {
    sourceModal.hidden = true;
    sourceModal.setAttribute("aria-hidden", "true");
  }
  if (!infoModal.hidden) {
    infoModal.hidden = true;
    infoModal.setAttribute("aria-hidden", "true");
  }
  openModal = null;
  syncStagedReplaySourceToActive();
  revealPlayerChrome(prefersTapChrome || isLoadingReplay);
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
setPlayerChromeVisible(true);
updateUi();
requestAnimationFrame(tick);
loadBuiltInReplay("csv-payments").catch((error) => {
  setLoadProgress(false);
  reportViewerIssue(`failed to load built-in dataset (${error.message})`);
});
