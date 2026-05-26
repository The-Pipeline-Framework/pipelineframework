import * as THREE from "../replay-viewer/vendor/three.module.min.js";

const root = document.getElementById("captureRoot");
const url = new URL(window.location.href);
const dataPath = url.searchParams.get("data");
const autoPlay = url.searchParams.get("capture") !== "1";

if (!dataPath) {
  root.textContent = "Missing required replay cinematic data path.";
  throw new Error("Missing required query parameter: data");
}

const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, preserveDrawingBuffer: true });
renderer.setPixelRatio(1);
renderer.setSize(root.clientWidth, root.clientHeight, false);
renderer.outputColorSpace = THREE.SRGBColorSpace;
root.appendChild(renderer.domElement);

const scene = new THREE.Scene();
scene.fog = new THREE.FogExp2(0x06101d, 0.05);
const camera = new THREE.PerspectiveCamera(34, root.clientWidth / root.clientHeight, 0.1, 100);

const dynamicPulses = [];
const dynamicHighlights = [];
const nodeGroups = new Map();
const edgeCurves = new Map();
let cinematicData = null;
let lastAutoFrame = performance.now();
let autoProgress = 0;

const palette = {
  folder: { base: 0x4a82ff, glow: 0x8ac5ff },
  input: { base: 0x39bdf6, glow: 0x94f4ff },
  await: { base: 0x7ca8ff, glow: 0xbdd0ff },
  status: { base: 0x52d6aa, glow: 0x96ffd8 },
  output: { base: 0x69d8ff, glow: 0xa7f4ff },
  broker: { base: 0x8f78ff, glow: 0xbba7ff },
  provider: { base: 0xc299ff, glow: 0xe0c6ff },
  store: { base: 0x49c0ff, glow: 0x92ebff }
};

function roundedRectShape(width, height, radius) {
  const shape = new THREE.Shape();
  const x = -width / 2;
  const y = -height / 2;
  shape.moveTo(x + radius, y);
  shape.lineTo(x + width - radius, y);
  shape.quadraticCurveTo(x + width, y, x + width, y + radius);
  shape.lineTo(x + width, y + height - radius);
  shape.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
  shape.lineTo(x + radius, y + height);
  shape.quadraticCurveTo(x, y + height, x, y + height - radius);
  shape.lineTo(x, y + radius);
  shape.quadraticCurveTo(x, y, x + radius, y);
  return shape;
}

function makeLayeredMaterial(baseHex, glowHex, opacity = 1) {
  return new THREE.MeshStandardMaterial({
    color: baseHex,
    emissive: glowHex,
    emissiveIntensity: 0.56,
    roughness: 0.3,
    metalness: 0.24,
    transparent: opacity < 1,
    opacity
  });
}

function makeGlowSprite(colorHex, size = 1) {
  const canvas = document.createElement("canvas");
  canvas.width = 256;
  canvas.height = 256;
  const context = canvas.getContext("2d");
  const gradient = context.createRadialGradient(128, 128, 8, 128, 128, 112);
  const color = new THREE.Color(colorHex);
  gradient.addColorStop(0, `rgba(${Math.round(color.r * 255)}, ${Math.round(color.g * 255)}, ${Math.round(color.b * 255)}, 0.95)`);
  gradient.addColorStop(0.4, `rgba(${Math.round(color.r * 255)}, ${Math.round(color.g * 255)}, ${Math.round(color.b * 255)}, 0.26)`);
  gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
  context.fillStyle = gradient;
  context.fillRect(0, 0, 256, 256);
  const texture = new THREE.CanvasTexture(canvas);
  const material = new THREE.SpriteMaterial({ map: texture, transparent: true, depthWrite: false, color: 0xffffff });
  const sprite = new THREE.Sprite(material);
  sprite.scale.set(size, size, 1);
  return sprite;
}

function makeBackdrop() {
  const panelMaterial = new THREE.MeshBasicMaterial({ color: 0x10213a, transparent: true, opacity: 0.2, depthWrite: false });
  const accentMaterial = new THREE.MeshBasicMaterial({ color: 0x234b85, transparent: true, opacity: 0.12, depthWrite: false });
  const floor = new THREE.Mesh(new THREE.PlaneGeometry(30, 18), panelMaterial);
  floor.position.set(0.5, -4.15, -6);
  floor.rotation.x = -Math.PI / 2.9;
  scene.add(floor);

  const backWall = new THREE.Mesh(new THREE.PlaneGeometry(30, 18), panelMaterial);
  backWall.position.set(0, 6.35, -12);
  scene.add(backWall);

  const leftFin = new THREE.Mesh(new THREE.PlaneGeometry(8, 18), accentMaterial);
  leftFin.position.set(-10.8, 2.15, -6.5);
  leftFin.rotation.y = 0.55;
  scene.add(leftFin);

  const rightFin = new THREE.Mesh(new THREE.PlaneGeometry(9, 18), accentMaterial);
  rightFin.position.set(11.2, 1.95, -6.8);
  rightFin.rotation.y = -0.6;
  scene.add(rightFin);

  const bridgeGlow = new THREE.Mesh(new THREE.PlaneGeometry(12, 0.7), new THREE.MeshBasicMaterial({
    color: 0x5db6ff, transparent: true, opacity: 0.14, depthWrite: false
  }));
  bridgeGlow.position.set(0.25, 5.9, -5.1);
  scene.add(bridgeGlow);

  const haze = makeGlowSprite(0x2c5ca8, 24);
  haze.position.set(0, 3.05, -9);
  scene.add(haze);
}

function addLights() {
  scene.add(new THREE.AmbientLight(0xc7deff, 1.25));
  const key = new THREE.DirectionalLight(0x9cc7ff);
  key.position.set(-6, 8, 8);
  scene.add(key);
  key.intensity = 2.4;
  const rim = new THREE.DirectionalLight(0x5fd9ff, 1.65);
  rim.position.set(7, 4, 6);
  scene.add(rim);
  const fill = new THREE.PointLight(0x7f94ff, 22, 40, 2);
  fill.position.set(0, -2.6, 3);
  scene.add(fill);
}

function createPrimaryNode(role, scale) {
  const colors = palette[role];
  const group = new THREE.Group();
  const slab = new THREE.Mesh(
    new THREE.ExtrudeGeometry(roundedRectShape(1.7, 1.02, 0.28), { depth: 0.32, bevelEnabled: false }),
    makeLayeredMaterial(colors.base, colors.glow)
  );
  slab.position.z = -0.16;
  group.add(slab);

  const glowPlate = new THREE.Mesh(
    new THREE.PlaneGeometry(1.65, 0.95),
    new THREE.MeshBasicMaterial({ color: colors.glow, transparent: true, opacity: 0.24, depthWrite: false })
  );
  glowPlate.position.set(0, 0, 0.19);
  group.add(glowPlate);

  const halo = makeGlowSprite(colors.glow, 2.65);
  halo.position.set(0, 0, -0.1);
  group.add(halo);

  if (role === "folder") {
    const tab = new THREE.Mesh(new THREE.BoxGeometry(0.52, 0.34, 0.22), makeLayeredMaterial(colors.glow, colors.glow));
    tab.position.set(-0.36, 0.27, 0.13);
    group.add(tab);
  } else if (role === "input") {
    [-0.42, -0.12, 0.18].forEach((offset, index) => {
      const bar = new THREE.Mesh(
        new THREE.BoxGeometry(0.12, 0.46 + index * 0.1, 0.1),
        makeLayeredMaterial(colors.glow, colors.glow)
      );
      bar.position.set(offset, 0.02, 0.18);
      group.add(bar);
    });
  } else if (role === "await") {
    const ring = new THREE.Mesh(
      new THREE.TorusGeometry(0.54, 0.09, 18, 48),
      makeLayeredMaterial(colors.glow, colors.glow)
    );
    ring.rotation.x = Math.PI / 2;
    ring.position.set(0, 0, 0.16);
    group.add(ring);
  } else if (role === "status") {
    const disc = new THREE.Mesh(
      new THREE.CylinderGeometry(0.5, 0.5, 0.1, 40),
      makeLayeredMaterial(colors.glow, colors.glow)
    );
    disc.rotation.x = Math.PI / 2;
    disc.position.set(0, 0, 0.19);
    group.add(disc);
    const ring = new THREE.Mesh(
      new THREE.TorusGeometry(0.42, 0.03, 12, 40),
      new THREE.MeshBasicMaterial({ color: colors.glow, transparent: true, opacity: 0.75 })
    );
    ring.rotation.x = Math.PI / 2;
    ring.position.set(0, 0, 0.24);
    group.add(ring);
  } else if (role === "output") {
    const slit = new THREE.Mesh(
      new THREE.PlaneGeometry(0.84, 0.1),
      new THREE.MeshBasicMaterial({ color: colors.glow, transparent: true, opacity: 0.88 })
    );
    slit.position.set(0, 0.04, 0.19);
    group.add(slit);
  }

  group.scale.setScalar(scale);
  return group;
}

function createSupportNode(role, scale) {
  const colors = palette[role];
  const group = new THREE.Group();
  if (role === "broker") {
    [-0.24, 0, 0.24].forEach((offset) => {
      const bar = new THREE.Mesh(
        new THREE.BoxGeometry(0.95, 0.12, 0.18),
        makeLayeredMaterial(colors.base, colors.glow)
      );
      bar.position.y = offset;
      group.add(bar);
    });
  } else if (role === "provider") {
    const diamond = new THREE.Mesh(
      new THREE.OctahedronGeometry(0.62, 0),
      makeLayeredMaterial(colors.base, colors.glow)
    );
    group.add(diamond);
  } else if (role === "store") {
    [-0.38, 0, 0.38].forEach((offset, index) => {
      const disk = new THREE.Mesh(
        new THREE.CylinderGeometry(1.08 - index * 0.06, 1.08 - index * 0.06, 0.34, 48),
        makeLayeredMaterial(colors.base, colors.glow)
      );
      disk.position.y = offset;
      group.add(disk);
    });
    const halo = makeGlowSprite(colors.glow, 4.2);
    halo.position.set(0, 0.15, -0.55);
    group.add(halo);
  }
  const aura = makeGlowSprite(colors.glow, role === "store" ? 4.8 : 2.2);
  aura.position.set(0, 0, role === "store" ? -0.7 : -0.1);
  group.add(aura);
  group.scale.setScalar(scale);
  return group;
}

function createNode(node) {
  const group = node.tier === "primary"
    ? createPrimaryNode(node.role, node.scale)
    : createSupportNode(node.role, node.scale);
  group.position.set(node.x, node.y, node.z);
  group.userData.baseScale = node.scale;
  group.userData.role = node.role;
  scene.add(group);
  nodeGroups.set(node.id, group);
}

function controlPointsForEdge(fromNode, toNode, edge) {
  const start = new THREE.Vector3(fromNode.x, fromNode.y, fromNode.z);
  const end = new THREE.Vector3(toNode.x, toNode.y, toNode.z);
  const midpoint = start.clone().lerp(end, 0.5);
  const liftByKind = {
    primary: 0.38,
    request: -0.78,
    completion: -0.05,
    store: -1.05
  };
  midpoint.y += liftByKind[edge.kind] ?? 0.3;
  midpoint.z += edge.kind === "store" ? -1.15 : edge.kind === "request" ? 0.36 : edge.kind === "completion" ? 0.16 : 0.42;
  return new THREE.CatmullRomCurve3([start, midpoint, end]);
}

function addEdges(edges, nodesById) {
  for (const edge of edges) {
    const fromNode = nodesById.get(edge.from);
    const toNode = nodesById.get(edge.to);
    if (!fromNode || !toNode) {
      continue;
    }
    const curve = controlPointsForEdge(fromNode, toNode, edge);
    edgeCurves.set(edge.id, curve);
    const tube = new THREE.Mesh(
      new THREE.TubeGeometry(curve, 48, edge.kind === "primary" ? 0.038 : 0.03, 8, false),
      new THREE.MeshBasicMaterial({
        color: edge.kind === "store" ? 0x56cfff : edge.kind === "request" ? 0x7e90ff : edge.kind === "completion" ? 0xc3b2ff : 0x2c4f7e,
        transparent: true,
        opacity: edge.kind === "primary" ? 0.42 : 0.5
      })
    );
    scene.add(tube);

    const glow = new THREE.Mesh(
      new THREE.TubeGeometry(curve, 48, edge.kind === "primary" ? 0.058 : 0.045, 8, false),
      new THREE.MeshBasicMaterial({
        color: edge.kind === "store" ? 0x7ce8ff : edge.kind === "request" ? 0x9fafef : edge.kind === "completion" ? 0xe3c5ff : 0x58b8ff,
        transparent: true,
        opacity: 0.12,
        depthWrite: false
      })
    );
    scene.add(glow);
  }
}

function addDynamicPulses(pulses) {
  for (const pulse of pulses) {
    const color = new THREE.Color(pulse.color);
    const mesh = new THREE.Mesh(
      new THREE.SphereGeometry(pulse.size, 18, 18),
      new THREE.MeshStandardMaterial({
        color,
        emissive: color,
        emissiveIntensity: 0.8,
        roughness: 0.25,
        metalness: 0.15,
        transparent: true,
        opacity: 0
      })
    );
    const halo = makeGlowSprite(color, pulse.size * 10);
    halo.visible = false;
    scene.add(mesh);
    scene.add(halo);
    dynamicPulses.push({ ...pulse, mesh, halo });
  }
}

function addHighlights(highlights) {
  for (const highlight of highlights) {
    dynamicHighlights.push(highlight);
  }
}

function animateCamera(progress) {
  const x = THREE.MathUtils.lerp(-0.55, 0.88, progress) + Math.sin(progress * Math.PI * 2) * 0.14;
  const y = 1.92 + Math.sin(progress * Math.PI) * 0.22;
  const z = 16.5 - Math.sin(progress * Math.PI) * 0.72;
  camera.position.set(x, y, z);
  camera.lookAt(x * 0.08, 1.46, -1.28);
}

function applyNodeDynamics(timeSeconds) {
  for (const [id, group] of nodeGroups) {
    const pulse = 1 + Math.sin(timeSeconds * 0.9 + group.position.x * 0.35) * 0.01;
    let highlightBoost = 0;
    for (const highlight of dynamicHighlights) {
      if (highlight.targetId !== id || timeSeconds < highlight.start || timeSeconds > highlight.end) {
        continue;
      }
      const local = (timeSeconds - highlight.start) / Math.max(0.0001, highlight.end - highlight.start);
      const wave = Math.sin(local * Math.PI);
      highlightBoost = Math.max(highlightBoost, wave * 0.09 * highlight.intensity);
    }
    const scale = group.userData.baseScale * (pulse + highlightBoost);
    group.scale.setScalar(scale);
  }
}

function applyPulseState(timeSeconds) {
  for (const pulse of dynamicPulses) {
    const curve = edgeCurves.get(pulse.edgeId);
    if (!curve || timeSeconds < pulse.start || timeSeconds > pulse.start + pulse.duration) {
      pulse.mesh.visible = false;
      pulse.halo.visible = false;
      continue;
    }
    const t = (timeSeconds - pulse.start) / pulse.duration;
    const eased = THREE.MathUtils.smoothstep(t, 0, 1);
    const point = curve.getPoint(eased);
    pulse.mesh.visible = true;
    pulse.halo.visible = true;
    pulse.mesh.position.copy(point);
    pulse.halo.position.copy(point);
    pulse.mesh.material.opacity = Math.sin(t * Math.PI);
    pulse.halo.material.opacity = 0.72 * Math.sin(t * Math.PI);
  }
}

function renderFrame(progress) {
  if (!cinematicData) {
    return;
  }
  const timeSeconds = THREE.MathUtils.clamp(progress, 0, 1) * cinematicData.clipDurationSeconds;
  animateCamera(progress);
  applyNodeDynamics(timeSeconds);
  applyPulseState(timeSeconds);
  renderer.render(scene, camera);
}

async function loadScene() {
  const response = await fetch(dataPath);
  cinematicData = await response.json();
  makeBackdrop();
  addLights();
  const nodesById = new Map(cinematicData.nodes.map((node) => [node.id, node]));
  cinematicData.nodes.forEach(createNode);
  addEdges(cinematicData.edges, nodesById);
  addDynamicPulses(cinematicData.pulses);
  addHighlights(cinematicData.highlights ?? []);
  renderFrame(0);
  window.homepageReplayCinematic = {
    ready: true,
    cinematicData,
    renderFrame: (progress) => {
      renderFrame(progress);
      return true;
    }
  };
  if (autoPlay) {
    requestAnimationFrame(loop);
  }
}

function loop(now) {
  const delta = Math.min(0.05, (now - lastAutoFrame) / 1000);
  lastAutoFrame = now;
  autoProgress = (autoProgress + delta / cinematicData.clipDurationSeconds) % 1;
  renderFrame(autoProgress);
  requestAnimationFrame(loop);
}

loadScene().catch((error) => {
  console.error(error);
  window.homepageReplayCinematic = { ready: false, error: String(error) };
});
