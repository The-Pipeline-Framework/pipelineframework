import { readFileSync, statSync } from "node:fs";
import { createHash } from "node:crypto";
import { gunzipSync } from "node:zlib";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BUILT_IN_REPLAYS_CONFIG } from "../../tools/replay-viewer/built-in-replays.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../..");
const viewerDir = path.join(repoRoot, "tools", "replay-viewer");
const appSource = readFileSync(path.join(viewerDir, "app.js"), "utf8");

function sha256(filePath) {
  return createHash("sha256").update(readFileSync(filePath)).digest("hex");
}

function readStringConstant(name, seen = new Set()) {
  if (seen.has(name)) {
    return null;
  }
  seen.add(name);
  const literal = appSource.match(new RegExp(`const\\s+${name}\\s*=\\s*"([^"]+)"`));
  if (literal) {
    return literal[1];
  }
  const reference = appSource.match(new RegExp(`const\\s+${name}\\s*=\\s*([A-Z0-9_]+)`));
  if (!reference) {
    return null;
  }
  return readStringConstant(reference[1], seen);
}

const defaultSourceKey = readStringConstant("DEFAULT_REPLAY_SOURCE_KEY");
if (!defaultSourceKey) {
  throw new Error("Replay viewer DEFAULT_REPLAY_SOURCE_KEY was not found.");
}

const defaultDatasetMaxBytes = 1_000_000;
const shippedDatasetMaxBytes = 15_000_000;
const emptySourceKey = "none";
const datasetEntries = BUILT_IN_REPLAYS_CONFIG.map((entry) => ({
  key: entry.key,
  label: entry.label,
  path: entry.path,
  compression: entry.compression
}));

if (datasetEntries.length === 0) {
  throw new Error("Replay viewer built-in datasets were not found.");
}

const defaultEntry = datasetEntries.find((entry) => entry.key === defaultSourceKey);
if (defaultSourceKey !== emptySourceKey && !defaultEntry) {
  throw new Error(`Default replay source '${defaultSourceKey}' is not registered as a built-in dataset.`);
}

for (const entry of datasetEntries) {
  const relativePath = entry.path.replace(/^\.\//, "");
  const filePath = path.join(viewerDir, relativePath);
  let size;
  try {
    size = statSync(filePath).size;
  } catch (error) {
    throw new Error(`Dataset file not found: ${entry.label} at ${filePath} (${error.message})`);
  }
  if (size > shippedDatasetMaxBytes) {
    throw new Error(`${entry.label} is ${(size / 1_000_000).toFixed(2)} MB, over the shipped replay dataset budget.`);
  }
  if (entry.key === defaultSourceKey && size > defaultDatasetMaxBytes) {
    throw new Error(`${entry.label} is ${(size / 1_000_000).toFixed(2)} MB, over the startup replay dataset budget.`);
  }
}

const csvPaymentsEntry = datasetEntries.find((entry) => entry.key === "csv-payments");
if (!csvPaymentsEntry) {
  throw new Error("CSV Payments built-in replay dataset is not registered.");
}

const csvPaymentsReplay = JSON.parse(
  readFileSync(path.join(viewerDir, csvPaymentsEntry.path.replace(/^\.\//, "")), "utf8"),
);
const branchStarts = csvPaymentsReplay.events.reduce(
  (counts, event) => {
    if (event.event !== "start") {
      return counts;
    }
    if (event.step === "ProcessApprovedPaymentStatus") {
      counts.approved += 1;
    } else if (event.step === "ProcessUnapprovedPaymentStatus") {
      counts.unapproved += 1;
    }
    return counts;
  },
  { approved: 0, unapproved: 0 },
);

if (branchStarts.approved !== 907 || branchStarts.unapproved !== 93) {
  throw new Error(
    `CSV Payments built-in replay must preserve the deterministic 907/93 approved/unapproved split; got ${branchStarts.approved}/${branchStarts.unapproved}.`,
  );
}

const paged10kEntry = datasetEntries.find((entry) => entry.key === "csv-payments-10k");
if (!paged10kEntry || paged10kEntry.compression !== "gzip") {
  throw new Error("The compressed CSV Payments 10k paging replay is not registered.");
}
const paged10kReplay = JSON.parse(gunzipSync(readFileSync(
  path.join(viewerDir, paged10kEntry.path.replace(/^\.\//, "")),
)).toString("utf8"));
const count10k = (step, event) => paged10kReplay.events.filter((entry) =>
  entry.step === step && entry.event === event).length;
if (paged10kReplay.status !== "completed"
    || count10k("ProcessCsvPaymentsInput", "start") !== 10
    || count10k("ProcessCsvPaymentsInput", "emit") !== 10_000
    || count10k("ProcessCsvPaymentsInput", "success") !== 10
    || count10k("ProcessApprovedPaymentStatus", "start") !== 9_170
    || count10k("ProcessUnapprovedPaymentStatus", "start") !== 830
    || paged10kReplay.events.some((event) => event.step === "PagedSourceStepAdapter")) {
  throw new Error("CSV Payments 10k replay must preserve ten semantic source pages and all 10,000 item paths.");
}
const first10kStatus = Math.min(...paged10kReplay.events
  .filter((event) => event.event === "start" &&
    (event.step === "ProcessApprovedPaymentStatus" || event.step === "ProcessUnapprovedPaymentStatus"))
  .map((event) => event.startTime));
const last10kSourceEmit = Math.max(...paged10kReplay.events
  .filter((event) => event.step === "ProcessCsvPaymentsInput" && event.event === "emit")
  .map((event) => event.startTime));
if (!(first10kStatus < last10kSourceEmit)) {
  throw new Error("CSV Payments 10k replay must show downstream status progress before source exhaustion.");
}

const slow1kEntry = datasetEntries.find((entry) => entry.key === "csv-payments-1k-slow");
if (!slow1kEntry || slow1kEntry.compression !== "gzip") {
  throw new Error("The compressed CSV Payments 1k slow-provider replay is not registered.");
}
const slow1kReplay = JSON.parse(gunzipSync(readFileSync(
  path.join(viewerDir, slow1kEntry.path.replace(/^\.\//, "")),
)).toString("utf8"));
const countSlow1k = (step, event) => slow1kReplay.events.filter((entry) =>
  entry.step === step && entry.event === event).length;
if (slow1kReplay.status !== "completed"
    || countSlow1k("ProcessCsvPaymentsInput", "start") !== 1
    || countSlow1k("ProcessCsvPaymentsInput", "emit") !== 1_000
    || countSlow1k("ProcessApprovedPaymentStatus", "start") !== 907
    || countSlow1k("ProcessUnapprovedPaymentStatus", "start") !== 93
    || Math.abs(slow1kReplay.durationMs - paged10kReplay.durationMs) > 5_000) {
  throw new Error("The slow 1k replay must preserve its one-page output and matched provider-paced duration.");
}

const homepageManifestPath = path.join(repoRoot, "docs", "public", "home", "replay-proof-manifest.json");
const homepageManifest = JSON.parse(readFileSync(homepageManifestPath, "utf8"));
for (const [relativePath, expectedHash] of Object.entries(homepageManifest.sources ?? {})) {
  const sourcePath = path.join(repoRoot, relativePath);
  if (sha256(sourcePath) !== expectedHash) {
    throw new Error(
      `Homepage replay asset provenance is stale for ${relativePath}; run npm --prefix docs run build:homepage-video.`,
    );
  }
}

console.log(`Replay dataset and homepage provenance checks passed; startup source '${defaultSourceKey}' is within budget.`);
