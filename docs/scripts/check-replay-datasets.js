import { readFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BUILT_IN_REPLAYS_CONFIG } from "../../tools/replay-viewer/built-in-replays.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../..");
const viewerDir = path.join(repoRoot, "tools", "replay-viewer");
const appSource = readFileSync(path.join(viewerDir, "app.js"), "utf8");

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
  path: entry.path
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

console.log(`Replay dataset size check passed; startup source '${defaultSourceKey}' is within budget.`);
