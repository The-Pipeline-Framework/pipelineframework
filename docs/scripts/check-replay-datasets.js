import { readFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../..");
const viewerDir = path.join(repoRoot, "tools", "replay-viewer");
const appSource = readFileSync(path.join(viewerDir, "app.js"), "utf8");

function readStringConstant(name) {
  const literal = appSource.match(new RegExp(`const\\s+${name}\\s*=\\s*"([^"]+)"`));
  if (literal) {
    return literal[1];
  }
  const reference = appSource.match(new RegExp(`const\\s+${name}\\s*=\\s*([A-Z0-9_]+)`));
  if (!reference) {
    return null;
  }
  return readStringConstant(reference[1]);
}

const defaultSourceKey = readStringConstant("DEFAULT_REPLAY_SOURCE_KEY");
if (!defaultSourceKey) {
  throw new Error("Replay viewer DEFAULT_REPLAY_SOURCE_KEY was not found.");
}

const defaultDatasetMaxBytes = 1_000_000;
const shippedDatasetMaxBytes = 15_000_000;
const emptySourceKey = "none";
const datasetEntries = [...appSource.matchAll(/\["([^"]+)",\s*\{\s*label:\s*"([^"]+)",\s*path:\s*"([^"]+)"/g)]
  .map((match) => ({
    key: match[1],
    label: match[2],
    path: match[3]
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
  const size = statSync(filePath).size;
  if (size > shippedDatasetMaxBytes) {
    throw new Error(`${entry.label} is ${(size / 1_000_000).toFixed(2)} MB, over the shipped replay dataset budget.`);
  }
  if (entry.key === defaultSourceKey && size > defaultDatasetMaxBytes) {
    throw new Error(`${entry.label} is ${(size / 1_000_000).toFixed(2)} MB, over the startup replay dataset budget.`);
  }
}

console.log(`Replay dataset size check passed; startup source '${defaultSourceKey}' is within budget.`);
