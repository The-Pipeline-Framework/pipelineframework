import { cpSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const docsDir = path.resolve(__dirname, "..");
const repoRoot = path.resolve(docsDir, "..");
const sourceDir = path.join(repoRoot, "tools", "replay-viewer");
const targetDir = path.join(docsDir, "public", "replay-viewer");

rmSync(targetDir, { recursive: true, force: true });
mkdirSync(path.dirname(targetDir), { recursive: true });
cpSync(sourceDir, targetDir, { recursive: true });
