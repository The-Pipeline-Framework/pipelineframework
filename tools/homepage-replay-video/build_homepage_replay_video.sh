#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOL_DIR="$REPO_ROOT/tools/homepage-replay-video"
TOOL_VENV="$TOOL_DIR/.venv"
INPUT_REPLAY="$REPO_ROOT/tools/replay-viewer/datasets/csv-payments-built-in.json"
SIMPLIFIED_DATA="$TOOL_DIR/data/csv-payments-cinematic.json"
FRAMES_DIR="$TOOL_DIR/.tmp/frames"
WEBM_OUT="$REPO_ROOT/docs/public/home/replay-proof.webm"
MP4_OUT="$REPO_ROOT/docs/public/home/replay-proof.mp4"
POSTER_OUT="$REPO_ROOT/docs/public/home/replay-proof-poster.jpg"
MANIFEST_OUT="$REPO_ROOT/docs/public/home/replay-proof-manifest.json"
OUTPUT_DIR="$(dirname "$WEBM_OUT")"

NODE_BIN="${NODE_BIN:-$(command -v node || true)}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"

if [[ -z "$NODE_BIN" ]]; then
  echo "Unable to find node. Set NODE_BIN to a usable Node.js executable." >&2
  exit 1
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Unable to find python3. Set PYTHON_BIN to a usable Python executable." >&2
  exit 1
fi

if [[ ! -r "$INPUT_REPLAY" ]]; then
  echo "Unable to read replay source at $INPUT_REPLAY" >&2
  exit 1
fi

if [[ -z "${NODE_MODULES_DIR:-}" ]]; then
  # Prefer repo-local installs first, then NODE_PATH/global npm roots, then module directories inferred
  # from NODE_BIN and cached Codex runtimes. The first directory that actually contains Playwright wins.
  declare -a node_module_candidates=()
  node_module_candidates+=("$REPO_ROOT/node_modules")
  node_module_candidates+=("$REPO_ROOT/docs/node_modules")
  node_module_candidates+=("${NODE_PATH:-}")
  node_module_candidates+=("$(npm root -g 2>/dev/null || true)")
  node_bin_dir="$(cd "$(dirname "$NODE_BIN")" && pwd)"
  node_runtime_modules="$(cd "$node_bin_dir/.." && pwd)/node_modules"
  node_module_candidates+=("$node_runtime_modules")
  shopt -s nullglob
  for candidate in "$HOME"/.cache/codex-runtimes/*/dependencies/node/node_modules; do
    node_module_candidates+=("$candidate")
  done
  shopt -u nullglob

  for candidate in "${node_module_candidates[@]}"; do
    if [[ -n "$candidate" && -d "$candidate/playwright" ]]; then
      NODE_MODULES_DIR="$candidate"
      break
    fi
  done
fi

if [[ -z "${NODE_MODULES_DIR:-}" || ! -d "$NODE_MODULES_DIR" ]]; then
  echo "Unable to find a Node module directory with Playwright. Set NODE_MODULES_DIR explicitly." >&2
  exit 1
fi

if [[ ! -x "$TOOL_VENV/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$TOOL_VENV"
fi

"$TOOL_VENV/bin/python" -m pip install --quiet --upgrade pip
"$TOOL_VENV/bin/python" -m pip install --quiet imageio-ffmpeg
export NODE_PATH="$NODE_MODULES_DIR${NODE_PATH:+:$NODE_PATH}"
mkdir -p "$OUTPUT_DIR"

"$NODE_BIN" "$TOOL_DIR/prepare_replay_data.cjs" \
  --input "$INPUT_REPLAY" \
  --output "$SIMPLIFIED_DATA"

rm -rf "$FRAMES_DIR"
mkdir -p "$FRAMES_DIR"
trap 'rm -rf "$FRAMES_DIR"' EXIT
"$NODE_BIN" "$TOOL_DIR/capture_frames.cjs" \
  --root "$REPO_ROOT" \
  --data "$SIMPLIFIED_DATA" \
  --frames-dir "$FRAMES_DIR" \
  --poster "$POSTER_OUT"
"$TOOL_VENV/bin/python" "$TOOL_DIR/encode_video.py" --input-dir "$FRAMES_DIR" --fps 24 --format webm --output "$WEBM_OUT"
"$TOOL_VENV/bin/python" "$TOOL_DIR/encode_video.py" --input-dir "$FRAMES_DIR" --fps 24 --format mp4 --output "$MP4_OUT"

"$NODE_BIN" - "$REPO_ROOT" "$MANIFEST_OUT" "$INPUT_REPLAY" "$SIMPLIFIED_DATA" "$TOOL_DIR/prepare_replay_data.cjs" "$TOOL_DIR/render_scene.html" "$TOOL_DIR/render_scene.js" "$TOOL_DIR/capture_frames.cjs" "$TOOL_DIR/encode_video.py" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const [repoRoot, manifestPath, ...sourcePaths] = process.argv.slice(2);
const hashFile = (filePath) => crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
const manifest = {
  generatedBy: "tools/homepage-replay-video/build_homepage_replay_video.sh",
  sources: Object.fromEntries(sourcePaths.map((filePath) => [
    path.relative(repoRoot, filePath).split(path.sep).join("/"),
    hashFile(filePath)
  ]))
};
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
NODE
