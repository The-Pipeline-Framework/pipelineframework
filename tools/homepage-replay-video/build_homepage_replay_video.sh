#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE_BIN="${NODE_BIN:-/Users/mari/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node}"
NODE_MODULES_DIR="${NODE_MODULES_DIR:-/Users/mari/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules}"
PYTHON_BIN="${PYTHON_BIN:-/Users/mari/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3}"
TOOL_DIR="$REPO_ROOT/tools/homepage-replay-video"
TOOL_VENV="$TOOL_DIR/.venv"
INPUT_REPLAY="$REPO_ROOT/tools/replay-viewer/datasets/csv-payments-built-in.json"
SIMPLIFIED_DATA="$TOOL_DIR/data/csv-payments-cinematic.json"
FRAMES_DIR="$TOOL_DIR/.tmp/frames"
WEBM_OUT="$REPO_ROOT/docs/public/home/replay-proof.webm"
MP4_OUT="$REPO_ROOT/docs/public/home/replay-proof.mp4"
POSTER_OUT="$REPO_ROOT/docs/public/home/replay-proof-poster.jpg"

if [[ ! -x "$TOOL_VENV/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$TOOL_VENV"
fi

"$TOOL_VENV/bin/python" -m pip install --quiet --upgrade pip
"$TOOL_VENV/bin/python" -m pip install --quiet imageio-ffmpeg
export NODE_PATH="$NODE_MODULES_DIR${NODE_PATH:+:$NODE_PATH}"

"$NODE_BIN" "$TOOL_DIR/prepare_replay_data.cjs" \
  --input "$INPUT_REPLAY" \
  --output "$SIMPLIFIED_DATA"

rm -rf "$FRAMES_DIR"
mkdir -p "$FRAMES_DIR"
"$NODE_BIN" "$TOOL_DIR/capture_frames.cjs" \
  --root "$REPO_ROOT" \
  --data "$SIMPLIFIED_DATA" \
  --frames-dir "$FRAMES_DIR" \
  --poster "$POSTER_OUT"
"$TOOL_VENV/bin/python" "$TOOL_DIR/encode_video.py" --input-dir "$FRAMES_DIR" --fps 24 --format webm --output "$WEBM_OUT"
"$TOOL_VENV/bin/python" "$TOOL_DIR/encode_video.py" --input-dir "$FRAMES_DIR" --fps 24 --format mp4 --output "$MP4_OUT"
rm -rf "$FRAMES_DIR"
