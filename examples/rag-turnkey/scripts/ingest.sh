#!/usr/bin/env bash
set -euo pipefail
test $# -eq 1 || { echo "usage: $0 DOCUMENT.txt" >&2; exit 2; }
inbox="$(dirname "$0")/../indexer/inbox"
mkdir -p "$inbox"
cp "$1" "$inbox/"
