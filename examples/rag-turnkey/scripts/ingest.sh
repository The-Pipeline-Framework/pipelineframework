#!/usr/bin/env bash
set -euo pipefail
test $# -eq 1 || { echo "usage: $0 DOCUMENT.txt" >&2; exit 2; }
inbox="$(dirname "$0")/../indexer/inbox"
mkdir -p "$inbox"
document_name="$(basename -- "$1")"
destination="$inbox/$document_name"
temporary="$(mktemp "$inbox/.$document_name.partial.XXXXXX")"
cleanup() {
  rm -f "$temporary"
}
trap cleanup EXIT
if [[ -d "$destination" ]]; then
  echo "refusing directory destination: $destination" >&2
  exit 1
fi
cp -- "$1" "$temporary"
if ! ln "$temporary" "$destination" 2>/dev/null; then
  echo "refusing to overwrite previously admitted document: $destination" >&2
  exit 1
fi
