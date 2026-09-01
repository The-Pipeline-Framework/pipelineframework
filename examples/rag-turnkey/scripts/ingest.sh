#!/usr/bin/env bash
set -euo pipefail
test $# -ge 1 || { echo "usage: $0 DOCUMENT..." >&2; exit 2; }
inbox="$(dirname "$0")/../indexer/inbox"
mkdir -p "$inbox"
temporary=""
cleanup() {
  if [[ -n "$temporary" ]]; then
    rm -f -- "$temporary"
  fi
}
trap cleanup EXIT

for source in "$@"; do
  if [[ ! -f "$source" ]]; then
    echo "document is not a regular file: $source" >&2
    exit 1
  fi

  document_name="$(basename -- "$source")"
  declared_extension="${document_name##*.}"
  extension="$(printf '%s' "$declared_extension" | tr '[:upper:]' '[:lower:]')"
  case "$extension" in
    txt|md|pdf|docx) ;;
    *) echo "unsupported document extension: .$extension (expected .txt, .md, .pdf, or .docx)" >&2; exit 2 ;;
  esac
  if [[ "$declared_extension" != "$extension" ]]; then
    echo "use a lowercase document extension so the inbox filter can admit it: .$extension" >&2
    exit 2
  fi

  destination="$inbox/$document_name"
  if [[ -d "$destination" ]]; then
    echo "refusing directory destination: $destination" >&2
    exit 1
  fi

  temporary="$(mktemp "$inbox/.$document_name.partial.XXXXXX")"
  cp -- "$source" "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    echo "refusing to overwrite previously admitted document: $destination" >&2
    exit 1
  fi
  rm -f -- "$temporary"
  temporary=""
  echo "admitted: $document_name"
done
