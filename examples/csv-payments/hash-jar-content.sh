#!/usr/bin/env bash

set -euo pipefail

JAR_PATH="${1:?Usage: hash-jar-content.sh <jar-path>}"

hash_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

# Maven JARs in this reactor do not have a reproducible output timestamp. Hash
# canonical entry names and contents so equivalent rebuilds have one identity.
unzip -Z1 "$JAR_PATH" |
  LC_ALL=C sort |
  while IFS= read -r entry; do
    printf '%s\0' "$entry"
    unzip -p "$JAR_PATH" "$entry"
  done |
  hash_stream
