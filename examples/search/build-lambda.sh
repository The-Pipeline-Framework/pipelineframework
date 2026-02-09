#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SEARCH_DIR="$ROOT_DIR/examples/search"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/mvnw}"

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven launcher is not executable: $MVN_BIN" >&2
  exit 1
fi

PIPELINE_PLATFORM="${PIPELINE_PLATFORM:-LAMBDA}"
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-REST}"
PIPELINE_REST_NAMING_STRATEGY="${PIPELINE_REST_NAMING_STRATEGY:-RESOURCEFUL}"

"$MVN_BIN" -f "$SEARCH_DIR/pom.xml" \
  -Dpipeline.platform="$PIPELINE_PLATFORM" \
  -Dpipeline.transport="$PIPELINE_TRANSPORT" \
  -Dpipeline.rest.naming.strategy="$PIPELINE_REST_NAMING_STRATEGY" \
  clean install "$@"
