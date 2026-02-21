#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CSV_DIR="$ROOT_DIR/examples/csv-payments"
ACTIVE_MAPPING="$CSV_DIR/config/pipeline.runtime.yaml"
MONOLITH_MAPPING="$CSV_DIR/config/runtime-mapping/monolith.yaml"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/mvnw}"

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven launcher is not executable: $MVN_BIN" >&2
  exit 1
fi
if [[ ! -f "$MONOLITH_MAPPING" ]]; then
  echo "Monolith runtime mapping file not found: $MONOLITH_MAPPING" >&2
  exit 1
fi

backup_file=""
if [[ -f "$ACTIVE_MAPPING" ]]; then
  backup_file="$(mktemp "${TMPDIR:-/tmp}/pipeline-runtime.XXXXXX")"
  cp "$ACTIVE_MAPPING" "$backup_file"
fi

# cleanup restores ACTIVE_MAPPING from $backup_file if one exists; otherwise it removes the ACTIVE_MAPPING file.
cleanup() {
  if [[ -n "$backup_file" ]]; then
    cp "$backup_file" "$ACTIVE_MAPPING"
    rm -f "$backup_file"
  else
    rm -f "$ACTIVE_MAPPING"
  fi
}
trap cleanup EXIT

cp "$MONOLITH_MAPPING" "$ACTIVE_MAPPING"

# Build orchestrator-svc first to generate LOCAL client sources and metadata
# The monolith build copies these from orchestrator-svc/target/
echo "Building orchestrator-svc to generate LOCAL client sources..."
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-LOCAL}"
"$MVN_BIN" -f "$CSV_DIR/pom.pipeline-runtime.xml" -Dtpf.build.transport="$PIPELINE_TRANSPORT" clean install -pl orchestrator-svc -am

# Now build monolith (copies generated sources from orchestrator-svc/target/)
echo "Building monolith..."
"$MVN_BIN" -f "$CSV_DIR/pom.monolith.xml" clean install "$@"