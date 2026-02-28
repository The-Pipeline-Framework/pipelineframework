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

# cleanup restores ACTIVE_MAPPING from $backup_file if one exists.
cleanup() {
  if [[ -n "$backup_file" ]]; then
    cp "$backup_file" "$ACTIVE_MAPPING"
    rm -f "$backup_file"
  fi
}
trap cleanup EXIT

cp "$MONOLITH_MAPPING" "$ACTIVE_MAPPING"

# Build orchestrator-svc first to generate LOCAL client sources and metadata used by monolith-svc.
echo "Building orchestrator-svc to generate LOCAL client sources..."
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-LOCAL}"

# Ensure module parent POM is available in local repository for Quarkus bootstrap/codegen.
"$MVN_BIN" -f "$CSV_DIR/pom.xml" -N install
# Ensure foundational plugin coordinates are resolvable even from a clean local repository.
"$MVN_BIN" -f "$ROOT_DIR/plugins/foundational/persistence/pom.xml" -DskipTests install

ORCHESTRATOR_ARGS=()
for arg in "$@"; do
  if [[ "$arg" == "-DskipTests" ]]; then
    ORCHESTRATOR_ARGS+=("$arg")
  fi
done
"$MVN_BIN" -f "$CSV_DIR/pom.pipeline-runtime.xml" -Dcsv.runtime.layout=monolith -Dtpf.build.transport="$PIPELINE_TRANSPORT" clean install -pl orchestrator-svc -am "${ORCHESTRATOR_ARGS[@]}"

echo "Building monolith..."
"$MVN_BIN" -f "$CSV_DIR/pom.monolith.xml" -Dtpf.build.transport="$PIPELINE_TRANSPORT" clean install "$@"
