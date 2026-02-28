#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CSV_DIR="$ROOT_DIR/examples/csv-payments"
ACTIVE_MAPPING="$CSV_DIR/config/pipeline.runtime.yaml"
PIPELINE_RUNTIME_MAPPING="$CSV_DIR/config/runtime-mapping/pipeline-runtime.yaml"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/mvnw}"

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven launcher is not executable: $MVN_BIN" >&2
  exit 1
fi
if [[ ! -f "$PIPELINE_RUNTIME_MAPPING" ]]; then
  echo "Pipeline-runtime mapping file not found: $PIPELINE_RUNTIME_MAPPING" >&2
  exit 1
fi

mkdir -p "$(dirname "$ACTIVE_MAPPING")"

backup_file=""
if [[ -f "$ACTIVE_MAPPING" ]]; then
  backup_file="$(mktemp "${TMPDIR:-/tmp}/pipeline-runtime.XXXXXX")"
  cp "$ACTIVE_MAPPING" "$backup_file"
fi

cleanup() {
  if [[ -n "$backup_file" ]]; then
    cp "$backup_file" "$ACTIVE_MAPPING"
    rm -f "$backup_file"
  fi
}
trap cleanup EXIT

cp "$PIPELINE_RUNTIME_MAPPING" "$ACTIVE_MAPPING"
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-GRPC}"

# Ensure module parent POM is available in local repository for Quarkus bootstrap/codegen.
"$MVN_BIN" -f "$CSV_DIR/pom.xml" -N install
# Ensure foundational plugin coordinates are resolvable even from a clean local repository.
"$MVN_BIN" -f "$ROOT_DIR/plugins/foundational/persistence/pom.xml" -DskipTests install

"$MVN_BIN" -f "$CSV_DIR/pom.pipeline-runtime.xml" -Dcsv.runtime.layout=pipeline-runtime -Dtpf.build.transport="$PIPELINE_TRANSPORT" clean install "$@"
