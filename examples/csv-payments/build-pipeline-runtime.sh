#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CSV_DIR="$ROOT_DIR/examples/csv-payments"
ACTIVE_MAPPING="$CSV_DIR/config/pipeline.runtime.yaml"
ACTIVE_PIPELINE_CONFIG="$CSV_DIR/config/pipeline.yaml"
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
pipeline_config_backup_file=""
if [[ -n "${PIPELINE_CONFIG:-}" ]]; then
  if [[ ! -f "${PIPELINE_CONFIG}" ]]; then
    echo "Pipeline config file not found: ${PIPELINE_CONFIG}" >&2
    exit 1
  fi
  active_pipeline_config_real="$(cd "$(dirname "$ACTIVE_PIPELINE_CONFIG")" && pwd -P)/$(basename "$ACTIVE_PIPELINE_CONFIG")"
  pipeline_config_real="$(cd "$(dirname "$PIPELINE_CONFIG")" && pwd -P)/$(basename "$PIPELINE_CONFIG")"
  if [[ "$pipeline_config_real" != "$active_pipeline_config_real" ]]; then
    if [[ -f "$ACTIVE_PIPELINE_CONFIG" ]]; then
      pipeline_config_backup_file="$(mktemp "${TMPDIR:-/tmp}/pipeline-config.XXXXXX")"
      cp "$ACTIVE_PIPELINE_CONFIG" "$pipeline_config_backup_file"
    fi
  fi
fi

cleanup() {
  if [[ -n "$backup_file" ]]; then
    cp "$backup_file" "$ACTIVE_MAPPING"
    rm -f "$backup_file"
  fi
  if [[ -n "$pipeline_config_backup_file" ]]; then
    cp "$pipeline_config_backup_file" "$ACTIVE_PIPELINE_CONFIG"
    rm -f "$pipeline_config_backup_file"
  fi
}
trap cleanup EXIT

cp "$PIPELINE_RUNTIME_MAPPING" "$ACTIVE_MAPPING"
if [[ -n "${PIPELINE_CONFIG:-}" ]]; then
  active_pipeline_config_real="$(cd "$(dirname "$ACTIVE_PIPELINE_CONFIG")" && pwd -P)/$(basename "$ACTIVE_PIPELINE_CONFIG")"
  pipeline_config_real="$(cd "$(dirname "$PIPELINE_CONFIG")" && pwd -P)/$(basename "$PIPELINE_CONFIG")"
  if [[ "$pipeline_config_real" != "$active_pipeline_config_real" ]]; then
    cp "$PIPELINE_CONFIG" "$ACTIVE_PIPELINE_CONFIG"
  fi
fi
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-GRPC}"

# Ensure module parent POM is available in local repository for Quarkus bootstrap/codegen.
"$ROOT_DIR/scripts/ci/bootstrap-local-repo-prereqs.sh" csv

"$MVN_BIN" -f "$CSV_DIR/pom.pipeline-runtime.xml" -Dcsv.runtime.layout=pipeline-runtime -Dtpf.build.transport="$PIPELINE_TRANSPORT" clean install "$@"
