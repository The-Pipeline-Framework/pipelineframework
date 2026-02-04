#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CSV_DIR="$ROOT_DIR/examples/csv-payments"
SCENARIO_DIR="$CSV_DIR/config/runtime-mapping"
ACTIVE_MAPPING="$CSV_DIR/config/pipeline.runtime.yaml"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/mvnw}"
if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven launcher is not executable: $MVN_BIN" >&2
  echo "Set MVN_BIN to a valid executable (for example ./mvnw or mvn)." >&2
  exit 1
fi

WITH_E2E=0
declare -a SCENARIOS=()

usage() {
  cat <<'USAGE'
Usage: run-runtime-mapping-matrix.sh [options]

Options:
  --with-e2e           Run functional CsvPaymentsEndToEndIT after each scenario build.
  --scenario <name>    Run a single scenario (can be repeated).
                       Known: modular-auto, modular-strict, pipeline-runtime
  --help               Show this help.

Examples:
  ./examples/csv-payments/run-runtime-mapping-matrix.sh
  ./examples/csv-payments/run-runtime-mapping-matrix.sh --with-e2e
  ./examples/csv-payments/run-runtime-mapping-matrix.sh --scenario pipeline-runtime --with-e2e

Note:
  This runner validates build/runtime-mapping and functional behavior.
  It does not assert deployment topology shape (container/runtime count).
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-e2e)
      WITH_E2E=1
      shift
      ;;
    --scenario)
      if [[ $# -lt 2 ]]; then
        echo "Missing scenario name after --scenario" >&2
        exit 1
      fi
      SCENARIOS+=("$2")
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
  SCENARIOS=("modular-auto" "modular-strict" "pipeline-runtime")
fi

for scenario in "${SCENARIOS[@]}"; do
  scenario_file="$SCENARIO_DIR/$scenario.yaml"
  if [[ ! -f "$scenario_file" ]]; then
    echo "Scenario file not found: $scenario_file" >&2
    exit 1
  fi
done

backup_file=""
if [[ -f "$ACTIVE_MAPPING" ]]; then
  backup_file="$(mktemp "${TMPDIR:-/tmp}/pipeline-runtime.XXXXXX")"
  cp "$ACTIVE_MAPPING" "$backup_file"
fi

cleanup() {
  if [[ -n "$backup_file" ]]; then
    cp "$backup_file" "$ACTIVE_MAPPING"
    rm -f "$backup_file"
  else
    rm -f "$ACTIVE_MAPPING"
  fi
}
trap cleanup EXIT

for scenario in "${SCENARIOS[@]}"; do
  echo "=== Scenario: $scenario ==="
  cp "$SCENARIO_DIR/$scenario.yaml" "$ACTIVE_MAPPING"

  if [[ $WITH_E2E -eq 1 ]]; then
    "$MVN_BIN" -f "$CSV_DIR/pom.xml" -DskipTests clean install
    "$MVN_BIN" -f "$CSV_DIR/orchestrator-svc/pom.xml" -Dtest=CsvPaymentsEndToEndIT test
  else
    "$MVN_BIN" -f "$CSV_DIR/pom.xml" -DskipTests clean compile
  fi
done

echo "Runtime mapping matrix completed successfully."
