#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
CSV_DIR="$ROOT_DIR/examples/csv-payments"
ACTIVE_MAPPING="$CSV_DIR/config/pipeline.runtime.yaml"
MODULAR_MAPPING="$CSV_DIR/config/runtime-mapping/modular-strict.yaml"

if [[ ! -f "$MODULAR_MAPPING" ]]; then
  echo "Modular runtime mapping file not found: $MODULAR_MAPPING" >&2
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

cp "$MODULAR_MAPPING" "$ACTIVE_MAPPING"

cd "$ROOT_DIR"

IMAGE_TAG="${CSV_E2E_TELEMETRY_IMAGE_TAG:-otel}"

exec ./mvnw -f examples/csv-payments/pom.xml -DskipTests clean package \
  -Dtpf.build.transport=GRPC \
  -Dquarkus.container-image.tag="${IMAGE_TAG}" \
  -Dquarkus.otel.enabled=true \
  -Dquarkus.otel.sdk.disabled=false \
  -Dquarkus.otel.metrics.enabled=false \
  -Dquarkus.otel.traces.enabled=true \
  -Dquarkus.otel.logs.enabled=false \
  -Dquarkus.otel.exporter.otlp.enabled=false \
  -Dquarkus.otel.traces.sampler=parentbased_always_on \
  -Dquarkus.otel.traces.sampler.arg=1.0 \
  "$@"
