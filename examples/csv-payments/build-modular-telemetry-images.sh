#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

IMAGE_TAG="${CSV_E2E_TELEMETRY_IMAGE_TAG:-otel}"

exec ./mvnw -f examples/csv-payments/pom.xml -DskipTests package \
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
