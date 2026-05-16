#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

IMAGE_TAG="${CSV_E2E_OBSERVABILITY_IMAGE_TAG:-observability}"

exec ./mvnw -f examples/csv-payments/pom.xml -DskipTests package \
  -Dquarkus.container-image.tag="${IMAGE_TAG}" \
  -Dquarkus.otel.enabled=true \
  -Dquarkus.otel.sdk.disabled=false \
  -Dquarkus.otel.metrics.enabled=false \
  -Dquarkus.otel.traces.enabled=true \
  -Dquarkus.otel.logs.enabled=false \
  -Dquarkus.otel.exporter.otlp.enabled=true \
  -Dquarkus.otel.exporter.otlp.protocol=http/protobuf \
  -Dquarkus.otel.traces.sampler=parentbased_always_on \
  -Dquarkus.otel.traces.sampler.arg=1.0 \
  -Dquarkus.observability.lgtm.enabled=false \
  "$@"
