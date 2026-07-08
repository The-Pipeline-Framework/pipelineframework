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

IMAGE_TAG="${CSV_E2E_OBSERVABILITY_IMAGE_TAG:-observability}"

resolve_default_image_platforms() {
  local docker_arch
  docker_arch="$(docker version --format '{{.Server.Arch}}' 2>/dev/null || uname -m)"
  case "$docker_arch" in
    x86_64|amd64)
      echo "linux/amd64"
      ;;
    arm64|aarch64)
      echo "linux/arm64/v8"
      ;;
    *)
      echo "linux/amd64"
      ;;
  esac
}

DEFAULT_IMAGE_PLATFORMS="$(resolve_default_image_platforms)"
IMAGE_PLATFORMS="${CSV_E2E_IMAGE_PLATFORMS:-$DEFAULT_IMAGE_PLATFORMS}"
MAVEN_IMAGE_THREADS="${CSV_E2E_IMAGE_MAVEN_THREADS:-1}"

expected_arch() {
  case "$IMAGE_PLATFORMS" in
    linux/amd64)
      echo "amd64"
      ;;
    linux/arm64|linux/arm64/v8)
      echo "arm64"
      ;;
    *)
      echo ""
      ;;
  esac
}

verify_image_architecture() {
  local expected
  expected="$(expected_arch)"
  if [[ -z "$expected" ]]; then
    echo "Skipping local image architecture check for multi/unknown platform set: $IMAGE_PLATFORMS"
    return
  fi

  local service actual image
  for service in \
    persistence-svc \
    input-csv-file-processing-svc \
    payments-processing-svc \
    payment-status-svc \
    output-csv-file-processing-svc \
    orchestrator-svc
  do
    image="localhost/csv-payments/${service}:${IMAGE_TAG}"
    actual="$(docker image inspect "$image" --format '{{.Architecture}}' 2>/dev/null || true)"
    if [[ "$actual" != "$expected" ]]; then
      echo "Image architecture mismatch for $image: expected $expected from $IMAGE_PLATFORMS, got ${actual:-<missing>}" >&2
      exit 1
    fi
  done
}

# Jib writes shared cache metadata during image packaging; keep this reactor serialized
# unless a caller explicitly opts into parallelism.
./mvnw -T "$MAVEN_IMAGE_THREADS" -f examples/csv-payments/pom.xml -DskipTests clean package \
  -Dtpf.build.transport=GRPC \
  -Dquarkus.container-image.tag="${IMAGE_TAG}" \
  -Dquarkus.jib.platforms="${IMAGE_PLATFORMS}" \
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

verify_image_architecture
