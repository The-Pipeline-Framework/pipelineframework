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

active_mapping_existed=false

if [[ -f "$ACTIVE_MAPPING" ]]; then
  active_mapping_existed=true
  backup_file="$(mktemp "${TMPDIR:-/tmp}/pipeline-runtime.XXXXXX")"
  cp "$ACTIVE_MAPPING" "$backup_file"
fi

cleanup() {
  if [[ "$active_mapping_existed" == true ]]; then
    cp "$backup_file" "$ACTIVE_MAPPING"
    rm -f "$backup_file"
  else
    rm -f "$ACTIVE_MAPPING"
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
MAVEN_REPOSITORY="${MAVEN_REPOSITORY:-$ROOT_DIR/.m2/repository}"
PROVENANCE_FILE="$MAVEN_REPOSITORY/org/pipelineframework/tpf-worktree-provenance.properties"

MAVEN_REPOSITORY="$MAVEN_REPOSITORY" "$CSV_DIR/ensure-framework-artifact-provenance.sh"

property_value() {
  local key="$1"
  sed -n "s/^${key}=//p" "$PROVENANCE_FILE"
}

FRAMEWORK_VERSION="$(property_value 'framework.version')"
FRAMEWORK_COMMIT="$(property_value 'framework.commit')"
FRAMEWORK_SOURCE_FINGERPRINT="$(property_value 'framework.source.fingerprint')"
FRAMEWORK_RUNTIME_SHA256="$(property_value 'framework.runtime.sha256')"

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

  local service actual image label_value
  for service in \
    persistence-svc \
    input-csv-file-processing-svc \
    payments-processing-svc \
    payment-status-svc \
    orchestrator-svc
  do
    image="localhost/csv-payments/${service}:${IMAGE_TAG}"

    if ! docker image inspect "$image" >/dev/null 2>&1; then
      echo "Expected local image was not created: $image" >&2
      echo "Available csv-payments images:" >&2
      docker image ls \
        --format '  {{.Repository}}:{{.Tag}}  {{.ID}}' |
        grep 'csv-payments' >&2 || true
      exit 1
    fi

    actual="$(docker image inspect "$image" --format '{{.Architecture}}')"

    if [[ "$actual" != "$expected" ]]; then
      echo \
        "Image architecture mismatch for $image: expected $expected from $IMAGE_PLATFORMS, got $actual" \
        >&2
      exit 1
    fi

    for label in \
      tpf_framework_version \
      tpf_framework_commit \
      tpf_framework_source_fingerprint \
      tpf_framework_runtime_sha256
    do
      case "$label" in
        tpf_framework_version) expected_label="$FRAMEWORK_VERSION" ;;
        tpf_framework_commit) expected_label="$FRAMEWORK_COMMIT" ;;
        tpf_framework_source_fingerprint) expected_label="$FRAMEWORK_SOURCE_FINGERPRINT" ;;
        tpf_framework_runtime_sha256) expected_label="$FRAMEWORK_RUNTIME_SHA256" ;;
      esac
      label_value="$(docker image inspect "$image" --format "{{ index .Config.Labels \"$label\" }}")"
      if [[ "$label_value" != "$expected_label" ]]; then
        echo "Framework provenance mismatch for $image label $label: expected $expected_label, got $label_value" >&2
        exit 1
      fi
    done
    echo "Verified $image uses framework ${FRAMEWORK_VERSION} commit ${FRAMEWORK_COMMIT} source ${FRAMEWORK_SOURCE_FINGERPRINT} runtime ${FRAMEWORK_RUNTIME_SHA256}"
  done
}

# Jib writes shared cache metadata during image packaging; keep this reactor serialized
# unless a caller explicitly opts into parallelism.
QUARKUS_OTEL_LOGS_ENABLED=false \
QUARKUS_OTEL_TRACES_SAMPLER=always_on \
QUARKUS_OTEL_TRACES_SAMPLER_ARG=1.0 \
./mvnw -T "$MAVEN_IMAGE_THREADS" -f examples/csv-payments/pom.xml -DskipTests clean package \
  -Dmaven.repo.local="$MAVEN_REPOSITORY" \
  -Dtpf.build.transport=GRPC \
  -Dquarkus.container-image.tag="${IMAGE_TAG}" \
  -Dquarkus.container-image.labels.tpf_framework_version="${FRAMEWORK_VERSION}" \
  -Dquarkus.container-image.labels.tpf_framework_commit="${FRAMEWORK_COMMIT}" \
  -Dquarkus.container-image.labels.tpf_framework_source_fingerprint="${FRAMEWORK_SOURCE_FINGERPRINT}" \
  -Dquarkus.container-image.labels.tpf_framework_runtime_sha256="${FRAMEWORK_RUNTIME_SHA256}" \
  -Dquarkus.jib.platforms="${IMAGE_PLATFORMS}" \
  -Dquarkus.otel.enabled=true \
  -Dquarkus.otel.sdk.disabled=false \
  -Dquarkus.otel.metrics.enabled=false \
  -Dquarkus.otel.traces.enabled=true \
  -Dquarkus.otel.exporter.otlp.enabled=true \
  -Dquarkus.otel.exporter.otlp.protocol=http/protobuf \
  -Dquarkus.observability.lgtm.enabled=false \
  "$@"

verify_image_architecture
