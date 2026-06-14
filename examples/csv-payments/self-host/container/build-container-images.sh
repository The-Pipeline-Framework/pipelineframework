#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${EXAMPLE_DIR}/../.." && pwd)"
MVN_BIN="${MVN_BIN:-${REPO_ROOT}/mvnw}"

if [[ ! -x "${MVN_BIN}" ]]; then
  echo "ERROR: Maven wrapper not found or not executable at ${MVN_BIN}" >&2
  exit 1
fi

IMAGE_REGISTRY="${IMAGE_REGISTRY:-localhost}"
IMAGE_GROUP="${IMAGE_GROUP:-csv-payments}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-GRPC}"
expected_coordinator_image="${IMAGE_REGISTRY}/${IMAGE_GROUP}/orchestrator-svc:${IMAGE_TAG}"
expected_worker_image="${expected_coordinator_image}"
expected_runtime_image="${IMAGE_REGISTRY}/${IMAGE_GROUP}/pipeline-runtime-svc:${IMAGE_TAG}"
expected_persistence_image="${IMAGE_REGISTRY}/${IMAGE_GROUP}/persistence-svc:${IMAGE_TAG}"
export TPF_CSV_COORDINATOR_IMAGE="${TPF_CSV_COORDINATOR_IMAGE:-${expected_coordinator_image}}"
export TPF_CSV_WORKER_IMAGE="${TPF_CSV_WORKER_IMAGE:-${expected_worker_image}}"
export TPF_CSV_RUNTIME_IMAGE="${TPF_CSV_RUNTIME_IMAGE:-${expected_runtime_image}}"
export TPF_CSV_PERSISTENCE_IMAGE="${TPF_CSV_PERSISTENCE_IMAGE:-${expected_persistence_image}}"
export TPF_SKIP_FRAMEWORK_INSTALL="${TPF_SKIP_FRAMEWORK_INSTALL:-false}"

require_matching_image() {
  local name="$1"
  local actual="$2"
  local expected="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "ERROR: ${name}=${actual} does not match image built by this script: ${expected}" >&2
    echo "Set IMAGE_REGISTRY, IMAGE_GROUP, and IMAGE_TAG to match the TPF_CSV_*_IMAGE overrides." >&2
    exit 1
  fi
}

require_matching_image "TPF_CSV_COORDINATOR_IMAGE" "${TPF_CSV_COORDINATOR_IMAGE}" "${expected_coordinator_image}"
require_matching_image "TPF_CSV_WORKER_IMAGE" "${TPF_CSV_WORKER_IMAGE}" "${expected_worker_image}"
require_matching_image "TPF_CSV_RUNTIME_IMAGE" "${TPF_CSV_RUNTIME_IMAGE}" "${expected_runtime_image}"
require_matching_image "TPF_CSV_PERSISTENCE_IMAGE" "${TPF_CSV_PERSISTENCE_IMAGE}" "${expected_persistence_image}"

if [[ "${TPF_SKIP_FRAMEWORK_INSTALL}" != "true" ]]; then
  echo "Installing current framework SNAPSHOT for the CSV example build..."
  "${MVN_BIN}" -f "${REPO_ROOT}/framework/pom.xml" clean install
fi

echo "Building CSV pipeline-runtime topology images with gRPC step transport..."
IMAGE_REGISTRY="${IMAGE_REGISTRY}" \
IMAGE_GROUP="${IMAGE_GROUP}" \
IMAGE_TAG="${IMAGE_TAG}" \
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT}" \
"${EXAMPLE_DIR}/build-pipeline-runtime.sh" \
  -DskipTests \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=false

echo "Rebuilding CSV coordinator image with the long-running service entrypoint..."
IMAGE_REGISTRY="${IMAGE_REGISTRY}" \
IMAGE_GROUP="${IMAGE_GROUP}" \
IMAGE_TAG="${IMAGE_TAG}" \
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT}" \
"${EXAMPLE_DIR}/build-pipeline-runtime.sh" \
  -pl orchestrator-svc \
  -DskipTests \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=false \
  -Dquarkus.package.main-class=org.pipelineframework.csv.orchestrator.OrchestratorServiceApplication

echo "Built images:"
echo "  coordinator: ${TPF_CSV_COORDINATOR_IMAGE}"
echo "  worker:      ${TPF_CSV_WORKER_IMAGE}"
echo "  runtime:     ${TPF_CSV_RUNTIME_IMAGE}"
echo "  persistence: ${TPF_CSV_PERSISTENCE_IMAGE}"
