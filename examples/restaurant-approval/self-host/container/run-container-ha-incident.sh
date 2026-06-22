#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF_HOST_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${EXAMPLE_DIR}/../.." && pwd)"
MONOLITH_DIR="${EXAMPLE_DIR}/monolith-svc"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"

export TPF_REPO_ROOT="${TPF_REPO_ROOT:-${REPO_ROOT}}"
export TPF_RESTAURANT_IMAGE="${TPF_RESTAURANT_IMAGE:-localhost/restaurant-approval/monolith-svc:latest}"
export TPF_TENANT_ID="${TPF_TENANT_ID:-restaurant-demo}"
export TPF_PIPELINE_ID="${TPF_PIPELINE_ID:-org.pipelineframework.restaurantapproval}"
export TPF_AWAIT_STEP_ID="${TPF_AWAIT_STEP_ID:-ProcessAwaitRestaurantDecisionService}"
export TPF_ORDER_TYPE="${TPF_ORDER_TYPE:-org.pipelineframework.restaurantapproval.common.dto.PlaceRestaurantOrderRequestDto}"
export TPF_COORDINATOR_PORT="${TPF_COORDINATOR_PORT:-8081}"
export TPF_WORKER_PORT="${TPF_WORKER_PORT:-8181}"
export TPF_CONTROL_PLANE_TOKEN="${TPF_CONTROL_PLANE_TOKEN:-restaurant-control-plane-admin-token}"
export TPF_ADMIN_TOKEN="${TPF_ADMIN_TOKEN:-restaurant-control-plane-admin-token}"
export TPF_WORKER_SECRET="${TPF_WORKER_SECRET:-restaurant-transition-worker-secret}"
export TPF_WORKER_ID="${TPF_WORKER_ID:-restaurant-rest-worker}"
export TPF_WORKER_PROTOCOL="${TPF_WORKER_PROTOCOL:-rest}"
export TPF_WORKER_ENDPOINT="${TPF_WORKER_ENDPOINT:-http://worker:8181}"
export TPF_RUN_DIR="${TPF_RUN_DIR:-${MONOLITH_DIR}/target/tpf-container-ha-incident}"
export TPF_RELEASE_DESCRIPTOR="${TPF_RELEASE_DESCRIPTOR:-${TPF_RUN_DIR}/pipeline-release.json}"
export TPF_ORCHESTRATOR_MAX_RETRIES="${TPF_ORCHESTRATOR_MAX_RETRIES:-0}"
export TPF_SKIP_CONTAINER_BUILD="${TPF_SKIP_CONTAINER_BUILD:-false}"

CI_MODE=false
if [[ "${1:-}" == "--ci" ]]; then
  CI_MODE=true
fi

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

cleanup() {
  local exit_code="${1:-0}"
  if [[ "${CI_MODE}" == "true" && "${exit_code}" != "0" && "${TPF_KEEP_STACK_ON_FAILURE:-false}" == "true" ]]; then
    echo "Containerized self-host HA incident stack failed and is being preserved for log collection."
    return
  fi
  if [[ "${CI_MODE}" == "true" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "Containerized self-host HA incident stack is still running."
    echo "Stop it with: docker compose -f ${COMPOSE_FILE} down -v"
  fi
}
trap 'cleanup $?' EXIT

if [[ "${CI_MODE}" == "true" ]]; then
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${TPF_RUN_DIR}"
fi
mkdir -p "${TPF_RUN_DIR}"

CERT_FILE_PERMISSIONS="${CERT_FILE_PERMISSIONS:-644}"
if [[ ! -f "${EXAMPLE_DIR}/target/dev-certs/orchestrator-svc/client-truststore.jks" ]]; then
  CERT_FILE_PERMISSIONS="${CERT_FILE_PERMISSIONS}" bash "${EXAMPLE_DIR}/generate-dev-certs.sh"
fi
find "${EXAMPLE_DIR}/target/dev-certs" -type f \( -name "*.p12" -o -name "*.jks" \) \
  -exec chmod "${CERT_FILE_PERMISSIONS}" {} +

if [[ "${TPF_SKIP_CONTAINER_BUILD}" != "true" ]]; then
  "${SCRIPT_DIR}/build-container-image.sh"
fi
find "${EXAMPLE_DIR}/target/dev-certs" -type f \( -name "*.p12" -o -name "*.jks" \) \
  -exec chmod "${CERT_FILE_PERMISSIONS}" {} +

"${SCRIPT_DIR}/bootstrap-localstack.sh"

echo "Starting restaurant approval worker and coordinator containers for incident demo..."
compose up -d worker coordinator

python3 "${SELF_HOST_DIR}/demo-client.py" wait-health \
  --base-url "http://localhost:${TPF_WORKER_PORT}" \
  --name worker \
  --timeout-seconds 120

python3 "${SELF_HOST_DIR}/demo-client.py" wait-health \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --name coordinator \
  --timeout-seconds 120

TPF_BUNDLE_JAR="${TPF_BUNDLE_JAR:-$(python3 "${SELF_HOST_DIR}/demo-client.py" locate-bundle \
  --target-dir "${MONOLITH_DIR}/target" \
  --pipeline-id "${TPF_PIPELINE_ID}")}"
export TPF_BUNDLE_JAR

"${SELF_HOST_DIR}/register-and-activate-release.sh"

python3 "${SELF_HOST_DIR}/demo-client.py" run-incident \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --await-step-id "${TPF_AWAIT_STEP_ID}" \
  --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
  --admin-token "${TPF_ADMIN_TOKEN}" \
  --order-type "${TPF_ORDER_TYPE}" \
  --timeout-seconds 120

if [[ "${CI_MODE}" == "true" ]]; then
  echo "Containerized self-host HA incident demo completed in CI mode."
else
  echo "Containerized self-host HA incident demo completed."
  echo "Logs: docker compose -f ${COMPOSE_FILE} logs coordinator worker localstack"
fi
