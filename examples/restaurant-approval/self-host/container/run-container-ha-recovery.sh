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
export TPF_RUN_DIR="${TPF_RUN_DIR:-${MONOLITH_DIR}/target/tpf-container-ha-recovery}"
export TPF_RELEASE_DESCRIPTOR="${TPF_RELEASE_DESCRIPTOR:-${TPF_RUN_DIR}/pipeline-release.json}"
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
    echo "Containerized self-host HA recovery stack failed and is being preserved for log collection."
    return
  fi
  if [[ "${CI_MODE}" == "true" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "Containerized self-host HA recovery stack is still running."
    echo "Stop it with: docker compose -f ${COMPOSE_FILE} down -v"
  fi
}
trap 'cleanup $?' EXIT

wait_health() {
  local name="$1"
  local port="$2"
  python3 "${SELF_HOST_DIR}/demo-client.py" wait-health \
    --base-url "http://localhost:${port}" \
    --name "${name}" \
    --timeout-seconds 120
}

register_release_and_worker() {
  "${SELF_HOST_DIR}/register-and-activate-release.sh"
}

submit_and_wait_external() {
  local output_file="$1"
  local customer="$2"
  local restaurant="$3"
  python3 "${SELF_HOST_DIR}/demo-client.py" submit-order \
    --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
    --tenant-id "${TPF_TENANT_ID}" \
    --pipeline-id "${TPF_PIPELINE_ID}" \
    --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
    --order-type "${TPF_ORDER_TYPE}" \
    --customer-name "${customer}" \
    --restaurant-name "${restaurant}" \
    --output "${output_file}"
  local execution_id
  execution_id="$(cat "${output_file}")"
  python3 "${SELF_HOST_DIR}/demo-client.py" wait-status \
    --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
    --tenant-id "${TPF_TENANT_ID}" \
    --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
    --execution-id "${execution_id}" \
    --target-status WAITING_EXTERNAL \
    --timeout-seconds 120 >/dev/null
}

complete_and_assert() {
  local execution_id="$1"
  local decision="$2"
  local expected="$3"
  python3 "${SELF_HOST_DIR}/demo-client.py" complete-pending \
    --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
    --tenant-id "${TPF_TENANT_ID}" \
    --await-step-id "${TPF_AWAIT_STEP_ID}" \
    --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
    --execution-id "${execution_id}" \
    --decision "${decision}"
  python3 "${SELF_HOST_DIR}/demo-client.py" assert-result \
    --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
    --tenant-id "${TPF_TENANT_ID}" \
    --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
    --execution-id "${execution_id}" \
    --expected-outcome "${expected}" \
    --timeout-seconds 120
}

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
# The Maven/Jib build may regenerate or copy cert files; normalize again before bind-mounting them into containers.
find "${EXAMPLE_DIR}/target/dev-certs" -type f \( -name "*.p12" -o -name "*.jks" \) \
  -exec chmod "${CERT_FILE_PERMISSIONS}" {} +

"${SCRIPT_DIR}/bootstrap-localstack.sh"

echo "Starting restaurant approval worker and coordinator containers for recovery proof..."
compose up -d worker coordinator
wait_health worker "${TPF_WORKER_PORT}"
wait_health coordinator "${TPF_COORDINATOR_PORT}"

TPF_BUNDLE_JAR="${TPF_BUNDLE_JAR:-$(python3 "${SELF_HOST_DIR}/demo-client.py" locate-bundle \
  --target-dir "${MONOLITH_DIR}/target" \
  --pipeline-id "${TPF_PIPELINE_ID}")}"
export TPF_BUNDLE_JAR

register_release_and_worker

FIRST_EXECUTION_FILE="${TPF_RUN_DIR}/coordinator-restart-execution-id.txt"
submit_and_wait_external "${FIRST_EXECUTION_FILE}" "Dorothy Vaughan" "Restart Bistro"
FIRST_EXECUTION_ID="$(cat "${FIRST_EXECUTION_FILE}")"

echo "Restarting coordinator after execution ${FIRST_EXECUTION_ID} reached WAITING_EXTERNAL..."
compose restart coordinator
wait_health coordinator "${TPF_COORDINATOR_PORT}"

python3 "${SELF_HOST_DIR}/demo-client.py" status \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
  --execution-id "${FIRST_EXECUTION_ID}" >/dev/null
complete_and_assert "${FIRST_EXECUTION_ID}" accepted APPROVED

SECOND_EXECUTION_FILE="${TPF_RUN_DIR}/worker-restart-execution-id.txt"
submit_and_wait_external "${SECOND_EXECUTION_FILE}" "Mary Jackson" "Worker Cafe"
SECOND_EXECUTION_ID="$(cat "${SECOND_EXECUTION_FILE}")"

echo "Restarting worker after execution ${SECOND_EXECUTION_ID} reached WAITING_EXTERNAL..."
compose restart worker
wait_health worker "${TPF_WORKER_PORT}"
register_release_and_worker
complete_and_assert "${SECOND_EXECUTION_ID}" declined DECLINED

if [[ "${CI_MODE}" == "true" ]]; then
  echo "Containerized self-host HA recovery proof completed in CI mode."
else
  echo "Containerized self-host HA recovery proof completed."
  echo "Logs: docker compose -f ${COMPOSE_FILE} logs coordinator worker localstack"
fi
