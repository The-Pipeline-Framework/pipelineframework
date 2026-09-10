#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${EXAMPLE_DIR}/../.." && pwd)"
MONOLITH_DIR="${EXAMPLE_DIR}/monolith-svc"

TPF_TENANT_ID="${TPF_TENANT_ID:-restaurant-demo}"
TPF_PIPELINE_ID="${TPF_PIPELINE_ID:-org.pipelineframework.restaurantapproval}"
TPF_AWAIT_STEP_ID="${TPF_AWAIT_STEP_ID:-ProcessAwaitRestaurantDecisionService}"
TPF_COORDINATOR_PORT="${TPF_COORDINATOR_PORT:-8081}"
TPF_CONTROL_PLANE_TOKEN="${TPF_CONTROL_PLANE_TOKEN:-restaurant-control-plane-admin-token}"
TPF_ADMIN_TOKEN="${TPF_ADMIN_TOKEN:-restaurant-control-plane-admin-token}"
TPF_RUN_DIR="${TPF_RUN_DIR:-${MONOLITH_DIR}/target/tpf-self-host}"
TPF_LOG_DIR="${TPF_LOG_DIR:-${TPF_RUN_DIR}/logs}"
TPF_PID_DIR="${TPF_PID_DIR:-${TPF_RUN_DIR}/pids}"
TPF_SKIP_FRAMEWORK_INSTALL="${TPF_SKIP_FRAMEWORK_INSTALL:-false}"

CI_MODE=false
if [[ "${1:-}" == "--ci" ]]; then
  CI_MODE=true
fi

cleanup() {
  for pid_file in "${TPF_PID_DIR}/coordinator.pid" "${TPF_PID_DIR}/worker.pid"; do
    if [[ -f "${pid_file}" ]]; then
      pid="$(cat "${pid_file}")"
      if kill -0 "${pid}" >/dev/null 2>&1; then
        kill "${pid}" >/dev/null 2>&1 || true
        for _ in {1..50}; do
          if ! kill -0 "${pid}" >/dev/null 2>&1; then
            break
          fi
          sleep 0.1
        done
        if kill -0 "${pid}" >/dev/null 2>&1; then
          kill -9 "${pid}" >/dev/null 2>&1 || true
        fi
      fi
      rm -f "${pid_file}"
    fi
  done
}
trap cleanup EXIT

if [[ "${CI_MODE}" == "true" ]]; then
  rm -rf "${TPF_RUN_DIR}"
fi

mkdir -p "${TPF_LOG_DIR}" "${TPF_PID_DIR}"
rm -f "${TPF_PID_DIR}/coordinator.pid" "${TPF_PID_DIR}/worker.pid"

if [[ "${TPF_SKIP_FRAMEWORK_INSTALL}" != "true" ]]; then
  echo "Installing current framework SNAPSHOT for the example build..."
  "${REPO_ROOT}/mvnw" -f "${REPO_ROOT}/framework/pom.xml" -pl runtime,deployment -am -DskipTests install
fi

echo "Packaging restaurant-approval monolith..."
"${REPO_ROOT}/mvnw" -f "${EXAMPLE_DIR}/pom.xml" -pl monolith-svc -am -DskipTests package

echo "Starting batteries-included coordinator..."
"${SCRIPT_DIR}/start-coordinator.sh"

python3 "${SCRIPT_DIR}/demo-client.py" wait-health --base-url "http://localhost:${TPF_COORDINATOR_PORT}" --name coordinator

"${SCRIPT_DIR}/register-and-activate-release.sh"

python3 "${SCRIPT_DIR}/demo-client.py" run-flows \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --await-step-id "${TPF_AWAIT_STEP_ID}" \
  --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}"

if [[ "${CI_MODE}" == "true" ]]; then
  echo "Self-hosted coordinator demo completed in CI mode."
else
  echo "Self-hosted coordinator demo completed."
  echo "Logs:"
  echo "  ${TPF_LOG_DIR}/coordinator.log"
fi
