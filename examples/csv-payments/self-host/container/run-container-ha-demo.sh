#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${EXAMPLE_DIR}/../.." && pwd)"
PIPELINE_RUNTIME_DIR="${EXAMPLE_DIR}/pipeline-runtime-svc"
ORCHESTRATOR_DIR="${EXAMPLE_DIR}/orchestrator-svc"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMPOSE_KAFKA_FILE="${SCRIPT_DIR}/compose.kafka.yaml"
CLIENT="${SCRIPT_DIR}/demo-client.py"

export TPF_REPO_ROOT="${TPF_REPO_ROOT:-${REPO_ROOT}}"
export TPF_CSV_AWAIT_TRANSPORT="${TPF_CSV_AWAIT_TRANSPORT:-sqs}"
TPF_CSV_PIPELINE_CONFIG_EXPLICIT="${TPF_CSV_PIPELINE_CONFIG:-}"
case "${TPF_CSV_AWAIT_TRANSPORT}" in
  sqs)
    TPF_CSV_PIPELINE_CONFIG_TEMPLATE="${EXAMPLE_DIR}/config/pipeline.container-sqs.yaml"
    export TPF_RUNTIME_AWAIT_KAFKA_ENABLED="${TPF_RUNTIME_AWAIT_KAFKA_ENABLED:-false}"
    export TPF_RUNTIME_KAFKA_PROVIDER_ENABLED="${TPF_RUNTIME_KAFKA_PROVIDER_ENABLED:-false}"
    export TPF_RUNTIME_AWAIT_SQS_PROVIDER_ENABLED="${TPF_RUNTIME_AWAIT_SQS_PROVIDER_ENABLED:-true}"
    export TPF_WORKER_AWAIT_KAFKA_ENABLED="${TPF_WORKER_AWAIT_KAFKA_ENABLED:-false}"
    export TPF_COORDINATOR_AWAIT_KAFKA_ENABLED="${TPF_COORDINATOR_AWAIT_KAFKA_ENABLED:-false}"
    export TPF_COORDINATOR_AWAIT_SQS_POLLER_ENABLED="${TPF_COORDINATOR_AWAIT_SQS_POLLER_ENABLED:-true}"
    ;;
  kafka)
    TPF_CSV_PIPELINE_CONFIG_TEMPLATE="${EXAMPLE_DIR}/config/pipeline.yaml"
    export TPF_KAFKA_PORT="${TPF_KAFKA_PORT:-9093}"
    export TPF_KAFKA_BOOTSTRAP_SERVERS="${TPF_KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
    export TPF_RUNTIME_AWAIT_KAFKA_ENABLED="${TPF_RUNTIME_AWAIT_KAFKA_ENABLED:-false}"
    export TPF_RUNTIME_KAFKA_PROVIDER_ENABLED="${TPF_RUNTIME_KAFKA_PROVIDER_ENABLED:-true}"
    export TPF_RUNTIME_AWAIT_SQS_PROVIDER_ENABLED="${TPF_RUNTIME_AWAIT_SQS_PROVIDER_ENABLED:-false}"
    export TPF_WORKER_AWAIT_KAFKA_ENABLED="${TPF_WORKER_AWAIT_KAFKA_ENABLED:-true}"
    export TPF_COORDINATOR_AWAIT_KAFKA_ENABLED="${TPF_COORDINATOR_AWAIT_KAFKA_ENABLED:-false}"
    export TPF_COORDINATOR_AWAIT_SQS_POLLER_ENABLED="${TPF_COORDINATOR_AWAIT_SQS_POLLER_ENABLED:-false}"
    ;;
  *)
    echo "ERROR: TPF_CSV_AWAIT_TRANSPORT must be 'sqs' or 'kafka'." >&2
    exit 1
    ;;
esac
export TPF_CSV_COORDINATOR_IMAGE="${TPF_CSV_COORDINATOR_IMAGE:-localhost/csv-payments/orchestrator-svc:latest}"
export TPF_CSV_WORKER_IMAGE="${TPF_CSV_WORKER_IMAGE:-localhost/csv-payments/orchestrator-svc:latest}"
export TPF_CSV_RUNTIME_IMAGE="${TPF_CSV_RUNTIME_IMAGE:-localhost/csv-payments/pipeline-runtime-svc:latest}"
export TPF_CSV_PERSISTENCE_IMAGE="${TPF_CSV_PERSISTENCE_IMAGE:-localhost/csv-payments/persistence-svc:latest}"
export TPF_TENANT_ID="${TPF_TENANT_ID:-csv-demo}"
export TPF_PIPELINE_ID="${TPF_PIPELINE_ID:-org.pipelineframework.csv}"
export TPF_COORDINATOR_PORT="${TPF_COORDINATOR_PORT:-8082}"
export TPF_WORKER_PORT="${TPF_WORKER_PORT:-8182}"
export TPF_RUNTIME_PORT="${TPF_RUNTIME_PORT:-8283}"
export TPF_PERSISTENCE_PORT="${TPF_PERSISTENCE_PORT:-8282}"
export TPF_LOCALSTACK_PORT="${TPF_LOCALSTACK_PORT:-4567}"
export TPF_CONTROL_PLANE_TOKEN="${TPF_CONTROL_PLANE_TOKEN:-csv-control-plane-admin-token}"
export TPF_ADMIN_TOKEN="${TPF_ADMIN_TOKEN:-csv-control-plane-admin-token}"
export TPF_WORKER_SECRET="${TPF_WORKER_SECRET:-csv-transition-worker-secret}"
export TPF_WORKER_ID="${TPF_WORKER_ID:-csv-rest-worker}"
export TPF_WORKER_PROTOCOL="${TPF_WORKER_PROTOCOL:-rest}"
export TPF_WORKER_ENDPOINT="${TPF_WORKER_ENDPOINT:-http://worker:8182}"
export TPF_RUN_DIR="${TPF_RUN_DIR:-${SCRIPT_DIR}/target/tpf-container-ha}"
export TPF_INPUT_DIR="${TPF_INPUT_DIR:-${TPF_RUN_DIR}/input}"
export TPF_OUTPUT_DIR="${TPF_OUTPUT_DIR:-${TPF_INPUT_DIR}}"
export TPF_RELEASE_DESCRIPTOR="${TPF_RELEASE_DESCRIPTOR:-${TPF_RUN_DIR}/pipeline-release.json}"
export TPF_SOURCE_CSV="${TPF_SOURCE_CSV:-${EXAMPLE_DIR}/input-csv-file-processing-svc/csv/payments_12.csv}"
export TPF_SKIP_CONTAINER_BUILD="${TPF_SKIP_CONTAINER_BUILD:-false}"

CI_MODE=false
if [[ "${1:-}" == "--ci" ]]; then
  CI_MODE=true
fi

compose() {
  if [[ "${TPF_CSV_AWAIT_TRANSPORT}" == "kafka" ]]; then
    docker compose -f "${COMPOSE_FILE}" -f "${COMPOSE_KAFKA_FILE}" "$@"
    return
  fi
  docker compose -f "${COMPOSE_FILE}" "$@"
}

require_free_port() {
  local name="$1"
  local port="$2"
  if command -v lsof >/dev/null 2>&1; then
    if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "${name} port ${port} is already in use. Stop the existing process or override the port with TPF_${name}_PORT." >&2
      exit 1
    fi
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    if ! python3 - "${port}" >/dev/null 2>&1 <<'PY'
import socket
import sys

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", int(sys.argv[1])))
PY
    then
      echo "${name} port ${port} is already in use. Stop the existing process or override the port with TPF_${name}_PORT." >&2
      exit 1
    fi
    return
  fi
  echo "Warning: no port availability checker found; skipping ${name} port ${port} check." >&2
}

cleanup() {
  local exit_code="${1:-0}"
  if [[ "${CI_MODE}" == "true" && "${exit_code}" != "0" && "${TPF_KEEP_STACK_ON_FAILURE:-false}" == "true" ]]; then
    echo "CSV containerized self-host HA stack failed and is being preserved for log collection."
    return
  fi
  if [[ "${CI_MODE}" == "true" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "CSV containerized self-host HA stack is still running."
    echo "Stop it with: docker compose -f ${COMPOSE_FILE} down -v"
  fi
}
trap 'cleanup $?' EXIT

generate_container_pipeline_config() {
  mkdir -p "${TPF_RUN_DIR}" "${TPF_INPUT_DIR}"
  if [[ -n "${TPF_CSV_PIPELINE_CONFIG_EXPLICIT}" ]]; then
    export TPF_CSV_PIPELINE_CONFIG="${TPF_CSV_PIPELINE_CONFIG_EXPLICIT}"
    return
  fi
  export TPF_CSV_PIPELINE_CONFIG="${TPF_RUN_DIR}/pipeline.container-${TPF_CSV_AWAIT_TRANSPORT}.yaml"
  cp "${TPF_CSV_PIPELINE_CONFIG_TEMPLATE}" "${TPF_CSV_PIPELINE_CONFIG}"
  TPF_CONTAINER_OBJECT_ROOT="${TPF_INPUT_DIR}" \
    perl -0pi -e 's#root: \.\./input-csv-file-processing-svc/csv#root: $ENV{TPF_CONTAINER_OBJECT_ROOT}#g' \
    "${TPF_CSV_PIPELINE_CONFIG}"
}

if [[ "${CI_MODE}" == "true" ]]; then
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${TPF_RUN_DIR}"
fi

require_free_port "COORDINATOR" "${TPF_COORDINATOR_PORT}"
require_free_port "WORKER" "${TPF_WORKER_PORT}"
require_free_port "RUNTIME" "${TPF_RUNTIME_PORT}"
require_free_port "PERSISTENCE" "${TPF_PERSISTENCE_PORT}"
require_free_port "LOCALSTACK" "${TPF_LOCALSTACK_PORT}"
if [[ "${TPF_CSV_AWAIT_TRANSPORT}" == "kafka" ]]; then
  require_free_port "KAFKA" "${TPF_KAFKA_PORT}"
fi

if [[ "${TPF_SKIP_CONTAINER_BUILD}" != "true" ]]; then
  if [[ -n "${TPF_CSV_PIPELINE_CONFIG_EXPLICIT}" ]]; then
    export TPF_CSV_PIPELINE_CONFIG="${TPF_CSV_PIPELINE_CONFIG_EXPLICIT}"
  else
    unset TPF_CSV_PIPELINE_CONFIG
  fi
  "${SCRIPT_DIR}/build-container-images.sh"
fi
generate_container_pipeline_config

bash "${EXAMPLE_DIR}/generate-dev-certs.sh" >/dev/null

"${SCRIPT_DIR}/bootstrap-localstack.sh"
if [[ "${TPF_CSV_AWAIT_TRANSPORT}" == "kafka" ]]; then
  "${SCRIPT_DIR}/bootstrap-kafka.sh"
fi

echo "Starting CSV persistence, runtime, worker, and coordinator containers with ${TPF_CSV_AWAIT_TRANSPORT} await..."
compose up -d persistence runtime worker coordinator

python3 "${CLIENT}" wait-health \
  --base-url "http://localhost:${TPF_PERSISTENCE_PORT}" \
  --name persistence \
  --timeout-seconds 120

python3 "${CLIENT}" wait-health \
  --base-url "http://localhost:${TPF_WORKER_PORT}" \
  --name worker \
  --timeout-seconds 180

python3 "${CLIENT}" wait-health \
  --base-url "http://localhost:${TPF_RUNTIME_PORT}" \
  --name runtime \
  --timeout-seconds 180

python3 "${CLIENT}" wait-health \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --name coordinator \
  --timeout-seconds 180

if [[ -z "${TPF_RELEASE_ARTIFACT:-}" ]]; then
  TPF_RELEASE_ARTIFACT="$(python3 "${CLIENT}" locate-artifact \
    --target-dir "${ORCHESTRATOR_DIR}/target" \
    --pipeline-id "${TPF_PIPELINE_ID}")" || {
      echo "Unable to locate CSV release artifact." >&2
      exit 1
    }
fi
export TPF_RELEASE_ARTIFACT

python3 "${CLIENT}" create-release \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --artifact-path "${TPF_RELEASE_ARTIFACT}" \
  --output "${TPF_RELEASE_DESCRIPTOR}"

python3 "${CLIENT}" register-activate \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --admin-token "${TPF_ADMIN_TOKEN}" \
  --release-descriptor-path "${TPF_RELEASE_DESCRIPTOR}" \
  --worker-id "${TPF_WORKER_ID}" \
  --worker-protocol "${TPF_WORKER_PROTOCOL}" \
  --worker-endpoint "${TPF_WORKER_ENDPOINT}"

python3 "${CLIENT}" run-flow \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --control-plane-token "${TPF_CONTROL_PLANE_TOKEN}" \
  --input-dir "${TPF_INPUT_DIR}" \
  --output-dir "${TPF_OUTPUT_DIR}" \
  --source-csv "${TPF_SOURCE_CSV}" \
  --timeout-seconds 300

if [[ "${CI_MODE}" == "true" ]]; then
  echo "CSV containerized self-host HA demo (${TPF_CSV_AWAIT_TRANSPORT}) completed in CI mode."
else
  echo "CSV containerized self-host HA demo (${TPF_CSV_AWAIT_TRANSPORT}) completed."
  echo "Logs: docker compose -f ${COMPOSE_FILE} logs coordinator worker runtime persistence localstack"
fi
