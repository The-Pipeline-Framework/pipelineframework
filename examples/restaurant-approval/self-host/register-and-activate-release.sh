#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MONOLITH_DIR="${EXAMPLE_DIR}/monolith-svc"

TPF_TENANT_ID="${TPF_TENANT_ID:-restaurant-demo}"
TPF_PIPELINE_ID="${TPF_PIPELINE_ID:-org.pipelineframework.restaurantapproval}"
TPF_COORDINATOR_PORT="${TPF_COORDINATOR_PORT:-8081}"
TPF_ADMIN_TOKEN="${TPF_ADMIN_TOKEN:-restaurant-control-plane-admin-token}"
TPF_WORKER_ID="${TPF_WORKER_ID:-restaurant-local-worker}"
TPF_WORKER_PROTOCOL="${TPF_WORKER_PROTOCOL:-local}"
TPF_WORKER_ENDPOINT="${TPF_WORKER_ENDPOINT:-in-process}"

BUNDLE_JAR="${TPF_BUNDLE_JAR:-}"
if [[ -z "${BUNDLE_JAR}" ]]; then
  BUNDLE_JAR="$(python3 "${SCRIPT_DIR}/demo-client.py" locate-bundle --target-dir "${MONOLITH_DIR}/target" --pipeline-id "${TPF_PIPELINE_ID}")"
fi

if [[ ! -f "${BUNDLE_JAR}" ]]; then
  echo "Release artifact JAR not found: ${BUNDLE_JAR}" >&2
  exit 1
fi

RELEASE_DESCRIPTOR="${TPF_RELEASE_DESCRIPTOR:-${MONOLITH_DIR}/target/tpf-self-host/pipeline-release.json}"
python3 "${SCRIPT_DIR}/demo-client.py" create-release \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --artifact-path "${BUNDLE_JAR}" \
  --output "${RELEASE_DESCRIPTOR}" >/dev/null

python3 "${SCRIPT_DIR}/demo-client.py" register-activate \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --admin-token "${TPF_ADMIN_TOKEN}" \
  --release-descriptor-path "${RELEASE_DESCRIPTOR}" \
  --worker-id "${TPF_WORKER_ID}" \
  --worker-protocol "${TPF_WORKER_PROTOCOL}" \
  --worker-endpoint "${TPF_WORKER_ENDPOINT}"
