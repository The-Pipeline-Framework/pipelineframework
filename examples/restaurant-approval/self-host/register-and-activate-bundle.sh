#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MONOLITH_DIR="${EXAMPLE_DIR}/monolith-svc"

TPF_TENANT_ID="${TPF_TENANT_ID:-restaurant-demo}"
TPF_PIPELINE_ID="${TPF_PIPELINE_ID:-org.pipelineframework.restaurantapproval}"
TPF_COORDINATOR_PORT="${TPF_COORDINATOR_PORT:-8081}"
TPF_ADMIN_TOKEN="${TPF_ADMIN_TOKEN:-restaurant-control-plane-admin-token}"

BUNDLE_JAR="${TPF_BUNDLE_JAR:-}"
if [[ -z "${BUNDLE_JAR}" ]]; then
  BUNDLE_JAR="$(python3 "${SCRIPT_DIR}/demo-client.py" locate-bundle --target-dir "${MONOLITH_DIR}/target" --pipeline-id "${TPF_PIPELINE_ID}")"
fi

if [[ ! -f "${BUNDLE_JAR}" ]]; then
  echo "Bundle JAR not found: ${BUNDLE_JAR}" >&2
  exit 1
fi

python3 "${SCRIPT_DIR}/demo-client.py" register-activate \
  --base-url "http://localhost:${TPF_COORDINATOR_PORT}" \
  --tenant-id "${TPF_TENANT_ID}" \
  --pipeline-id "${TPF_PIPELINE_ID}" \
  --admin-token "${TPF_ADMIN_TOKEN}" \
  --artifact-path "${BUNDLE_JAR}"
