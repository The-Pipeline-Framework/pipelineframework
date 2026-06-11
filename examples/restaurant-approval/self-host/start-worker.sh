#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MONOLITH_DIR="${EXAMPLE_DIR}/monolith-svc"

TPF_WORKER_PORT="${TPF_WORKER_PORT:-8181}"
TPF_WORKER_SECRET="${TPF_WORKER_SECRET:-restaurant-transition-worker-secret}"
TPF_RUN_DIR="${TPF_RUN_DIR:-${MONOLITH_DIR}/target/tpf-self-host}"
TPF_LOG_DIR="${TPF_LOG_DIR:-${TPF_RUN_DIR}/logs}"
TPF_PID_DIR="${TPF_PID_DIR:-${TPF_RUN_DIR}/pids}"

RUNNER="${MONOLITH_DIR}/target/quarkus-app/quarkus-run.jar"
if [[ ! -f "${RUNNER}" ]]; then
  echo "Missing ${RUNNER}. Run ./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc -am -DskipTests package first." >&2
  exit 1
fi

mkdir -p "${TPF_LOG_DIR}" "${TPF_PID_DIR}"

(
  cd "${MONOLITH_DIR}"
  java \
    --enable-preview \
    -Dquarkus.http.host=127.0.0.1 \
    -Dquarkus.http.port="${TPF_WORKER_PORT}" \
    -Dquarkus.http.ssl-port=0 \
    -Dquarkus.http.insecure-requests=enabled \
    -Dquarkus.grpc.server.use-separate-server=false \
    -Dquarkus.grpc.server.plain-text=true \
    -Dpipeline.module.monolith-svc.host=localhost \
    -Dpipeline.module.monolith-svc.port="${TPF_WORKER_PORT}" \
    -Dquarkus.rest-client.process-validate-order-request.url="http://localhost:${TPF_WORKER_PORT}" \
    -Dquarkus.rest-client.process-create-pending-approval.url="http://localhost:${TPF_WORKER_PORT}" \
    -Dquarkus.rest-client.process-finalize-restaurant-decision.url="http://localhost:${TPF_WORKER_PORT}" \
    -Dquarkus.otel.enabled=false \
    -Dquarkus.otel.sdk.disabled=true \
    -Dquarkus.micrometer.export.prometheus.enabled=false \
    -Dquarkus.micrometer.binder.http-server.enabled=false \
    -Dquarkus.micrometer.binder.http-client.enabled=false \
    -Dquarkus.micrometer.binder.netty.enabled=false \
    -Dpipeline.orchestrator.worker.rest.server-enabled=true \
    -Dpipeline.orchestrator.worker.rest.shared-secret="${TPF_WORKER_SECRET}" \
    -Dpipeline.orchestrator.strict-startup=false \
    -jar "${RUNNER}"
) > "${TPF_LOG_DIR}/worker.log" 2>&1 &

echo "$!" > "${TPF_PID_DIR}/worker.pid"
echo "Started restaurant approval REST worker on http://localhost:${TPF_WORKER_PORT}"
echo "Log: ${TPF_LOG_DIR}/worker.log"
