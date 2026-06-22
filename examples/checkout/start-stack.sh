#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHECKOUT_DIR="${SCRIPT_DIR}"
LOG_DIR="${CHECKOUT_DIR}/target/stack-runtime"
CONFIG_DIR="${CHECKOUT_DIR}/target/stack-runtime-config"
MVN_BIN="${REPO_ROOT}/mvnw"
TPF_MAVEN_REPO_LOCAL="${TPF_MAVEN_REPO_LOCAL:-${REPO_ROOT}/.m2/repository}"
MVN_REPO_ARG="-Dmaven.repo.local=${TPF_MAVEN_REPO_LOCAL}"

SKIP_BUILD="${SKIP_BUILD:-false}"
RUN_VERIFY="${RUN_VERIFY:-false}"
RUN_COMMAND=""
START_UI="${START_UI:-false}"
CLEAN_PORTS="${CLEAN_PORTS:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      SKIP_BUILD="true"
      shift
      ;;
    --verify)
      RUN_VERIFY="true"
      shift
      ;;
    --run-cmd)
      if [[ $# -lt 2 ]]; then
        echo "Missing argument for --run-cmd" >&2
        echo "Usage: --run-cmd \"<command>\"" >&2
        exit 1
      fi
      RUN_COMMAND="$2"
      shift 2
      ;;
    --with-ui)
      START_UI="true"
      shift
      ;;
    --clean-ports)
      CLEAN_PORTS="true"
      AUTO_KILL_PORT_CONFLICTS="true"
      shift
      ;;
    --help|-h)
      echo "Usage: $(basename "$0") [--skip-build] [--verify | --run-cmd \"<command>\"] [--with-ui] [--clean-ports]"
      echo "  --skip-build            Skip Maven package step and reuse already-built artifacts"
      echo "  --verify                Run default checkout Maven verify while services are running, then exit"
      echo "  --run-cmd \"<command>\"  Run an arbitrary command while services are running, then exit"
      echo "                         (command executed via bash -lc from the repo root)"
      echo "  --with-ui               Start the checkout Next.js UI after backend modules are ready"
      echo "  --clean-ports           Stop any processes already using known checkout stack ports"
      echo "  --help, -h              Show this help text"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Use --help for usage" >&2
      exit 1
      ;;
  esac
done

if [[ "$RUN_VERIFY" == "true" && -n "$RUN_COMMAND" ]]; then
  echo "Cannot combine --verify with --run-cmd" >&2
  exit 1
fi

if [[ "$RUN_VERIFY" == "true" && -z "$RUN_COMMAND" ]]; then
  RUN_COMMAND="${MVN_BIN} ${MVN_REPO_ARG} -f ${CHECKOUT_DIR}/pom.xml verify"
fi

if [[ "$START_UI" == "true" ]] && [[ -n "$RUN_COMMAND" ]]; then
  echo "Cannot combine --with-ui with --verify or --run-cmd." >&2
  echo "Use --with-ui for interactive sessions, or --verify/--run-cmd without UI." >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to wait for service health checks" >&2
  exit 1
fi

SKIP_PORT_PRECHECK="${SKIP_PORT_PRECHECK:-false}"
AUTO_KILL_PORT_CONFLICTS="${AUTO_KILL_PORT_CONFLICTS:-false}"

if [[ "$SKIP_PORT_PRECHECK" != "true" ]] && ! command -v lsof >/dev/null 2>&1; then
  echo "lsof is required for port preflight checks (install lsof or set SKIP_PORT_PRECHECK=true)." >&2
  exit 1
fi

declare -a MODULES=(
  "pipeline-runtime-svc"
  "checkout-orchestrator-svc"
  "consumer-validation-orchestrator-svc"
  "restaurant-acceptance-orchestrator-svc"
  "kitchen-preparation-orchestrator-svc"
  "dispatch-orchestrator-svc"
  "delivery-execution-orchestrator-svc"
  "payment-capture-orchestrator-svc"
  "compensation-failure-orchestrator-svc"
)

if [[ "$START_UI" == "true" ]] && ! command -v npm >/dev/null 2>&1; then
  echo "npm is required when using --with-ui" >&2
  exit 1
fi

if [[ "$SKIP_BUILD" == "true" ]]; then
  echo "Note: --skip-build keeps existing module artifacts." \
    "Re-run without --skip-build after code or pipeline changes."
fi

if [[ "$SKIP_BUILD" == "true" && "${ALLOW_STALE_ARTIFACTS:-false}" != "true" ]]; then
  is_artifact_stale() {
    local module_dir=$1
    local artifact="${module_dir}/target/quarkus-app/quarkus-run.jar"
    local stale_source

    if [[ ! -f "$artifact" ]]; then
      echo "Build artifact missing for $(basename "$module_dir"): ${artifact}" >&2
      return 0
    fi

    stale_source=$(find "$module_dir" -type f \( -name "*.java" -o -name "*.yaml" -o -name "*.yml" -o -name "pom.xml" -o -name "pipeline.yaml" \) -print0 2>/dev/null |
      while IFS= read -r -d '' file; do
        [[ "$file" -nt "$artifact" ]] && printf '%s' "$file"
      done | head -c 1)

    if [[ -n "$stale_source" ]]; then
      return 0
    fi
    return 1
  }

  check_stale_artifacts() {
    local module=$1
    local module_dir="${CHECKOUT_DIR}/${module}"
    if is_artifact_stale "$module_dir"; then
      echo "Stale module artifacts detected for ${module} (sources changed after artifact build)." >&2
      echo "Run ./start-stack.sh without --skip-build (or set ALLOW_STALE_ARTIFACTS=true)." >&2
      exit 1
    fi
  }

  for module in "${MODULES[@]}"; do
    check_stale_artifacts "$module"
  done
fi

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven wrapper not executable: $MVN_BIN" >&2
  exit 1
fi

mkdir -p "${LOG_DIR}" "${CONFIG_DIR}" "${TPF_MAVEN_REPO_LOCAL}"

cleanup() {
  if [[ ${#PIDS[@]:-0} -gt 0 ]]; then
    for pid in "${PIDS[@]}"; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
      fi
    done
    for pid in "${PIDS[@]}"; do
      wait "$pid" >/dev/null 2>&1 || true
    done
  fi

  if [[ -d "${CONFIG_DIR}" ]]; then
    rm -rf "${CONFIG_DIR}"
  fi
}

trap cleanup EXIT INT TERM

declare -a PIDS=()

module_http_port() {
  case "$1" in
    pipeline-runtime-svc) echo 9000 ;;
    checkout-orchestrator-svc) echo 8080 ;;
    consumer-validation-orchestrator-svc) echo 8081 ;;
    restaurant-acceptance-orchestrator-svc) echo 8082 ;;
    kitchen-preparation-orchestrator-svc) echo 8083 ;;
    dispatch-orchestrator-svc) echo 8084 ;;
    delivery-execution-orchestrator-svc) echo 8085 ;;
    payment-capture-orchestrator-svc) echo 8086 ;;
    compensation-failure-orchestrator-svc) echo 8087 ;;
    *) echo "" ;;
  esac
}

module_grpc_port() {
  case "$1" in
    pipeline-runtime-svc) echo 9000 ;;
    checkout-orchestrator-svc) echo 18080 ;;
    consumer-validation-orchestrator-svc) echo 18081 ;;
    restaurant-acceptance-orchestrator-svc) echo 18082 ;;
    kitchen-preparation-orchestrator-svc) echo 18083 ;;
    dispatch-orchestrator-svc) echo 18084 ;;
    delivery-execution-orchestrator-svc) echo 18085 ;;
    payment-capture-orchestrator-svc) echo 18086 ;;
    compensation-failure-orchestrator-svc) echo 18087 ;;
    *) echo "" ;;
  esac
}

module_publication() {
  case "$1" in
    checkout-orchestrator-svc) echo tpfgo.checkout.order-pending.v1 ;;
    consumer-validation-orchestrator-svc) echo tpfgo.consumer.order-approved.v1 ;;
    restaurant-acceptance-orchestrator-svc) echo tpfgo.restaurant.order-accepted.v1 ;;
    kitchen-preparation-orchestrator-svc) echo tpfgo.kitchen.order-ready.v1 ;;
    dispatch-orchestrator-svc) echo tpfgo.dispatch.delivery-assigned.v1 ;;
    delivery-execution-orchestrator-svc) echo tpfgo.delivery.order-delivered.v1 ;;
    payment-capture-orchestrator-svc) echo tpfgo.payment.capture-result.v1 ;;
    compensation-failure-orchestrator-svc) echo tpfgo.compensation.terminal-state.v1 ;;
    *) echo "" ;;
  esac
}

function bind_target_port() {
  local module=$1

  case "$module" in
    checkout-orchestrator-svc) echo "$(module_grpc_port consumer-validation-orchestrator-svc)" ;;
    consumer-validation-orchestrator-svc) echo "$(module_grpc_port restaurant-acceptance-orchestrator-svc)" ;;
    restaurant-acceptance-orchestrator-svc) echo "$(module_grpc_port kitchen-preparation-orchestrator-svc)" ;;
    kitchen-preparation-orchestrator-svc) echo "$(module_grpc_port dispatch-orchestrator-svc)" ;;
    dispatch-orchestrator-svc) echo "$(module_grpc_port delivery-execution-orchestrator-svc)" ;;
    delivery-execution-orchestrator-svc) echo "$(module_grpc_port payment-capture-orchestrator-svc)" ;;
    payment-capture-orchestrator-svc) echo "$(module_grpc_port compensation-failure-orchestrator-svc)" ;;
    compensation-failure-orchestrator-svc) echo "" ;;
    pipeline-runtime-svc) echo "" ;;
    *) echo "" ;;
  esac
}

function internal_clients() {
  local module=$1
  case "$module" in
    checkout-orchestrator-svc)
      echo "process-checkout-validate-request process-checkout-create-pending"
      ;;
    consumer-validation-orchestrator-svc)
      echo "process-consumer-validate-order"
      ;;
    restaurant-acceptance-orchestrator-svc)
      echo "process-restaurant-accept-order"
      ;;
    kitchen-preparation-orchestrator-svc)
      echo "process-kitchen-expand-tasks process-kitchen-reduce-completion"
      ;;
    dispatch-orchestrator-svc)
      echo "process-dispatch-assign-courier"
      ;;
    delivery-execution-orchestrator-svc)
      echo "process-delivery-execute-order"
      ;;
    payment-capture-orchestrator-svc)
      echo "process-payment-capture-order"
      ;;
    compensation-failure-orchestrator-svc)
      echo "process-compensation-finalize-order"
      ;;
    *)
      echo ""
      ;;
  esac
}

write_service_props() {
  local module=$1
  local config_file=$2
  local runtime_port=$3
  local http_port=$4
  local grpc_port=$5
  local config_dir

  config_dir="$(dirname "${config_file}")"
  mkdir -p "${config_dir}"

  {
    echo "quarkus.http.host=127.0.0.1"
    echo "quarkus.http.port=${http_port}"
    echo "quarkus.grpc.server.host=127.0.0.1"
    echo "quarkus.grpc.server.port=${grpc_port}"
    echo "quarkus.otel.sdk.disabled=true"
    echo "pipeline.module.pipeline-runtime-svc.host=127.0.0.1"
    echo "pipeline.module.pipeline-runtime-svc.port=${runtime_port}"

    if [[ "$module" != "pipeline-runtime-svc" ]]; then
      echo "pipeline.orchestrator.mode=QUEUE_ASYNC"
      echo "pipeline.orchestrator.idempotency-policy=CLIENT_KEY_REQUIRED"
      echo "pipeline.orchestrator.state-provider=memory"
      echo "pipeline.orchestrator.dispatcher-provider=event"
      echo "pipeline.orchestrator.dlq-provider=log"
      echo "pipeline.orchestrator.retry-delay=PT0.25S"
      echo "pipeline.orchestrator.retry-multiplier=1.0"
      echo "pipeline.orchestrator.sweep-interval=PT1S"

      for client in $(internal_clients "$module"); do
        echo "quarkus.grpc.clients.${client}.host=127.0.0.1"
        echo "quarkus.grpc.clients.${client}.port=${runtime_port}"
        echo "quarkus.grpc.clients.${client}.plain-text=true"
      done

      local publication=$(module_publication "$module")
      local target=$(bind_target_port "$module")
      if [[ -n "$publication" ]]; then
        if [[ -n "$target" ]]; then
          local next_prefix="pipeline.handoff.bindings.\"${publication}\".targets.next"
          echo "${next_prefix}.kind=GRPC"
          echo "${next_prefix}.host=127.0.0.1"
          echo "${next_prefix}.port=${target}"
          echo "${next_prefix}.plaintext=true"
        fi

        local trace_prefix="pipeline.handoff.bindings.\"${publication}\".targets.trace"
        echo "${trace_prefix}.kind=HTTP"
        echo "${trace_prefix}.base-url=http://127.0.0.1:${runtime_port}"
        echo "${trace_prefix}.path=/checkout/journey/checkpoints"
        echo "${trace_prefix}.method=POST"
        echo "${trace_prefix}.encoding=JSON"
        echo "${trace_prefix}.content-type=application/json"
      fi
    fi
  } >"$config_file"
}

wait_for_health() {
  local module=$1
  local http_port=$2
  local deadline_seconds=120
  local start
  start=$(date +%s)

  while (( $(date +%s) - start < deadline_seconds )); do
    if curl -sf --connect-timeout 1 --max-time 2 -o /dev/null "http://127.0.0.1:${http_port}/q/health/ready" >/dev/null 2>&1; then
      echo "${module} ready on ${http_port}"
      return 0
    fi
    sleep 1
  done

  echo "${module} failed to become ready on HTTP port ${http_port}" >&2
  echo "--- ${module} log tail ---" >&2
  tail -n 120 "${LOG_DIR}/${module}.log" >&2
  exit 1
}

wait_for_http() {
  local label=$1
  local endpoint=$2
  local deadline_seconds=120
  local start
  start=$(date +%s)

  while (( $(date +%s) - start < deadline_seconds )); do
    if curl -sf --connect-timeout 1 --max-time 2 -o /dev/null "$endpoint" >/dev/null 2>&1; then
      echo "${label} ready on ${endpoint}"
      return 0
    fi
    sleep 1
  done

  echo "${label} failed to become ready at ${endpoint}" >&2
  exit 1
}

ensure_port_free() {
  if [[ "$SKIP_PORT_PRECHECK" == "true" ]]; then
    return
  fi

  local port=$1
  local name=$2
  local pids
  pids=$(lsof -tiTCP:"${port}" -sTCP:LISTEN -nP || true)
  if [[ -n "${pids}" ]]; then
    if [[ "$AUTO_KILL_PORT_CONFLICTS" == "true" ]]; then
      echo "Port ${port} is already in use for ${name}. Attempting to stop existing process(es): ${pids}"
      kill ${pids} || true
      sleep 2
      local still_busy
      still_busy=$(lsof -tiTCP:"${port}" -sTCP:LISTEN -nP || true)
      if [[ -n "${still_busy}" ]]; then
        echo "Port ${port} is still occupied for ${name} after cleanup attempt. Please stop manually: ${still_busy}"
        exit 1
      fi
      return
    fi

    echo "Port ${port} is already in use for ${name}."
    echo "Detected PIDs: ${pids}"
    echo "Stop stale services before starting this stack, for example:"
    echo "  kill ${pids}"
    exit 1
  fi
}

collect_known_ports() {
  local -a ports=()
  for module in "${MODULES[@]}"; do
    local http_port
    local grpc_port
    http_port="$(module_http_port "$module")"
    grpc_port="$(module_grpc_port "$module")"

    if [[ -n "$http_port" ]]; then
      ports+=("$http_port")
    fi

    if [[ -n "$grpc_port" ]]; then
      ports+=("$grpc_port")
    fi
  done

  # Deduplicate with stable ordering.
  printf '%s\n' "${ports[@]}" | sort -nu
}

clean_known_ports() {
  local port
  local pids
  local remaining_pids
  while IFS= read -r port; do
    [[ -z "$port" ]] && continue
    pids=$(lsof -tiTCP:"${port}" -sTCP:LISTEN -nP || true)
    if [[ -z "${pids}" ]]; then
      continue
    fi

    echo "Port ${port} is already in use. Stopping process(es): ${pids}"
    kill ${pids} || true
  done < <(collect_known_ports)

  sleep 2

  local still_busy=false
  while IFS= read -r port; do
    [[ -z "$port" ]] && continue
    remaining_pids=$(lsof -tiTCP:"${port}" -sTCP:LISTEN -nP || true)
    if [[ -n "${remaining_pids}" ]]; then
      echo "Port ${port} is still in use: ${remaining_pids}" >&2
      still_busy=true
    fi
  done < <(collect_known_ports)

  if [[ "$still_busy" == "true" ]]; then
    echo "Unable to clean all checkout stack ports. Set SKIP_PORT_PRECHECK=true and stop services manually if needed."
    exit 1
  fi
}

if [[ "$CLEAN_PORTS" == "true" ]]; then
  if [[ "$SKIP_PORT_PRECHECK" == "true" ]]; then
    echo "--clean-ports requires lsof; disable by unsetting SKIP_PORT_PRECHECK or set it to false."
    exit 1
  fi
  clean_known_ports
fi

start_nextjs_ui() {
  local ui_dir="${CHECKOUT_DIR}/nextjs-ui"
  local ui_port="${TPF_UI_PORT:-3000}"
  local ui_log="${LOG_DIR}/nextjs-ui.log"
  local ui_base_url="${TPF_BASE_URL:-http://127.0.0.1:8080}"
  local ui_runtime_base_url="${TPF_RUNTIME_BASE_URL:-http://127.0.0.1:$(module_http_port pipeline-runtime-svc)}"

  if [[ ! -d "${ui_dir}" ]]; then
    echo "Checkout UI directory missing: ${ui_dir}" >&2
    exit 1
  fi

  if [[ ! -d "${ui_dir}/node_modules" ]]; then
    echo "Checkout UI dependencies not installed: ${ui_dir}/node_modules" >&2
    echo "Run: npm --prefix ${ui_dir} install" >&2
    exit 1
  fi

  echo "Starting checkout Next.js UI (http=${ui_port}, baseUrl=${ui_base_url}, runtimeBaseUrl=${ui_runtime_base_url})"
  (
    cd "$ui_dir"
    TPF_BASE_URL="$ui_base_url" \
    TPF_RUNTIME_BASE_URL="$ui_runtime_base_url" \
    TPF_TENANT_ID="${TPF_TENANT_ID:-default}" \
    TPF_GRPC_HOST="${TPF_GRPC_HOST:-}" \
    TPF_GRPC_PORT="${TPF_GRPC_PORT:-}" \
    TPF_GRPC_PROTO_DIR="${TPF_GRPC_PROTO_DIR:-}" \
    npm run dev -- --hostname 127.0.0.1 --port "$ui_port"
  ) >"${ui_log}" 2>&1 &

  local pid=$!
  PIDS+=("$pid")
  wait_for_http "checkout-nextjs-ui" "http://127.0.0.1:${ui_port}"
}

start_module() {
  local module=$1
  local http_port=$2
  local grpc_port=$3
  local module_dir="${CHECKOUT_DIR}/${module}"
  local artifact="${module_dir}/target/quarkus-app/quarkus-run.jar"
  local config_file="${CONFIG_DIR}/${module}.properties"

  if [[ ! -f "$artifact" ]]; then
    echo "Build artifact missing for ${module}: ${artifact}" >&2
    echo "Run with --skip-build disabled (default) or build this module first." >&2
    exit 1
  fi

  ensure_port_free "${http_port}" "${module} (HTTP)"
  ensure_port_free "${grpc_port}" "${module} (gRPC)"

  local runtime_port="$(module_grpc_port pipeline-runtime-svc)"
  write_service_props "$module" "$config_file" "$runtime_port" "$http_port" "$grpc_port"

  echo "Starting ${module} (http=${http_port} grpc=${grpc_port})"
  (
    cd "$module_dir"
    QUARKUS_CONFIG_LOCATIONS="$config_file" \
      java -jar target/quarkus-app/quarkus-run.jar
  ) >"${LOG_DIR}/${module}.log" 2>&1 &

  local pid=$!
  PIDS+=("$pid")
  wait_for_health "$module" "$http_port"
}

build_modules() {
  echo "Using Maven local repository: ${TPF_MAVEN_REPO_LOCAL}"
  echo "Installing root parent POM into isolated Maven repository..."
  "$MVN_BIN" "${MVN_REPO_ARG}" -N -f "${REPO_ROOT}/pom.xml" \
    -DskipTests \
    install

  echo "Bootstrapping framework artifacts into isolated Maven repository..."
  "$MVN_BIN" "${MVN_REPO_ARG}" -f "${REPO_ROOT}/framework/pom.xml" \
    -DskipTests \
    install

  echo "Building checkout modules..."
  "$MVN_BIN" "${MVN_REPO_ARG}" -f "${CHECKOUT_DIR}/pom.xml" \
    -pl pipeline-runtime-svc,checkout-orchestrator-svc,consumer-validation-orchestrator-svc,restaurant-acceptance-orchestrator-svc,kitchen-preparation-orchestrator-svc,dispatch-orchestrator-svc,delivery-execution-orchestrator-svc,payment-capture-orchestrator-svc,compensation-failure-orchestrator-svc \
    -am \
    -DskipTests \
    package
}

if [[ "$SKIP_BUILD" != "true" ]]; then
  build_modules
fi

for module in "${MODULES[@]}"; do
  start_module "$module" \
    "$(module_http_port "$module")" \
    "$(module_grpc_port "$module")"
done

if [[ "$START_UI" == "true" ]]; then
  start_nextjs_ui
fi

cat <<EOF

Checkout services are running.
Checkout entrypoint: http://127.0.0.1:$(module_http_port checkout-orchestrator-svc)
Pipeline runtime:    http://127.0.0.1:$(module_http_port pipeline-runtime-svc)

Useful env:
  export TPF_BASE_URL=http://127.0.0.1:$(module_http_port checkout-orchestrator-svc)

Log files: ${LOG_DIR}/*.log
EOF

if [[ "$START_UI" == "true" ]]; then
  echo "Checkout UI:        http://127.0.0.1:${TPF_UI_PORT:-3000}"
  echo "UI config:          TPF_BASE_URL=${TPF_BASE_URL:-http://127.0.0.1:8080}"
  echo "                    TPF_RUNTIME_BASE_URL=${TPF_RUNTIME_BASE_URL:-http://127.0.0.1:$(module_http_port pipeline-runtime-svc)}"
fi
echo "Maven local repo:   ${TPF_MAVEN_REPO_LOCAL}"

if [[ -n "$RUN_COMMAND" ]]; then
  echo "Command mode is active; services will stop when the command exits."
else
  echo "Stop with Ctrl+C (all services will terminate)."
fi

if [[ -n "$RUN_COMMAND" ]]; then
  echo ""
  echo "Running command: $RUN_COMMAND"
  if (cd "$REPO_ROOT" && MAVEN_OPTS="${MAVEN_OPTS:-} ${MVN_REPO_ARG}" bash -lc "$RUN_COMMAND"); then
    echo ""
    echo "Command completed successfully: $RUN_COMMAND"
  else
    echo ""
    echo "Command failed: $RUN_COMMAND" >&2
    exit 1
  fi
else
  wait
fi
