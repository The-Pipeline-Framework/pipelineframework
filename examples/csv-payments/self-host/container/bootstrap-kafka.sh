#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMPOSE_KAFKA_FILE="${SCRIPT_DIR}/compose.kafka.yaml"

export TPF_REPO_ROOT="${TPF_REPO_ROOT:-${REPO_ROOT}}"
export TPF_KAFKA_PORT="${TPF_KAFKA_PORT:-9093}"

compose() {
  docker compose -f "${COMPOSE_FILE}" -f "${COMPOSE_KAFKA_FILE}" "$@"
}

wait_for_kafka() {
  local timeout_seconds="${KAFKA_WAIT_SECONDS:-180}"
  echo "Waiting for Kafka..."
  for _ in $(seq 1 "${timeout_seconds}"); do
    if compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "Timed out waiting for Kafka after ${timeout_seconds}s." >&2
  compose logs kafka >&2 || true
  exit 1
}

create_topic_if_missing() {
  local topic="$1"
  if compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:19092 \
      --describe \
      --topic "${topic}" >/dev/null 2>&1; then
    echo "Kafka topic exists: ${topic}"
    return
  fi
  echo "Creating Kafka topic: ${topic}"
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 1 \
    --replication-factor 1 >/dev/null
}

compose up -d kafka
wait_for_kafka
create_topic_if_missing csv-payments.payment.requests
create_topic_if_missing csv-payments.payment.results

echo "CSV Kafka bootstrap complete."
