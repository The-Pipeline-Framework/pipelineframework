#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"

export TPF_REPO_ROOT="${TPF_REPO_ROOT:-${REPO_ROOT}}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

compose_up() {
  if [[ "${TPF_CI_QUIET:-false}" == "true" ]]; then
    compose up --quiet-pull "$@"
    return
  fi
  compose up "$@"
}

awslocal() {
  compose exec -T localstack awslocal "$@"
}

wait_for_localstack() {
  echo "Waiting for LocalStack..."
  for _ in {1..120}; do
    if compose exec -T localstack curl -fsS "http://localhost:4566/_localstack/health" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "Timed out waiting for LocalStack." >&2
  compose logs localstack >&2 || true
  exit 1
}

create_table_if_missing() {
  local table_name="$1"
  shift
  if awslocal dynamodb describe-table --table-name "${table_name}" >/dev/null 2>&1; then
    echo "DynamoDB table exists: ${table_name}"
    return
  fi
  echo "Creating DynamoDB table: ${table_name}"
  awslocal dynamodb create-table --table-name "${table_name}" "$@" >/dev/null
  awslocal dynamodb wait table-exists --table-name "${table_name}"
}

create_queue_if_missing() {
  local queue_name="$1"
  if awslocal sqs get-queue-url --queue-name "${queue_name}" >/dev/null 2>&1; then
    echo "SQS queue exists: ${queue_name}"
    return
  fi
  echo "Creating SQS queue: ${queue_name}"
  awslocal sqs create-queue --queue-name "${queue_name}" >/dev/null
}

create_bucket_if_missing() {
  local bucket="$1"
  if awslocal s3api head-bucket --bucket "${bucket}" >/dev/null 2>&1; then
    echo "S3 bucket exists: ${bucket}"
    return
  fi
  echo "Creating S3 bucket: ${bucket}"
  awslocal s3api create-bucket --bucket "${bucket}" >/dev/null
}

compose_up -d localstack
wait_for_localstack

create_table_if_missing tpf_execution \
  --attribute-definitions \
    AttributeName=tenant_id,AttributeType=S \
    AttributeName=execution_id,AttributeType=S \
  --key-schema \
    AttributeName=tenant_id,KeyType=HASH \
    AttributeName=execution_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_execution_key \
  --attribute-definitions AttributeName=tenant_execution_key,AttributeType=S \
  --key-schema AttributeName=tenant_execution_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_unit \
  --attribute-definitions \
    AttributeName=tenant_id,AttributeType=S \
    AttributeName=unit_id,AttributeType=S \
  --key-schema \
    AttributeName=tenant_id,KeyType=HASH \
    AttributeName=unit_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_interaction \
  --attribute-definitions \
    AttributeName=tenant_id,AttributeType=S \
    AttributeName=interaction_id,AttributeType=S \
    AttributeName=query_unit_key,AttributeType=S \
    AttributeName=query_unit_sort,AttributeType=S \
    AttributeName=query_pending_tenant_key,AttributeType=S \
    AttributeName=query_pending_assignee_key,AttributeType=S \
    AttributeName=query_pending_group_key,AttributeType=S \
    AttributeName=query_pending_step_key,AttributeType=S \
    AttributeName=query_pending_deadline_sort,AttributeType=S \
    AttributeName=query_deadline_key,AttributeType=S \
    AttributeName=query_deadline_sort,AttributeType=S \
  --key-schema \
    AttributeName=tenant_id,KeyType=HASH \
    AttributeName=interaction_id,KeyType=RANGE \
  --global-secondary-indexes \
    'IndexName=await-interaction-by-unit,KeySchema=[{AttributeName=query_unit_key,KeyType=HASH},{AttributeName=query_unit_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-tenant,KeySchema=[{AttributeName=query_pending_tenant_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-assignee,KeySchema=[{AttributeName=query_pending_assignee_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-group,KeySchema=[{AttributeName=query_pending_group_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-step,KeySchema=[{AttributeName=query_pending_step_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-deadline,KeySchema=[{AttributeName=query_deadline_key,KeyType=HASH},{AttributeName=query_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_interaction_key \
  --attribute-definitions AttributeName=lookup_key,AttributeType=S \
  --key-schema AttributeName=lookup_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_release_registry \
  --attribute-definitions \
    AttributeName=registry_key,AttributeType=S \
    AttributeName=registry_sort,AttributeType=S \
  --key-schema \
    AttributeName=registry_key,KeyType=HASH \
    AttributeName=registry_sort,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_worker_registry \
  --attribute-definitions \
    AttributeName=registry_key,AttributeType=S \
    AttributeName=registry_sort,AttributeType=S \
  --key-schema \
    AttributeName=registry_key,KeyType=HASH \
    AttributeName=registry_sort,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_queue_if_missing tpf-work
create_queue_if_missing tpf-execution-dlq
create_bucket_if_missing tpf-release-artifacts

echo "LocalStack bootstrap complete."
