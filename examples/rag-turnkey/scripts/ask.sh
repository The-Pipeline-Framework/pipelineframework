#!/usr/bin/env bash
set -euo pipefail
question="${1:?question required}"
curl --fail --silent --show-error -X POST http://localhost:8081/pipeline/run \
  -H 'Content-Type: application/json' \
  -d "{\"questionId\":\"local-question\",\"text\":$(printf '%s' "$question" | jq -Rs .)}"
