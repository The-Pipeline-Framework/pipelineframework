#!/usr/bin/env bash
set -euo pipefail
question="${1:?question required}"
query_url="${QUERY_URL:-http://localhost:8081}"
curl --fail --silent --show-error -X POST "${query_url}/pipeline/run" \
  -H 'Content-Type: application/json' \
  -d "{\"questionId\":\"local-question\",\"text\":$(printf '%s' "$question" | jq -Rs .)}"
