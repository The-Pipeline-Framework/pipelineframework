#!/usr/bin/env bash
set -euo pipefail

reject_matches() {
  local message=$1
  shift
  set +e
  rg -n "$@"
  local status=$?
  set -e
  if [[ $status -eq 0 ]]; then
    echo "$message" >&2
    exit 1
  fi
  if [[ $status -gt 1 ]]; then
    echo "Migration guard search failed: $message" >&2
    exit "$status"
  fi
}

reject_matches "CSV source-selection Maven migration machinery is not allowed." \
  --hidden --glob 'pom.xml' '<id>\s*csv-v[0-9]+-(persistence|default-sources)\s*</id>|<name>\s*csv\.v[0-9]+\.persistence\s*</name>' .
reject_matches "CSV source-selection build arguments are not allowed." \
  --hidden --glob '*.yml' --glob '*.yaml' --glob '*.sh' --glob '*.java' --glob '*.xml' -- '-Dcsv\.v[0-9]+\.persistence=' .github examples/csv-payments scripts
