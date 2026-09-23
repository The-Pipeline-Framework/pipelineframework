#!/usr/bin/env bash
set -euo pipefail

suite=${1:?usage: run-coordination-suite.sh compatibility}
read -r -a maven_args <<< "${MAVEN_ARGS:-}" || true

case "$suite" in
  compatibility)
    ./mvnw -B "${maven_args[@]}" -f framework/pom.xml clean verify --no-transfer-progress
    ;;
  *)
    echo "Unknown coordination system-test suite: $suite" >&2
    exit 2
    ;;
esac
