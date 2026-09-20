#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/mvnw}"
SCOPE="${1:-all}"

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven launcher is not executable: $MVN_BIN" >&2
  exit 1
fi

maven_args=()
if [[ -n "${MAVEN_ARGS:-}" ]]; then
  read -r -a maven_args <<< "$MAVEN_ARGS"
fi

mvn() {
  if ((${#maven_args[@]})); then
    "$MVN_BIN" "${maven_args[@]}" "$@"
  else
    "$MVN_BIN" "$@"
  fi
}

install_framework_plugins_and_connectors() {
  mvn -N install -Dgpg.skip
  mvn -f "$ROOT_DIR/framework/pom.xml" clean install -DskipTests -Dgpg.skip
}

case "$SCOPE" in
  framework)
    install_framework_plugins_and_connectors
    ;;
  all)
    install_framework_plugins_and_connectors
    ;;
  *)
    echo "Unknown scope '$SCOPE'. Use one of: framework, all." >&2
    exit 1
    ;;
esac
