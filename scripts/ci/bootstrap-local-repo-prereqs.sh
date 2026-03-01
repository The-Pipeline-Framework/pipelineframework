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

install_framework_and_plugins() {
  mvn -N install
  mvn -f "$ROOT_DIR/framework/pom.xml" clean install -DskipTests
  mvn -f "$ROOT_DIR/plugins/foundational/persistence/pom.xml" clean install -DskipTests
  mvn -f "$ROOT_DIR/plugins/foundational/cache/pom.xml" clean install -DskipTests
}

install_csv_prereqs() {
  mvn -f "$ROOT_DIR/examples/csv-payments/pom.xml" -N install
  mvn -f "$ROOT_DIR/plugins/foundational/persistence/pom.xml" -DskipTests install
}

install_search_prereqs() {
  mvn -f "$ROOT_DIR/examples/search/pom.xml" -N install
  mvn -f "$ROOT_DIR/plugins/foundational/cache/pom.xml" -DskipTests install
}

case "$SCOPE" in
  framework)
    install_framework_and_plugins
    ;;
  csv)
    install_csv_prereqs
    ;;
  search)
    install_search_prereqs
    ;;
  all)
    install_framework_and_plugins
    install_csv_prereqs
    install_search_prereqs
    ;;
  *)
    echo "Unknown scope '$SCOPE'. Use one of: framework, csv, search, all." >&2
    exit 1
    ;;
esac
