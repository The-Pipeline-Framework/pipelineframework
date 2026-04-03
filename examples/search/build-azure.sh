#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SEARCH_DIR="$ROOT_DIR/examples/search"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/mvnw}"

if [[ ! -e "$MVN_BIN" ]]; then
  echo "Maven launcher not found: $MVN_BIN" >&2
  exit 1
fi

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven launcher is not executable: $MVN_BIN" >&2
  exit 1
fi

# Verify bootstrap script exists and is executable
BOOTSTRAP_SCRIPT="$ROOT_DIR/scripts/ci/bootstrap-local-repo-prereqs.sh"
if [[ ! -f "$BOOTSTRAP_SCRIPT" ]]; then
  echo "ERROR: Bootstrap script not found: $BOOTSTRAP_SCRIPT" >&2
  exit 1
fi

if [[ ! -x "$BOOTSTRAP_SCRIPT" ]]; then
  echo "ERROR: Bootstrap script is not executable: $BOOTSTRAP_SCRIPT" >&2
  exit 1
fi

# Verify POM exists
if [[ ! -f "$SEARCH_DIR/pom.xml" ]]; then
  echo "ERROR: POM file not found: $SEARCH_DIR/pom.xml" >&2
  exit 1
fi

PIPELINE_PLATFORM="${PIPELINE_PLATFORM:-FUNCTION}"
PIPELINE_TRANSPORT="${PIPELINE_TRANSPORT:-REST}"
PIPELINE_REST_NAMING_STRATEGY="${PIPELINE_REST_NAMING_STRATEGY:-RESOURCEFUL}"
PIPELINE_AZURE_DEPENDENCY_SCOPE="${PIPELINE_AZURE_DEPENDENCY_SCOPE:-provided}"

# Ensure module parent POM and foundational plugin coordinates are resolvable in clean local repositories.
"$ROOT_DIR/scripts/ci/bootstrap-local-repo-prereqs.sh" search

"$MVN_BIN" -f "$SEARCH_DIR/pom.xml" \
  -Dtpf.build.platform="$PIPELINE_PLATFORM" \
  -Dtpf.build.transport="$PIPELINE_TRANSPORT" \
  -Dtpf.build.rest.naming.strategy="$PIPELINE_REST_NAMING_STRATEGY" \
  -Dtpf.build.azure.scope="$PIPELINE_AZURE_DEPENDENCY_SCOPE" \
  -Dpipeline.function.provider=azure \
  -Dquarkus.profile=azure-functions \
  clean install "$@"
