#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MAVEN_EXECUTABLE="${1:-$ROOT_DIR/mvnw}"
FRAMEWORK_POM="${2:-$ROOT_DIR/framework/pom.xml}"
MAVEN_REPOSITORY="${3:-${MAVEN_REPOSITORY:-$ROOT_DIR/.m2/repository}}"

# CI supplies -V through MAVEN_ARGS. Maven writes that version banner to stdout,
# which must not become part of the evaluated project version.
FRAMEWORK_VERSION="$(
  MAVEN_ARGS= "$MAVEN_EXECUTABLE" -q -f "$FRAMEWORK_POM" help:evaluate \
    -Dexpression=project.version -DforceStdout \
    -Dmaven.repo.local="$MAVEN_REPOSITORY"
)"

if [[ -z "$FRAMEWORK_VERSION" || "$FRAMEWORK_VERSION" =~ [[:space:]] ]]; then
  echo "Unable to resolve a single framework project version: $FRAMEWORK_VERSION" >&2
  exit 1
fi

printf '%s\n' "$FRAMEWORK_VERSION"
