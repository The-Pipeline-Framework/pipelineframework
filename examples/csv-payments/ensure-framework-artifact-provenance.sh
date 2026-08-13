#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MAVEN_REPOSITORY="${MAVEN_REPOSITORY:-$ROOT_DIR/.m2/repository}"
PROVENANCE_FILE="$MAVEN_REPOSITORY/org/pipelineframework/tpf-worktree-provenance.properties"

hash_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

framework_source_fingerprint() {
  git -C "$ROOT_DIR" ls-files --cached --others --exclude-standard -- \
      framework pom.xml mvnw .mvn |
    LC_ALL=C sort -u |
    while IFS= read -r path; do
      if [[ -f "$ROOT_DIR/$path" ]]; then
        printf '%s\0%s\n' "$path" "$(git -C "$ROOT_DIR" hash-object "$ROOT_DIR/$path")"
      else
        printf '%s\0DELETED\n' "$path"
      fi
    done |
    git -C "$ROOT_DIR" hash-object --stdin
}

FRAMEWORK_COMMIT="$(git -C "$ROOT_DIR" rev-parse HEAD)"
FRAMEWORK_SOURCE_FINGERPRINT="$(framework_source_fingerprint)"
FRAMEWORK_VERSION="$(
  "$ROOT_DIR/examples/csv-payments/resolve-framework-version.sh" \
    "$ROOT_DIR/mvnw" "$ROOT_DIR/framework/pom.xml" "$MAVEN_REPOSITORY"
)"
FRAMEWORK_RUNTIME_JAR="$MAVEN_REPOSITORY/org/pipelineframework/pipelineframework/${FRAMEWORK_VERSION}/pipelineframework-${FRAMEWORK_VERSION}.jar"

property_value() {
  local key="$1"
  sed -n "s/^${key}=//p" "$PROVENANCE_FILE"
}

if [[ -f "$PROVENANCE_FILE" && -f "$FRAMEWORK_RUNTIME_JAR" \
  && "$(property_value 'framework.version')" == "$FRAMEWORK_VERSION" \
  && "$(property_value 'framework.commit')" == "$FRAMEWORK_COMMIT" \
  && "$(property_value 'framework.source.fingerprint')" == "$FRAMEWORK_SOURCE_FINGERPRINT" \
  && "$(property_value 'framework.runtime.sha256')" == "$(hash_file "$FRAMEWORK_RUNTIME_JAR")" ]]; then
  echo "Reusing current framework artifact provenance:"
  sed 's/^/  /' "$PROVENANCE_FILE"
  exit 0
fi

echo "Installing framework ${FRAMEWORK_VERSION} from ${FRAMEWORK_COMMIT} (${FRAMEWORK_SOURCE_FINGERPRINT})"
"$ROOT_DIR/mvnw" -f "$ROOT_DIR/framework/pom.xml" install \
  -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$MAVEN_REPOSITORY"

if [[ ! -f "$FRAMEWORK_RUNTIME_JAR" ]]; then
  echo "Installed framework runtime JAR not found: $FRAMEWORK_RUNTIME_JAR" >&2
  exit 1
fi
FRAMEWORK_RUNTIME_SHA256="$(hash_file "$FRAMEWORK_RUNTIME_JAR")"

mkdir -p "$(dirname "$PROVENANCE_FILE")"
{
  printf 'framework.version=%s\n' "$FRAMEWORK_VERSION"
  printf 'framework.commit=%s\n' "$FRAMEWORK_COMMIT"
  printf 'framework.source.fingerprint=%s\n' "$FRAMEWORK_SOURCE_FINGERPRINT"
  printf 'framework.runtime.sha256=%s\n' "$FRAMEWORK_RUNTIME_SHA256"
} > "$PROVENANCE_FILE"

echo "Framework artifact provenance:"
sed 's/^/  /' "$PROVENANCE_FILE"
