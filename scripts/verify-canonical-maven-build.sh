#!/usr/bin/env bash
set -euo pipefail

reject_pom_matches() {
  local message=$1
  local expression=$2
  local matches
  matches=$(find . \
    \( -name .git -o -name .m2 -o -name target \) -prune -o \
    -type f -name pom.xml -exec grep -nE "$expression" {} + || true)
  if [[ -n $matches ]]; then
    printf '%s\n' "$matches"
    echo "$message" >&2
    exit 1
  fi
}

reject_source_argument_matches() {
  local message=$1
  local expression=$2
  local matches
  matches=$(find . \
    \( -name .git -o -name .m2 -o -name target -o -name node_modules -o -name .repowise \) -prune -o \
    -type f \( -name '*.yml' -o -name '*.yaml' -o -name '*.sh' -o -name '*.java' -o -name '*.xml' -o -name '*.md' -o -name '*.properties' \) \
    -exec grep -nE "$expression" {} + || true)
  if [[ -n $matches ]]; then
    printf '%s\n' "$matches"
    echo "$message" >&2
    exit 1
  fi
}

reject_pom_matches "CSV source-selection Maven migration machinery is not allowed." \
  '<id>[[:space:]]*csv-v[0-9]+-(persistence[^<[:space:]]*|default-sources)[[:space:]]*</id>|<name>[[:space:]]*csv\.v[0-9]+\.persistence[[:space:]]*</name>'
reject_source_argument_matches "CSV source-selection build arguments are not allowed." \
  '-Dcsv\.v[0-9]+\.persistence([^[:alnum:]_.-]|$)|-P!?csv-v[0-9]+-(persistence[^[:space:]]*|default-sources)([^[:alnum:]_.-]|$)'
