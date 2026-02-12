#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $0 <module> <test-pattern> [focus-package]

Examples:
  $0 runtime FunctionTransportBridgeTest,UnaryFunctionTransportBridgeTest
  $0 deployment RestFunctionHandlerRendererTest
  $0 runtime FunctionTransportBridgeTest org/pipelineframework/transport/function

Notes:
  - module must be one of: runtime, deployment
  - test-pattern is passed to -Dtest (Surefire syntax)
  - focus-package is optional (defaults to FOCUS_PACKAGE env var,
    then org/pipelineframework/transport/function)
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

MODULE="$1"
TEST_PATTERN="$2"
FOCUS_PACKAGE="${3:-${FOCUS_PACKAGE:-org/pipelineframework/transport/function}}"

case "$MODULE" in
  runtime|deployment)
    ;;
  *)
    echo "Unsupported module: $MODULE (expected runtime or deployment)" >&2
    exit 2
    ;;
esac

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FRAMEWORK_DIR="$ROOT_DIR/framework"
MODULE_DIR="$FRAMEWORK_DIR/$MODULE"
EXEC_FILE="$MODULE_DIR/target/jacoco.exec"
REPORT_XML="$MODULE_DIR/target/site/jacoco/jacoco.xml"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.12}"
HOME_DIR="${HOME:-}"
if [[ -z "$HOME_DIR" ]]; then
  echo "HOME is not set; cannot resolve local Maven repository for JaCoCo agent." >&2
  exit 5
fi
JACOCO_AGENT_JAR="${HOME_DIR}/.m2/repository/org/jacoco/org.jacoco.agent/${JACOCO_VERSION}/org.jacoco.agent-${JACOCO_VERSION}-runtime.jar"

rm -f "$EXEC_FILE"

if [[ ! -f "$JACOCO_AGENT_JAR" ]]; then
  echo "JaCoCo agent not found; downloading ${JACOCO_VERSION}..." >&2
  (
    cd "$ROOT_DIR"
    ./mvnw -q org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get \
      -Dartifact=org.jacoco:org.jacoco.agent:${JACOCO_VERSION}:jar:runtime
  )
fi

(
  cd "$ROOT_DIR"
  ./mvnw -f framework/pom.xml \
    -pl "$MODULE" \
    -Dtest="$TEST_PATTERN" \
    -DsurefireArgLine="-javaagent:${JACOCO_AGENT_JAR}=destfile=${EXEC_FILE},append=false" \
    test \
    org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report \
    -Djacoco.dataFile="$EXEC_FILE"
)

if [[ ! -f "$REPORT_XML" ]]; then
  echo "Coverage XML was not generated: $REPORT_XML" >&2
  exit 3
fi

extract_counter_pair() {
  local type="$1"
  COV_TYPE="$type" perl -0777 -ne '
    my $ctype = $ENV{"COV_TYPE"};
    my $missed;
    my $covered;
    # Use only root-level <report> counters (exclude per-package/class/method counters).
    my ($root_block) = $_ =~ /<report\b[^>]*>(.*?)(?:<package\b|<\/report>)/s;
    $root_block = $_ unless defined $root_block;
    while ($root_block =~ /<counter type="\Q$ctype\E" missed="([0-9]+)" covered="([0-9]+)"\/>/g) {
      $missed = $1;
      $covered = $2;
    }
    if (defined $missed && defined $covered) {
      print "$missed $covered\n";
    }
  ' "$REPORT_XML"
}

extract_package_counter_pair() {
  local package_name="$1"
  local type="$2"
  COV_PACKAGE="$package_name" COV_TYPE="$type" perl -0777 -ne '
    my $pkgname = $ENV{"COV_PACKAGE"};
    my $ctype = $ENV{"COV_TYPE"};
    my $missed;
    my $covered;
    if (/<package name="\Q$pkgname\E">(.*?)<\/package>/s) {
      my $pkg = $1;
      while ($pkg =~ /<counter type="\Q$ctype\E" missed="([0-9]+)" covered="([0-9]+)"\/>/g) {
        $missed = $1;
        $covered = $2;
      }
      if (defined $missed && defined $covered) {
        print "$missed $covered\n";
      }
    }
  ' "$REPORT_XML"
}

print_coverage_line() {
  local label="$1"
  local covered="$2"
  local missed="$3"
  local total=$((covered + missed))
  local pct
  if [[ $total -eq 0 ]]; then
    pct="0.00"
  else
    pct="$(awk -v c="$covered" -v t="$total" 'BEGIN { printf "%.2f", (100*c)/t }')"
  fi
  echo "$label: $covered/$total (${pct}%)"
}

print_counter_from_pair() {
  local label="$1"
  local pair="$2"
  if [[ -n "$pair" ]]; then
    local missed="${pair%% *}"
    local covered="${pair##* }"
    if [[ -n "$missed" && -n "$covered" ]]; then
      print_coverage_line "$label" "$covered" "$missed"
    fi
  fi
}

line_pair="$(extract_counter_pair LINE)"
branch_pair="$(extract_counter_pair BRANCH || true)"
method_pair="$(extract_counter_pair METHOD || true)"
class_pair="$(extract_counter_pair CLASS || true)"

line_missed="${line_pair%% *}"
line_covered="${line_pair##* }"

if [[ -z "$line_missed" || -z "$line_covered" ]]; then
  echo "Could not parse LINE coverage counters from $REPORT_XML" >&2
  exit 4
fi

echo "Module: $MODULE"
echo "Tests : $TEST_PATTERN"
echo "Report: $REPORT_XML"
print_coverage_line "LINE  " "$line_covered" "$line_missed"
print_counter_from_pair "BRANCH" "$branch_pair"
print_counter_from_pair "METHOD" "$method_pair"
print_counter_from_pair "CLASS " "$class_pair"

package_line_pair="$(extract_package_counter_pair "$FOCUS_PACKAGE" LINE || true)"
package_branch_pair="$(extract_package_counter_pair "$FOCUS_PACKAGE" BRANCH || true)"
package_method_pair="$(extract_package_counter_pair "$FOCUS_PACKAGE" METHOD || true)"
package_class_pair="$(extract_package_counter_pair "$FOCUS_PACKAGE" CLASS || true)"
if [[ -n "$package_line_pair" || -n "$package_branch_pair" || -n "$package_method_pair" || -n "$package_class_pair" ]]; then
  echo "Package: $FOCUS_PACKAGE"
  print_counter_from_pair "LINE  " "$package_line_pair"
  print_counter_from_pair "BRANCH" "$package_branch_pair"
  print_counter_from_pair "METHOD" "$package_method_pair"
  print_counter_from_pair "CLASS " "$package_class_pair"
fi
