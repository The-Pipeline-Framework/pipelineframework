#!/bin/sh

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
state_dir="$(mktemp -d)"
trap 'rm -rf "$state_dir"' EXIT HUP INT TERM

test_repository="$state_dir/repository"
verify_state="$state_dir/verify-state"
run_state="$state_dir/run-state"
mkdir -p "$test_repository/tools" "$run_state"
cp "$repository_root/tools/full-verify.sh" "$test_repository/tools/full-verify.sh"
cat >"$test_repository/mvnw" <<'EOF'
#!/bin/sh
set -eu
: "${TPF_VERIFY_TEST_RUN_DIR:?}"
: >"$TPF_VERIFY_TEST_RUN_DIR/invocation-$$"
: >"$TPF_VERIFY_TEST_RUN_DIR/started"
while [ ! -f "$TPF_VERIFY_TEST_RUN_DIR/release" ]; do
  sleep 1
done
EOF
chmod +x "$test_repository/mvnw" "$test_repository/tools/full-verify.sh"

TPF_VERIFY_STATE_DIR="$verify_state" TPF_VERIFY_TEST_RUN_DIR="$run_state" \
  "$test_repository/tools/full-verify.sh" start >"$state_dir/start-one.out" &
first_start=$!
TPF_VERIFY_STATE_DIR="$verify_state" TPF_VERIFY_TEST_RUN_DIR="$run_state" \
  "$test_repository/tools/full-verify.sh" start >"$state_dir/start-two.out" &
second_start=$!
wait "$first_start"
wait "$second_start"

for ignored in 1 2 3 4 5; do
  [ -f "$run_state/started" ] && break
  sleep 1
done
[ -f "$run_state/started" ] || {
  printf 'full verification did not start\n' >&2
  exit 1
}
invocation_count="$(find "$run_state" -name 'invocation-*' -type f | wc -l | tr -d ' ')"
[ "$invocation_count" -eq 1 ] || {
  printf 'expected one full verification invocation, found %s\n' "$invocation_count" >&2
  exit 1
}

: >"$run_state/release"
for ignored in 1 2 3 4 5; do
  [ -f "$verify_state/exit-status" ] && break
  sleep 1
done
[ -f "$verify_state/exit-status" ] || {
  printf 'full verification did not publish completion\n' >&2
  exit 1
}

completed="$(TPF_VERIFY_STATE_DIR="$verify_state" "$test_repository/tools/full-verify.sh" status)"
[ "$completed" = 'TPF full verify: all went well' ] || {
  printf 'completion report failed: %s\n' "$completed" >&2
  exit 1
}
