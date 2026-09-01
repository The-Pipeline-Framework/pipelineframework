#!/bin/sh

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
state_dir="$(mktemp -d)"
trap 'rm -rf "$state_dir"' EXIT HUP INT TERM

printf '%s\n' "$$" >"$state_dir/pid"
running="$(TPF_VERIFY_STATE_DIR="$state_dir" "$repository_root/tools/full-verify.sh" start)"
case "$running" in
*'already running'*) ;;
*) printf 'duplicate-run guard failed: %s\n' "$running" >&2; exit 1 ;;
esac

rm -f "$state_dir/pid"
printf '%s\n' 'TPF full verify: all went well' >"$state_dir/verify.log"
printf '%s\n' 0 >"$state_dir/exit-status"
completed="$(TPF_VERIFY_STATE_DIR="$state_dir" "$repository_root/tools/full-verify.sh" status)"
[ "$completed" = 'TPF full verify: all went well' ] || {
  printf 'completion report failed: %s\n' "$completed" >&2
  exit 1
}
