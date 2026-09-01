#!/bin/sh

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
state_dir="${TPF_VERIFY_STATE_DIR:-$repository_root/target/full-verify}"
pid_file="$state_dir/pid"
status_file="$state_dir/exit-status"
log_file="$state_dir/verify.log"
start_claim_dir="$state_dir/start-claim"
success_message='TPF full verify: all went well'

is_running() {
  [ -r "$pid_file" ] || return 1
  verify_pid="$(cat "$pid_file")"
  [ -n "$verify_pid" ] && kill -0 "$verify_pid" 2>/dev/null
}

run_verify() {
  rm -f "$status_file"
  cd "$repository_root"
  set +e
  ./mvnw verify -Dmaven.repo.local="$repository_root/.m2/repository" >"$log_file" 2>&1
  verify_status=$?
  set -e
  if [ "$verify_status" -eq 0 ]; then
    printf '%s\n' "$success_message" >>"$log_file"
  else
    printf 'TPF full verify: failed with exit status %s\n' "$verify_status" >>"$log_file"
  fi
  printf '%s\n' "$verify_status" >"$status_file.tmp"
  mv "$status_file.tmp" "$status_file"
  exit "$verify_status"
}

release_start_claim() {
  rmdir "$start_claim_dir" 2>/dev/null || true
}

start_verify() {
  mkdir -p "$state_dir"
  if ! mkdir "$start_claim_dir" 2>/dev/null; then
    if is_running; then
      printf 'TPF full verify: already running as PID %s; log: %s\n' "$verify_pid" "$log_file"
    else
      printf 'TPF full verify: start already in progress; log: %s\n' "$log_file"
    fi
    return 0
  fi
  trap 'release_start_claim' EXIT
  trap 'release_start_claim; exit 1' HUP INT TERM
  if is_running; then
    printf 'TPF full verify: already running as PID %s; log: %s\n' "$verify_pid" "$log_file"
    return 0
  fi
  rm -f "$pid_file" "$status_file"
  : >"$log_file"
  nohup "$0" run >"$state_dir/launcher.log" 2>&1 </dev/null &
  verify_pid=$!
  printf '%s\n' "$verify_pid" >"$pid_file"
  printf 'TPF full verify: started as PID %s; poll with %s status\n' "$verify_pid" "$0"
  release_start_claim
  trap - EXIT HUP INT TERM
}

report_status() {
  if [ -r "$status_file" ]; then
    verify_status="$(cat "$status_file")"
    tail -n 1 "$log_file"
    return "$verify_status"
  fi
  if is_running; then
    printf 'TPF full verify: running as PID %s; log: %s\n' "$verify_pid" "$log_file"
    return 0
  fi
  printf 'TPF full verify: no active or completed run in %s\n' "$state_dir" >&2
  return 1
}

case "${1:-start}" in
start) start_verify ;;
status) report_status ;;
run) run_verify ;;
*)
  printf 'Usage: %s [start|status]\n' "$0" >&2
  exit 2
  ;;
esac
