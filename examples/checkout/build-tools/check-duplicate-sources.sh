#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <target-dir> <source-dir> [<source-dir> ...]" >&2
  exit 2
fi

target_dir="$1"
shift

tmp_file="${target_dir}/pipeline-runtime-source-files.txt"
mkdir -p "$target_dir"
trap 'rm -f "$tmp_file"' EXIT

exclude_patterns="${PIPELINE_RUNTIME_EXCLUDE_PATTERNS:-*/orchestrator/*}"
IFS=':' read -r -a exclude_pattern_array <<< "$exclude_patterns"

{
  for dir in "$@"; do
    if [ -d "$dir" ]; then
      while IFS= read -r -d '' file; do
        rel_path="${file#"${dir}/"}"
        skip=false
        # shellcheck disable=SC2053
        # Note: $pattern is intentionally unquoted to allow glob expansion (e.g., */orchestrator/*)
        for pattern in "${exclude_pattern_array[@]}"; do
          if [[ -n "$pattern" && "$rel_path" == $pattern ]]; then
            skip=true
            break
          fi
        done
        if [ "$skip" = true ]; then
          continue
        fi
        printf '%s\n' "$rel_path"
      done < <(find "$dir" -type f -name '*.java' -print0)
    fi
  done
} > "$tmp_file"

duplicates="$(sort "$tmp_file" | uniq -d)"
if [ -n "$duplicates" ]; then
  echo "Duplicate Java source paths detected while collecting pipeline-runtime sources into ${target_dir}:" >&2
  echo "$duplicates" >&2
  exit 1
fi