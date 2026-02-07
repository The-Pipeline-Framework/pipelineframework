#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <target-dir> <source-dir> [<source-dir> ...]" >&2
  exit 2
fi

target_dir="$1"
shift

tmp_file="${target_dir}/monolith-source-files.txt"
mkdir -p "$target_dir"
: > "$tmp_file"

for dir in "$@"; do
  if [ -d "$dir" ]; then
    find "$dir" -type f -name '*.java' -print | while IFS= read -r file; do
      rel_path="${file#"${dir}/"}"
      # Monolith keeps a single ExecutorProducer copy from input-csv-file-processing-svc.
      if [ "$rel_path" = "org/pipelineframework/csv/service/ExecutorProducer.java" ] \
        && { [[ "$dir" == *"/payments-processing-svc/src/main/java" ]] \
          || [[ "$dir" == *"/output-csv-file-processing-svc/src/main/java" ]]; }; then
        continue
      fi
      echo "$rel_path" >> "$tmp_file"
    done
  fi
done

duplicates="$(sort "$tmp_file" | uniq -d)"
if [ -n "$duplicates" ]; then
  echo "Duplicate Java source paths detected while collecting monolith sources into ${target_dir}:" >&2
  echo "$duplicates" >&2
  exit 1
fi
