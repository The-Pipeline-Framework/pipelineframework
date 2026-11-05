#!/usr/bin/env bash
set -euo pipefail

component=${1:?component required}
repository=${2:?repository required}
sha=${3:?commit SHA required}

printf '::error title=Candidate input unavailable::No attested current-main candidate exists for %s at %s (%s).\n' \
  "$component" "$sha" "$repository" >&2

for workflow in tpf-candidate-build.yml tpf-candidate-publish.yml; do
  if runs="$(gh run list --repo "$repository" --workflow "$workflow" --commit "$sha" --limit 3 \
    --json event,status,conclusion,url --jq '.[] | [.event, .status, (.conclusion // ""), .url] | @tsv' 2>/dev/null)"; then
    if [[ -n "$runs" ]]; then
      printf '%s for %s:\n%s\n' "$workflow" "$sha" "$runs" >&2
    else
      printf 'No %s runs found for %s.\n' "$workflow" "$sha" >&2
    fi
  else
    printf 'Could not look up %s for %s. See https://github.com/%s/actions/workflows/%s\n' \
      "$workflow" "$sha" "$repository" "$workflow" >&2
  fi
done
exit 1
