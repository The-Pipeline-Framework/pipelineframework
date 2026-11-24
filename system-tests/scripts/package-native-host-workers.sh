#!/usr/bin/env bash
set -euo pipefail

source=${1:?Pass the tested runtime checkout}
resolved_set=${2:?Pass the resolved compatibility set}
manifest=${3:?Pass the selected runtime Maven manifest}
output=${4:?Pass the destination for the package artifacts}
python=${TPF_PYTHON:-python3.14}
source_sha=$(jq -er '.components.runtime.sha | select(test("^[0-9a-f]{40}$"))' "$resolved_set")
manifest_digest=$(jq -er '.components.runtime.manifestDigest' "$resolved_set")
if [[ "$manifest_digest" != "sha256:$(sha256sum "$manifest" | cut -d' ' -f1)" ]]; then
  echo "Runtime Maven manifest differs from the selected compatibility set" >&2
  exit 1
fi
tested_sha=$(jq -er --arg sha "$source_sha" \
  'select(.component == "runtime" and .sourceSha == $sha) | .testedSha | select(test("^[0-9a-f]{40}$"))' "$manifest")
if [[ "$(git -C "$source" rev-parse HEAD)" != "$tested_sha" ]]; then
  echo "Runtime checkout differs from the exact tested Maven revision" >&2
  exit 1
fi
mkdir -p "$output"
output=$(cd "$output" && pwd)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git -C "$source" archive "$tested_sha" experimental/node-local experimental/python-local | tar -x -C "$work"
npm pack --ignore-scripts --pack-destination "$output" "$work/experimental/node-local"
"$python" -m venv "$work/venv"
"$work/venv/bin/python" -m pip install setuptools==80.9.0 wheel==0.45.1
"$work/venv/bin/python" -m pip wheel --no-deps --no-build-isolation --wheel-dir "$output" "$work/experimental/python-local"

node_digest=$(sha256sum "$output/tpf-experimental-node-local-0.0.0.tgz" | cut -d' ' -f1)
python_digest=$(sha256sum "$output/tpf_experimental_python_local-0.0.0-py3-none-any.whl" | cut -d' ' -f1)
jq -n --arg source "$source_sha" --arg tested "$tested_sha" --arg node "$node_digest" --arg python "$python_digest" \
  '{sourceSha: $source, testedSha: $tested, nodePackage: $node, pythonWheel: $python}' > "$output/native-packages.json"
