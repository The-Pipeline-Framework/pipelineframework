#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker_config="${DOCKER_CONFIG:-$HOME/.docker}/config.json"
if [[ -f "$docker_config" ]] \
    && grep -Eq '"credsStore"[[:space:]]*:[[:space:]]*"desktop"' "$docker_config" \
    && ! command -v docker-credential-desktop >/dev/null 2>&1; then
  cat >&2 <<'EOF'
Docker is configured to use docker-credential-desktop, but that helper is not installed.
For OrbStack, change "credsStore": "desktop" to "credsStore": "osxkeychain"
in ~/.docker/config.json, then rerun this command.
EOF
  exit 1
fi
if [[ $# -eq 0 ]]; then set -- up; fi
docker compose "$@"
