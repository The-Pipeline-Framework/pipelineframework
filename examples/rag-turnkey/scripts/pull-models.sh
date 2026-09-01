#!/usr/bin/env bash
set -euo pipefail
curl --fail --silent --show-error http://localhost:11435/api/pull -d '{"name":"nomic-embed-text"}'
curl --fail --silent --show-error http://localhost:11435/api/pull -d '{"name":"qwen3:8b"}'
