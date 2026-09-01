#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
./mvnw -pl examples/rag-turnkey/query -am quarkus:dev -Dmaven.repo.local="$PWD/.m2/repository"
