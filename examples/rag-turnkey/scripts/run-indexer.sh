#!/usr/bin/env bash
set -euo pipefail
repository_root="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$repository_root"
./mvnw -pl examples/rag-turnkey/indexer -am package -DskipTests -Dgpg.skip -Dmaven.javadoc.skip=true \
  -Dmaven.repo.local="$PWD/.m2/repository"
cd examples/rag-turnkey/indexer
exec java -jar target/quarkus-app/quarkus-run.jar
