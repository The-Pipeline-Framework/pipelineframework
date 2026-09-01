#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
./mvnw -pl examples/rag-turnkey/query -am package -DskipTests -Dgpg.skip -Dmaven.javadoc.skip=true \
  -Dmaven.repo.local="$PWD/.m2/repository"
exec java -jar examples/rag-turnkey/query/target/quarkus-app/quarkus-run.jar
