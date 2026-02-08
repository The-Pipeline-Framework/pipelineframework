# CSV Payments Pipeline-Runtime Walkthrough

This page documents the grouped pipeline-runtime topology in `examples/csv-payments`.

## What exists

- Pipeline-runtime parent POM: `examples/csv-payments/pom.pipeline-runtime.xml`
- Grouped pipeline runtime module: `examples/csv-payments/pipeline-runtime-svc/pom.xml`
- Runtime mapping scenario: `examples/csv-payments/config/runtime-mapping/pipeline-runtime.yaml`
- Build script: `examples/csv-payments/build-pipeline-runtime.sh`

## Topology shape

- `orchestrator-svc`: standalone orchestrator runtime
- `pipeline-runtime-svc`: grouped pipeline step runtime (all regular steps)
- `persistence-svc`: plugin/aspect runtime (persistence side effects)

## Build

```bash
./examples/csv-payments/build-pipeline-runtime.sh -DskipTests
```

What the script does:

- Applies `pipeline-runtime.yaml` as active runtime mapping.
- Builds `pom.pipeline-runtime.xml`.
- Uses `GRPC` transport by default (override with `PIPELINE_TRANSPORT=REST|LOCAL` if needed).
- Restores the previous active mapping file after the build.

## Verification smoke check

```bash
./mvnw -f examples/csv-payments/pom.pipeline-runtime.xml -DskipTests compile
```

## Required runtime environment

`pipeline-runtime-svc` requires explicit TLS path/password variables (no insecure defaults):

```bash
export SERVER_KEYSTORE_PATH=/deployments/server-keystore.jks
export SERVER_KEYSTORE_PASSWORD=secret
export CLIENT_TRUSTSTORE_PATH=/deployments/client-truststore.jks
export CLIENT_TRUSTSTORE_PASSWORD=secret
```

Startup now validates these values and fails fast if they are missing.

If you build container images in CI, set a deterministic image tag:

```bash
export IMAGE_REGISTRY=registry.example.com
export IMAGE_GROUP=csv-payments
export IMAGE_NAME=pipeline-runtime-svc
export IMAGE_TAG="$GITHUB_SHA"
```

## Operational note

This topology is the concrete bridge between fully modular and monolith:

- fewer runtime artifacts than modular,
- clearer isolation than monolith,
- explicit plugin runtime boundary retained.
