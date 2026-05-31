---
search: false
---

# Google Cloud Run Functions Platform (Development)

This page is the canonical TPF guide for `FUNCTION` platform builds that target Google Cloud Run functions. For the broader provider matrix, pair it with the [Multi-Cloud Function Providers Guide](/versions/v26.5.2/guide/build/runtime-layouts/function-providers).

## What TPF Supports Today

- Platform mode: `FUNCTION` (default platform remains `COMPUTE`)
- Transport mode: `REST` (required in Function mode)
- Google-specific handlers: generated `HttpFunction` handlers via the Quarkus Google Cloud Functions extension
- Local verification: bootstrap/smoke coverage through the Search example

`FUNCTION` does not currently support `gRPC` transport. If you select `FUNCTION`, the generated runtime must use `REST`.

## Cloud Run functions vs Cloud Run services

This page covers **Cloud Run functions**, the serverless function product path.

It does not cover generic **Cloud Run services**. A container-style Cloud Run deployment is closer to the normal `COMPUTE` path for a Quarkus service, not the current TPF `FUNCTION` provider path.

## Durable-workflow note

Provider-level durable or workflow products do not change TPF runtime semantics.

That means:

1. they are not a separate TPF platform mode,
2. they do not replace `COMPUTE` + `QUEUE_ASYNC`,
3. checkpoint handoff and queue-backed HA still belong to the orchestrator runtime path rather than the function-provider path.

## Example verification surface

The current repo verification surface for Google Cloud Run functions is located in `examples/search`.

Build:

```bash
./examples/search/build-gcp.sh -DskipTests
```

Bootstrap smoke:

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.gcp.scope=compile \
  -Dquarkus.profile=gcp-functions \
  -Dtest=GcpFunctionsBootstrapSmokeTest \
  test
```

## Next Steps

- [Multi-Cloud Function Providers Guide](/versions/v26.5.2/guide/build/runtime-layouts/function-providers)
- [Runtime Layouts](/versions/v26.5.2/guide/build/runtime-layouts/)
- [AWS Lambda Platform](/versions/v26.5.2/guide/development/aws-lambda)
