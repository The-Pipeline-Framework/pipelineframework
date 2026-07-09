---
search: false
---

# Google Cloud Run Functions Platform (Development)

This page is the canonical TPF guide for `FUNCTION` platform builds that target Google Cloud Run functions. For the broader provider matrix, pair it with the [Multi-Cloud Function Providers Guide](/versions/v26.7.1/deploy/function-providers).

## What TPF Supports Today

- Platform mode: `FUNCTION` (default platform remains `COMPUTE`)
- Transport mode: `REST` (required in Function mode)
- Google-specific handlers: generated `HttpFunction` handlers via the Quarkus Google Cloud Functions extension
- Local verification: bootstrap/smoke coverage through the Search example

`FUNCTION` does not currently support `gRPC` transport. If you select `FUNCTION`, the generated runtime must use `REST`.

## Durability Scope

`FUNCTION` is a serverless invocation and packaging path. It is not the TPF durable orchestration path.

| Path | Current support |
| --- | --- |
| `COMPUTE` + `QUEUE_ASYNC` | TPF-owned execution records, leases, await units, retry/DLQ, re-drive, release pinning, and worker lifecycle. |
| `FUNCTION` | Generated Cloud Run functions handlers and REST adapters. The platform may retry invocations depending on trigger configuration, but TPF does not own durable coordinator state inside the function runtime. |

Use Cloud Run functions mode for stateless or caller-retried function invocations. Use the durable coordinator path when the application requires TPF-owned recovery, await resume, DLQ/re-drive, or checkpoint handoff.

## Cloud Run functions vs Cloud Run services

This page covers **Cloud Run functions**, the serverless function product path.

It does not cover generic **Cloud Run services**. A container-style Cloud Run deployment is closer to the normal `COMPUTE` path for a Quarkus service, not the current TPF `FUNCTION` provider path.

## Durable-workflow note

Provider-level durable or workflow products do not change TPF runtime semantics.

That means:

1. they are not a separate TPF platform mode,
2. they do not replace `COMPUTE` + `QUEUE_ASYNC`,
3. checkpoint handoff and queue-backed HA still belong to the orchestrator runtime path rather than the function-provider path.

For the future all-serverless durable coordinator design track, see [All-Serverless Durable Coordinator](/versions/v26.7.1/evolve/durable-coordinator/all-serverless-coordinator). That design is not current Cloud Run functions support.

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

- [Multi-Cloud Function Providers Guide](/versions/v26.7.1/deploy/function-providers)
- [Runtime Layouts](/versions/v26.7.1/deploy/runtime-layouts/)
- [AWS Lambda Platform](/versions/v26.7.1/deploy/aws-lambda)
