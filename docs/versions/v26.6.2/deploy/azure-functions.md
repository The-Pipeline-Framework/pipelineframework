---
search: false
---

# Azure Functions Platform (Development)

This page is the canonical The Pipeline Framework (TPF) guide for `FUNCTION` platform builds that target Azure Functions. For the broader provider matrix, pair it with the [Multi-Cloud Function Providers Guide](/versions/v26.6.2/deploy/function-providers).

## What TPF Supports Today

- Platform mode: `FUNCTION` (default platform remains `COMPUTE`)
- Transport mode: `REST` (required in Function mode)
- Azure-specific handlers: generated HTTP-trigger handlers using Azure Functions bindings
- Local verification: Azure Functions Core Tools and the Search example in `examples/search`

`FUNCTION` does not currently support `gRPC` transport. If you select `FUNCTION`, the generated runtime must use `REST`.

## Durability Scope

`FUNCTION` is a serverless invocation and packaging path. It is not the TPF durable orchestration path.

| Path | Current support |
| --- | --- |
| `COMPUTE` + `QUEUE_ASYNC` | TPF-owned execution records, leases, await units, retry/DLQ, re-drive, release pinning, and worker lifecycle. |
| `FUNCTION` | Generated Azure Functions handlers and REST adapters. Azure may retry invocations depending on trigger configuration, but TPF does not own durable coordinator state inside the function runtime. |

Use Azure Functions mode for stateless or caller-retried function invocations. Use the durable coordinator path when the application requires TPF-owned recovery, await resume, DLQ/re-drive, or checkpoint handoff.

## What this path covers

TPF keeps the typed Java business flow unchanged while generating Azure-specific entry points around it.

This path is for:

1. generated Azure Functions handlers,
2. local/provider verification of the function runtime,
3. deploying the same flow through Azure’s function platform.

This path is not:

1. a replacement for `COMPUTE` + `QUEUE_ASYNC`,
2. a checkpoint-handoff runtime,
3. a separate TPF runtime model just because Azure also offers Durable Functions.

Azure Durable Functions do not change TPF runtime semantics. If you need queue-backed HA, checkpoint handoff, or orchestrator-managed crash recovery, use the `COMPUTE` + `QUEUE_ASYNC` path.

## Example verification surface

The current repo verification surface for Azure is located in `examples/search`.

Build:

```bash
./examples/search/build-azure.sh -DskipTests
```

Bootstrap smoke:

```bash
./scripts/ci/bootstrap-local-repo-prereqs.sh framework

./mvnw -f examples/search/orchestrator-svc/pom.xml \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=AzureFunctionsBootstrapSmokeTest \
  test
```

For deeper Azure-specific setup and local runtime testing with Core Tools, use the dedicated Search guide.

## Next Steps

- [Search Azure Verification Lane](/versions/v26.6.2/deploy/search-azure-functions)
- [Multi-Cloud Function Providers Guide](/versions/v26.6.2/deploy/function-providers)
- [Runtime Layouts](/versions/v26.6.2/deploy/runtime-layouts/)
