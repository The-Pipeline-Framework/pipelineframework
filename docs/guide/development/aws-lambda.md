# AWS Lambda Platform (Development)

This page is the canonical TPF guide for building pipeline applications for AWS Lambda.

## What TPF Supports Today

- Platform mode: `FUNCTION` (default platform remains `COMPUTE`)
- Transport mode: `REST` (required in Function mode)
- Step shapes: unary and streaming shapes are supported in FUNCTION mode through generated adapter/bridge wiring

Supported FUNCTION step shapes today:

| Step shape | Status in FUNCTION mode | Notes |
|------------|-------------------------|-------|
| `UNARY_UNARY` | Supported | Canonical handler path in B. |
| `UNARY_STREAMING` | Supported | Adapter/bridge generated (`invokeOneToMany`). |
| `STREAMING_UNARY` | Supported | Adapter/bridge generated (`invokeManyToOne`). |
| `STREAMING_STREAMING` | Supported | Adapter/bridge generated (`invokeManyToMany`). |

Set platform mode during build:

```bash
./mvnw -f <app-parent>/pom.xml \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.lambda.dependency.scope=compile \
  -Dquarkus.profile=lambda \
  clean verify
```

TPF uses build switches for this mode. It does not require a dedicated Maven profile.

## Required and Optional Quarkus Extensions

Required for FUNCTION runtime wiring and generated handlers:

- `io.quarkus:quarkus-amazon-lambda`

Optional API Gateway REST bridge (v1 events):

- `io.quarkus:quarkus-amazon-lambda-rest`
- `io.quarkus:quarkus-amazon-lambda-rest-event-server` (test/dev mock server for REST gateway events)

API Gateway HTTP bridge extensions are outside the canonical TPF FUNCTION path.

## Generated Lambda Handlers

In FUNCTION mode, TPF generates native Lambda handlers for REST bindings across supported cardinalities:

- Step handlers implement `RequestHandler<I, O>`
- Orchestrator handler implements `RequestHandler<I, O>`
- Handlers delegate to generated TPF resources, preserving pipeline behavior

When multiple handlers exist in one module, choose one explicitly:

```properties
quarkus.lambda.handler=PipelineRunFunctionHandler
```

TPF-generated handlers are annotated with `@Named("<HandlerClassName>")`.

## Function Transport Contracts (Preview)

TPF now defines function-transport runtime contracts for native Lambda/event-driven wiring:

- `org.pipelineframework.transport.function.FunctionSourceAdapter`
- `org.pipelineframework.transport.function.FunctionInvokeAdapter`
- `org.pipelineframework.transport.function.FunctionSinkAdapter`
- `org.pipelineframework.transport.function.TraceEnvelope`

These abstractions separate:

1. Event ingress → reactive stream (`SourceAdapter`)
2. Orchestrator-to-step invocation across function boundaries (`InvokeAdapter`)
3. Reactive output → egress target (`SinkAdapter`)

`TraceEnvelope` carries lineage, payload model/version, and idempotency metadata without forcing
application DTOs to become envelope types.

## ID Ownership and Idempotency Boundaries

TPF distinguishes between business IDs and transport IDs:

- Business IDs (for example CSV row IDs, domain entity IDs) are application-owned.
- Transport IDs (`traceId`, `itemId`, transport `idempotencyKey`) are framework-owned and opaque to business logic.

TPF does not impose business semantics on transport IDs. Authoritative dedupe/rejection remains in application and data layers (for example domain rules, warehouse checks, JPA/SQL primary keys and unique constraints).

## Function Transport Idempotency Policy

Function transport adapters support policy-driven idempotency derivation through `FunctionTransportContext` attributes:

- `tpf.idempotency.policy=CONTEXT_STABLE|EXPLICIT` (legacy `RANDOM` accepted as alias)
- `tpf.idempotency.key=<caller-stable-key>`

Behavior:

- `CONTEXT_STABLE` (default): adapter-generated deterministic transport keys from stable context/envelope identifiers (non-authoritative dedupe behavior).
  These keys provide transport-level correlation and can reduce duplicate processing attempts, but they do not guarantee business/data uniqueness and do not replace explicit business idempotency rules.
- `EXPLICIT`: uses caller-provided key where supported; this is the recommended mode when the application already has stable business identifiers.
  If the key is missing (or an adapter/runtime path does not support explicit override), adapters log a warning and fall back to `CONTEXT_STABLE` derivation.
- `RANDOM`: legacy compatibility alias mapped to `CONTEXT_STABLE`.

`EXPLICIT` is an opt-in transport assist, not a replacement for business/data-level uniqueness enforcement.

## Shape-To-Bridge Mapping And Failure Semantics

FUNCTION generation supports unary and streaming step cardinalities through bridge/adapters.

| Pipeline shape | Function bridge mapping | Failure semantics |
|----------------|-------------------------------|-------------------|
| `UNARY_UNARY` | Generated Lambda handler delegates to generated REST resource for step/orchestrator. | Step/resource failure becomes handler failure for that invocation.<br>Retry/timeout behavior is controlled by Lambda invoke source and TPF retry configuration inside the runtime. |
| `UNARY_STREAMING` | Generated handler delegates through `FunctionTransportBridge.invokeOneToMany`. | Adapter/bridge path returns list sink output and preserves transport metadata/idempotency policy. |
| `STREAMING_UNARY` | Generated handler delegates through `FunctionTransportBridge.invokeManyToOne`. | Adapter/bridge path reduces stream input to unary output with bounded collection policy. |
| `STREAMING_STREAMING` | Generated handler delegates through `FunctionTransportBridge.invokeManyToMany`. | Adapter/bridge path processes streaming input/output with bounded ingress/sink behavior. |

Failure semantics summary:

- Validation failures in FUNCTION mode are now primarily transport-mode related (`pipeline.transport=REST` required), while supported streaming shapes generate adapter/bridge wiring.
- Invocation failures in unary handlers fail the current function call.
- Retries are not implicit in the bridge itself; they follow configured TPF retry behavior and upstream Lambda/event-source policies.

## Backpressure Model in Function Deployments

Backpressure remains active inside each runtime via Mutiny `Multi`/`Uni`, but Lambda invocation
boundaries are explicit adaptation points:

- inside a runtime: normal reactive demand and overflow handling
- across runtime boundaries: flow control is managed by adapter batching policies and Lambda/event-source concurrency

## Operator Notes: Bounded Waits, Timeouts, And Adapter Behavior

Operators should configure bounded waits and clear timeout ownership at function boundaries.

- Lambda timeout should be finite and aligned to worst-case step latency plus retry budget.
- Startup/dependency waits should stay bounded (`pipeline.health.startup-timeout`).
- Function adapters are boundary adapters, not infinite buffers.
  Retry/backoff should stay bounded (`pipeline.defaults.retry-limit`, `pipeline.defaults.retry-wait-ms`, `pipeline.defaults.max-backoff`).
- Backpressure behavior inside a runtime remains controlled by step/connector overflow strategy and capacity settings.
- Adapters preserve reactive semantics inside a runtime; cross-runtime pacing is bounded by Lambda concurrency, event source behavior, and configured batching/overflow policy.

## Quarkus Integrations You Can Leverage in TPF Apps

With Quarkus Lambda integrations, app developers can leverage:

- AWS request context injection (request/event/context objects)
- request context attributes via Quarkus REST context (when REST gateway extensions are enabled)
- Security integration and custom identity provider hooks
- Custom Lambda auth mechanism hooks

These are application-level integrations layered on top of generated TPF resources.

## X-Ray Extension

For AWS X-Ray integration, use:

- `io.quarkus:quarkus-amazon-lambda-xray`

This is particularly relevant for Lambda deployments, including native-image scenarios where Quarkus supplies required substitutions/runtime support.

## Related Docs

- [Search Lambda Verification Lane](/guide/build/runtime-layouts/search-lambda)
- [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)
- [Configuration (Overview)](/guide/application/configuration)
