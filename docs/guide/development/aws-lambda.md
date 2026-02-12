# AWS Lambda Platform (Development)

This page is the canonical TPF guide for building pipeline applications for AWS Lambda.

## What TPF Supports Today

- Platform mode: `FUNCTION` (default platform remains `COMPUTE`)
- Transport mode: `REST` (required in Function mode)
- Current compile-time constraint: `UNARY_UNARY` step shape only (Milestone A scope)

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

In FUNCTION mode, TPF generates native Lambda handlers for unary REST bindings:

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

## Backpressure Model in Function Deployments

Backpressure remains active inside each runtime via Mutiny `Multi`/`Uni`, but Lambda invocation
boundaries are explicit adaptation points:

- inside a runtime: normal reactive demand and overflow handling
- across runtime boundaries: flow control is managed by adapter batching policies and Lambda/event-source concurrency

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
