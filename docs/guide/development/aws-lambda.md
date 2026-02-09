# AWS Lambda Platform (Development)

This page is the canonical TPF guide for building pipeline applications for AWS Lambda.

## What TPF Supports Today

- Platform mode: `LAMBDA`
- Transport mode: `REST` (required in Lambda mode)
- Current compile-time constraint: `UNARY_UNARY` step shape only

Set platform mode during build:

```bash
./mvnw -f <app-parent>/pom.xml -Dpipeline.platform=LAMBDA -Dpipeline.transport=REST clean verify
```

## Gateway Extension Choice

Quarkus provides different Lambda gateway extensions:

- `quarkus-amazon-lambda-http` for API Gateway HTTP API (v2 events)
- `quarkus-amazon-lambda-rest` for API Gateway REST API (v1 events)

TPF reference examples and CI currently use `quarkus-amazon-lambda-http`.

### If You Want REST API Gateway Instead

Use `quarkus-amazon-lambda-rest` in your application module(s) instead of `quarkus-amazon-lambda-http`.

In practice:

1. Remove `quarkus-amazon-lambda-http`
2. Add `quarkus-amazon-lambda-rest`
3. Keep `pipeline.platform=LAMBDA` and `pipeline.transport=REST`
4. Use the matching local event-server test artifact for your gateway type

TPF does not currently ship a separate first-class generator mode per gateway flavor; the gateway choice is an app dependency decision.

## Why No `RequestHandler<?, ?>` in Step Classes

In Lambda gateway mode, Quarkus provides the Lambda handler bridge. Your generated TPF resources remain JAX-RS REST resources.

- Step classes do not need to implement `RequestHandler`
- Quarkus handler bridges API Gateway events to Quarkus HTTP routing

## Quarkus Integrations You Can Leverage in TPF Apps

When using Quarkus Lambda HTTP gateway support, app developers can leverage:

- AWS request context injection (request/event/context objects)
- HTTP request context attributes via Quarkus REST request context
- Security integration and custom identity provider hooks
- Custom Lambda auth mechanism hooks

These are application-level integrations and can be layered on top of generated TPF resources.

## X-Ray Extension

For AWS X-Ray integration, use:

- `io.quarkus:quarkus-amazon-lambda-xray`

This is particularly relevant for Lambda deployments, including native-image scenarios where Quarkus supplies required substitutions/runtime support.

## Related Docs

- [Search Lambda Verification Lane](/guide/build/runtime-layouts/search-lambda)
- [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)
- [Configuration (Overview)](/guide/application/configuration)
