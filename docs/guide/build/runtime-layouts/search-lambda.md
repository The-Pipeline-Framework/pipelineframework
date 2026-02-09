# Search Lambda Walkthrough

This walkthrough shows how to build the `examples/search` pipeline for AWS Lambda support using The Pipeline Framework (TPF) platform mode.

## Scope

- Focuses on framework-level Lambda support and local validation.
- Uses Quarkus Lambda mock event server for local/dev-test checks.
- Does not require Terraform or live AWS infrastructure.

## Preconditions

- Java 21
- Docker (for containerized integration lanes, optional for this walkthrough)
- GraalVM (optional, only required for native build verification)

## Build in Lambda Mode

From repo root:

```bash
./examples/search/build-lambda.sh -DskipTests
```

This script sets:
- `pipeline.platform=LAMBDA`
- `pipeline.transport=REST`
- `pipeline.rest.naming.strategy=RESOURCEFUL`

## Verify Mock Lambda Event Server

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc -am \
  -Dpipeline.platform=LAMBDA \
  -Dtest=LambdaMockEventServerSmokeTest \
  test
```

Success criteria for this command:
- Maven finishes with `BUILD SUCCESS`.
- `LambdaMockEventServerSmokeTest` is reported as passed in the console surefire summary.
- Detailed output is available in `examples/search/orchestrator-svc/target/surefire-reports`.

## Verify Native Build in Lambda Mode

```bash
./mvnw -f examples/search/pom.xml \
  -pl crawl-source-svc -am \
  -Dpipeline.platform=LAMBDA \
  -DskipTests \
  -Dquarkus.native.enabled=true \
  -Pnative \
  package
```

## REST Endpoint Naming in Search

Search is currently all 1-1 steps, so `RESOURCEFUL` naming uses output types: when each request produces exactly one output, naming the resource by that output keeps the URL unambiguous and stable.
- `CrawlRequest -> RawDocument`: `/api/v1/raw-document/`
- `RawDocument -> ParsedDocument`: `/api/v1/parsed-document/`
- `ParsedDocument -> TokenBatch`: `/api/v1/token-batch/`
- `TokenBatch -> IndexAck`: `/api/v1/index-ack/`

For non-1-1 cardinalities, naming differs: 1-N and M-N shapes use input-type-based naming (or explicit disambiguators) to avoid collisions on collection-producing flows.

Plugin/synthetic side effects append plugin tokens to avoid collisions, for example:
- `/api/v1/parsed-document/cache-invalidate/`

## Current Lambda Constraints

At compile time, TPF currently enforces:
- `platform=LAMBDA` requires `transport=REST`
- only `UNARY_UNARY` steps (1-1 style cardinality) are supported
