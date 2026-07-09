---
search: false
---

# Multi-Cloud Function Providers Guide

This guide is the multi-cloud entry point for TPF function deployments. It shows how the same `FUNCTION` platform mode can target AWS Lambda, Azure Functions, and Google Cloud Run functions while keeping the typed business flow unchanged.

## Overview

TPF supports three major cloud function platforms:

| Provider | Extension | Handler Interface | Local Testing |
|----------|-----------|-------------------|---------------|
| **AWS Lambda** | `quarkus-amazon-lambda` | `RequestHandler<I, O>` | Mock event server |
| **Azure Functions** | `quarkus-azure-functions` | HTTP trigger + REST | Core Tools v4.x |
| **Google Cloud Run functions** | `quarkus-google-cloud-functions` | `HttpFunction` | Functions Framework |

Important runtime constraint:

- `FUNCTION` currently requires `REST` transport. `gRPC` is not a supported `FUNCTION` transport in the current implementation.
- Google's current product name is Cloud Run functions, while the current Quarkus extension name remains `quarkus-google-cloud-functions`.

## Support Matrix

| Path | What TPF owns | Current HA meaning |
| --- | --- | --- |
| `COMPUTE` + `QUEUE_ASYNC` durable coordinator | Execution records, leases, await units, retry/DLQ, re-drive, release pinning, and worker lifecycle. | TPF-owned durable orchestration path for self-hosted HA. |
| `FUNCTION` | Generated provider handlers and serverless invocation adapters for REST-backed pipeline or step calls. | Platform invocation availability only; not TPF-owned durable orchestration HA. |

Use `FUNCTION` when you want serverless invocation packaging for supported handlers and can rely on caller/platform retries plus application idempotency. Use `COMPUTE` + `QUEUE_ASYNC` when the pipeline needs TPF-owned durable execution state, await recovery, DLQ/re-drive, or checkpoint-style orchestration semantics.

## Architecture

### Step-Level Handlers

Step-level pipeline handlers (e.g., `CrawlSourceFunctionHandler`, `ParseDocumentFunctionHandler`) are **fully multi-cloud ready**. The framework generates provider-specific handlers based on:

1. **Explicit configuration**: `-Dpipeline.function.provider=aws|azure|gcp`
2. **Auto-detection**: Based on Quarkus extension in classpath
3. **Default**: AWS Lambda

### Orchestrator Handlers

The orchestrator handler (`PipelineRunFunctionHandler`) generates **provider-specific handlers** for each cloud platform:

- **AWS Lambda**: Implements `RequestHandler<I, O>` with Lambda `Context`
- **Azure Functions**: POJO with `ExecutionContext` parameter
- **Google Cloud Run functions**: Implements `HttpFunction` with `HttpRequest`/`HttpResponse`

All providers preserve FUNCTION platform semantics (cardinality, failure handling) and support async handlers (run-async, status, result). Azure and GCP FUNCTION deployments use the required REST transport approach, where HTTP triggers route requests to Quarkus REST endpoints while preserving FUNCTION semantics.

## What this guide covers and does not cover

This guide covers the current repo-supported `FUNCTION` targets:

1. AWS Lambda
2. Azure Functions
3. Google Cloud Run functions

It does not currently document or implement:

1. Google Cloud Run services as a generic container/service target,
2. Azure Durable Functions as a separate TPF runtime model,
3. `gRPC` as a `FUNCTION` transport,
4. all-serverless durable orchestration.

If you need queue-backed recovery, checkpoint handoff, or orchestrator-managed HA, use the `COMPUTE` + `QUEUE_ASYNC` path instead of treating function providers as a replacement for that runtime model.

An all-serverless durable coordinator would be a separate design, backed by durable services such as DynamoDB, SQS, and EventBridge-style scheduling. Current `FUNCTION` support should be read as serverless adapter support, not as that architecture.

The current architecture spike for that future path is [All-Serverless Durable Coordinator](/versions/v26.7.1/evolve/durable-coordinator/all-serverless-coordinator). It evaluates TPF-native single-shot coordinator actions first, and provider durable workflow engines such as Lambda durable functions, Step Functions, Azure Durable Functions, and Google Cloud Workflows as possible later adapters.

## Quick Start

### AWS Lambda

```bash
# Build
./build-lambda.sh -DskipTests

# Test
./mvnw -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.lambda.scope=compile \
  -Dquarkus.profile=lambda \
  -Dtest=LambdaMockEventServerSmokeTest \
  test
```

See [AWS Lambda Platform Guide](/versions/v26.7.1/deploy/aws-lambda) for detailed deployment instructions.

### Azure Functions

```bash
# Build
./build-azure.sh -DskipTests

# Test
./mvnw -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -Dtest=AzureFunctionsBootstrapSmokeTest \
  test
```

See [Search Azure Verification Lane](/versions/v26.7.1/deploy/search-azure-functions) and [Azure Functions Deployment Walkthrough](/versions/v26.7.1/deploy/azure-functions-deployment-walkthrough) for the current Azure verification and deployment path.

### Google Cloud Run functions

```bash
# Build
./build-gcp.sh -DskipTests

# Test
./mvnw -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.gcp.scope=compile \
  -Dquarkus.profile=gcp-functions \
  -Dtest=GcpFunctionsBootstrapSmokeTest \
  test
```

## Configuration

### Build Properties

| Property | Description | Values | Default |
|----------|-------------|--------|---------|
| `tpf.build.platform` | Target platform | `COMPUTE`, `FUNCTION` | `COMPUTE` |
| `tpf.build.transport` | Transport protocol for `FUNCTION` builds | `REST` | `REST` |
| `tpf.build.lambda.scope` | Lambda dependency scope | `compile`, `provided` | `provided` |
| `tpf.build.azure.scope` | Azure dependency scope | `compile`, `provided` | `provided` |
| `tpf.build.gcp.scope` | GCP dependency scope | `compile`, `provided` | `provided` |
| `quarkus.profile` | Quarkus profile | `lambda`, `azure-functions`, `gcp-functions` | - |

`FUNCTION` is a platform mode, not a transport. Function-provider builds generate provider handler artifacts and currently require `REST` transport; `gRPC` remains a transport option for `COMPUTE` deployments.

### Provider Selection

The framework selects the provider using this precedence:

1. **Explicit**: `-Dpipeline.function.provider=aws|azure|gcp`
2. **Auto-detect**: Based on Quarkus extension in classpath
3. **Default**: AWS Lambda

## Provider Comparison

### AWS Lambda

**Strengths:**
- Mature ecosystem with extensive integrations
- SnapStart for cold start mitigation
- Native support for streaming payloads
- Comprehensive monitoring via CloudWatch

**Considerations:**
- Maximum execution time: 15 minutes
- Memory: 128MB - 10GB
- Deployment package size limits

### Azure Functions

**Strengths:**
- Premium plan with pre-warmed instances
- Strong integration with Azure ecosystem
- Flexible pricing (Consumption, Premium, Dedicated)
- Application Insights for monitoring

**Considerations:**
- Maximum execution time varies by plan
- Cold starts on Consumption plan
- Storage account dependency

### Google Cloud Run functions

**Strengths:**
- Simple deployment model
- Tight integration with GCP services
- Cloud Monitoring and Logging
- 2nd gen functions with improved performance

**Considerations:**
- Maximum execution time: 9 minutes (1st gen), 60 minutes (2nd gen)
- Memory: 128MB - 32GB
- Cold starts on standard tier

## Wire Protocol

TPF uses **protobuf-over-HTTP** as the default wire protocol for remote function invocations:

- Payloads wrapped in `BytesValue` protobuf messages
- Content-Type: `application/x-protobuf`
- JSON fallback when protobuf not configured

This is **cloud-agnostic** and works identically across all providers.

## Testing Strategy

### Local Testing

| Provider | Test Class | Requirements |
|----------|-----------|--------------|
| AWS Lambda | `LambdaMockEventServerSmokeTest` | None |
| Azure Functions | `AzureFunctionsBootstrapSmokeTest` | None |
| Google Cloud Run functions | `GcpFunctionsBootstrapSmokeTest` | None |

### Integration Testing

| Provider | Test Class | Requirements |
|----------|-----------|--------------|
| AWS Lambda | `*-EndToEndIT` | AWS credentials, SAM/CloudFormation |
| Azure Functions | `AzureFunctionsEndToEndIT` | Azure subscription, Terraform |
| Google Cloud Run functions | `GcpFunctionsBootstrapSmokeTest` | Build with the `gcp-functions` profile; validates Quarkus Functions extension bootstrap and `HttpFunction` loading only |

## Troubleshooting

### Common Issues

**Multiple providers detected:**

```text
[TPF] Multiple function providers detected. Using AWS Lambda (aws).
```

Solution: Set explicit provider: `-Dpipeline.function.provider=azure`

**Extension not on classpath:**

```text
ClassNotFoundException: com.google.cloud.functions.HttpFunction
```

Solution: Build with correct scope: `-Dtpf.build.gcp.scope=compile`

## Next Steps

- Review provider-specific guides for detailed deployment instructions
- See [Function Platform Architecture](/versions/v26.7.1/evolve/compiler-pipeline-architecture) for implementation details
- Consult [Operations Playbook](/versions/v26.7.1/operate/operators-playbook) for production guidance
