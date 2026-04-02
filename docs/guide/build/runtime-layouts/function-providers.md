# Multi-Cloud Function Providers Guide

This guide covers deploying TPF (The Pipeline Framework) pipelines to multiple cloud function providers: AWS Lambda, Azure Functions, and Google Cloud Functions.

## Overview

TPF supports three major serverless function platforms:

| Provider | Extension | Handler Interface | Local Testing |
|----------|-----------|-------------------|---------------|
| **AWS Lambda** | `quarkus-amazon-lambda` | `RequestHandler<I, O>` | Mock event server |
| **Azure Functions** | `quarkus-azure-functions` | HTTP trigger + REST | Core Tools v4.x |
| **Google Cloud Functions** | `quarkus-google-cloud-functions` | `HttpFunction` | Functions Framework |

## Architecture

### Step-Level Handlers

Step-level pipeline handlers (e.g., `CrawlSourceFunctionHandler`, `ParseDocumentFunctionHandler`) are **fully multi-cloud ready**. The framework generates provider-specific handlers based on:

1. **Explicit configuration**: `-Dpipeline.function.provider=aws|azure|gcp`
2. **Auto-detection**: Based on Quarkus extension in classpath
3. **Default**: AWS Lambda

### Orchestrator Handlers

**Current Limitation**: The orchestrator handler (`PipelineRunFunctionHandler`) currently generates **AWS Lambda-specific code only**. For Azure Functions and GCP Cloud Functions deployments, use the **REST transport approach**:

- Azure Functions/GCP HTTP triggers route requests to Quarkus REST endpoints
- Quarkus handles pipeline execution through its REST resources
- FUNCTION platform semantics (cardinality, failure handling) are preserved

Multi-cloud orchestrator support is planned for a future iteration.

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

See [AWS Lambda Platform Guide](/guide/development/aws-lambda) for detailed deployment instructions.

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

See [Azure Functions Testing Guide](search-azure-functions.md) for detailed deployment instructions.

### Google Cloud Functions

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
| `tpf.build.transport` | Transport protocol | `REST`, `GRPC` | `REST` |
| `tpf.build.lambda.scope` | Lambda dependency scope | `compile`, `provided` | `provided` |
| `tpf.build.azure.scope` | Azure dependency scope | `compile`, `provided` | `provided` |
| `tpf.build.gcp.scope` | GCP dependency scope | `compile`, `provided` | `provided` |
| `quarkus.profile` | Quarkus profile | `lambda`, `azure-functions`, `gcp-functions` | - |

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

### Google Cloud Functions

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
| GCP Cloud Functions | `GcpFunctionsBootstrapSmokeTest` | None |

### Integration Testing

| Provider | Test Class | Requirements |
|----------|-----------|--------------|
| AWS Lambda | `*-EndToEndIT` | AWS credentials, SAM/CloudFormation |
| Azure Functions | `AzureFunctionsEndToEndIT` | Azure subscription, Terraform |
| GCP Cloud Functions | TBD | GCP project, Terraform |

## Troubleshooting

### Common Issues

**Multiple providers detected:**
```
[TPF] Multiple function providers detected. Using AWS Lambda (aws).
```
Solution: Set explicit provider: `-Dpipeline.function.provider=azure`

**Extension not on classpath:**
```
ClassNotFoundException: com.google.cloud.functions.HttpFunction
```
Solution: Build with correct scope: `-Dtpf.build.gcp.scope=compile`

**Orchestrator deployment fails on Azure/GCP:**
Solution: Use REST transport approach - HTTP triggers route to Quarkus REST endpoints

## Next Steps

- Review provider-specific guides for detailed deployment instructions
- See [Function Platform Architecture](/guide/evolve/compiler-pipeline-architecture.md) for implementation details
- Consult [Operations Playbook](/guide/operations/operators-playbook.md) for production guidance
