# Pipeline Compilation and Generation

## Overview

The Pipeline Framework uses YAML-first compilation to automatically generate the necessary infrastructure for pipeline execution.
For internal `service:` steps:

1. `pipeline.yaml` declares the service class, cardinality, domain input/output types, and optional inbound/outbound mappers
2. The Java service implements one supported TPF service contract
3. The compiler validates the YAML contract against the implemented reactive service interface
4. It generates transport adapters and client steps for the configured transport (gRPC or REST)
5. It expands configured aspects into synthetic side effect steps when a plugin host is present
6. It registers all generated components with the dependency injection container

This eliminates the need for manual configuration and ensures consistency across your pipeline.

## Annotation Processing Workflow

### Proto Generation (Pre-Processing)

Before annotation processing, pipeline protobuf contracts are generated from the pipeline template. For version 3 Java applications, the Java domain target is generated in the same lifecycle. The authoritative generators are:

- `framework/runtime/src/main/java/org/pipelineframework/proto/PipelineProtoGenerator.java`
- `framework/runtime/src/main/java/org/pipelineframework/proto/PipelineV3JavaDomainGenerator.java`

`PipelineV3ContractGenerator` is the convenience lifecycle command that invokes both independent target generators. It does not make the project scaffolder responsible for DSL-derived domain code.

### Build Timeline (gRPC)

```text
Pipeline template
      |
      v
PipelineV3ContractGenerator
      |                    \
      v                     v
protobuf contracts -> protoc -> descriptor set (.desc)    Java domain records + adapters
      |                     |
      +---------------------+
      |
      v
Annotation processor -> adapters/clients/resources/CLI
      |
      v
CDI registration
```

### 1. Build-Time Discovery

During the Maven build process, the compiler reads `pipeline.yaml` and resolves each internal `service:` step against the declared service class. `@PipelineStep` is still supported for existing code, but it is not required when YAML declares the step contract:

```yaml
steps:
  - name: process-payment
    service: com.app.payment.ProcessPaymentService
    cardinality: ONE_TO_ONE
    input: com.app.domain.PaymentRecord
    output: com.app.domain.PaymentStatus
    inboundMapper: com.app.payment.PaymentRecordMapper
    outboundMapper: com.app.payment.PaymentStatusMapper
```

```java
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
    @Override
    public Uni<PaymentStatus> process(PaymentRecord input) {
        // Implementation
    }
}
```

YAML is the authoritative source for the step contract. If YAML and deprecated `@PipelineStep` contract metadata are both present, the compiler uses YAML and emits a warning.

### 1.1 Orchestrator and Plugin Annotations

The processor also reacts to:

- `@PipelineOrchestrator` on a marker class to enable orchestrator endpoints and (optionally) CLI generation.
- `@PipelinePlugin` on plugin services to enable plugin-server generation and plugin-aspect expansion.

These annotations do not define pipeline steps themselves, but they control which orchestrator and plugin artifacts are generated.

### 2. Compile-time Code Generation

The Pipeline Framework extension processor generates several classes:

- If `transport: GRPC`, gRPC service adapters and gRPC client steps.
- If `transport: REST`, REST resource adapters and REST client steps.
- Synthetic client steps for configured plugin aspects (in a plugin host module).

The processor also writes generated metadata under `META-INF/pipeline/`:

- `order.json`: resolved runtime step order.
- `telemetry.json` and `replay-topology.json`: replay and observability topology.
- `platform.json`: platform, transport, module, and plugin-host metadata.
- `pipeline-contract.json`: deterministic semantic contract and ordered step descriptors used by release validation and queue-async transition-worker validation.

### 2.5 Scaffolding

The template generator provides the necessary scaffolding for:
- Service and orchestrator entry points
- Step interfaces and DTO placeholders
- REST/gRPC adapter wiring and routing
- Configuration files and environment defaults
- Tests and sample fixtures
- CI/workflow stubs for build and release

### 3. Dependency Injection Registration

All generated classes are automatically registered with the CDI container, making them available for injection.


## Guide Pages

- [Pipeline template types and linear contracts](../pipeline-template-dsl)
- [Generated Artifacts](./generated-artifacts)
- [Module Ownership](./module-ownership)
- [Build Integration](./build-integration)
- [Verification and Troubleshooting](./verification-and-troubleshooting)
