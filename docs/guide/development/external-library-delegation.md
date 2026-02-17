# YAML-Driven Pipeline Configuration

## Overview

The Pipeline Framework (TPF) uses YAML-driven pipeline configuration, where step generation is driven by YAML configuration rather than by the presence of `@PipelineStep` annotations alone.

## Key Concepts

### YAML is Authoritative

In the new architecture:
- YAML configuration drives step generation
- @PipelineStep annotations only mark internal execution services
- External delegated services ("operator") require **zero user-written Java glue classes when using operator types directly (Option 1)**
- When using domain types (Option 2), you need to provide an `ExternalMapper` implementation

### Two Kinds of Steps

There are two kinds of steps that can be defined in YAML:

1. **Internal Steps**: Refer to services within the application annotated with @PipelineStep
2. **Delegated Steps**: Refer to operators that are NOT annotated with @PipelineStep

### Type Layers for Delegated Steps

When delegation is used, there are four conceptual layers:

```
Application Domain Types
        ↓
External Mapper (App-provided)
        ↓
Operator Entity/DTO Types
        ↓
Operator Transport Mapper (DTO ↔ Proto)
        ↓
Transport Layer (grpc/http/etc.)
```

## YAML Configuration Format

### Internal Steps

To define an internal step that references a service annotated with @PipelineStep:

```yaml
steps:
  - name: process-payment
    service: com.app.payment.ProcessPaymentService
```

### Delegated Steps

To define a delegated step that references an external operator service:

```yaml
steps:
  - name: embed
    delegate: com.example.ai.sdk.service.EmbeddingService
    input: com.app.domain.TextChunk
    output: com.app.domain.Vector
    externalMapper: com.app.mapper.ChunkVectorMapper
```

### Full Example

Here's a complete pipeline.yaml example:

```yaml
appName: "My Pipeline App"
basePackage: "com.app.pipeline"
transport: "GRPC"
runtimeLayout: "MODULAR"
steps:
  # Internal step referencing a service annotated with @PipelineStep
  - name: process-payment
    service: com.app.payment.ProcessPaymentService
    
  # Delegated step referencing an external operator service
  - name: embed-text
    delegate: com.example.ai.sdk.service.EmbeddingService
    input: com.app.domain.TextChunk
    output: com.app.domain.Embedding
    externalMapper: com.app.mapper.TextEmbeddingMapper
    
  # Delegated step without external mapper (uses operator types directly)
  - name: send-email
    delegate: com.example.email.service.EmailService
    input: com.example.email.dto.EmailRequest
    output: com.example.email.dto.EmailResponse
```

## Creating Internal Services

For internal steps, you still need to create services annotated with @PipelineStep:

```java
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class,
   stepType = StepOneToOne.class,
   inboundMapper = PaymentRecordMapper.class,
   outboundMapper = PaymentStatusMapper.class
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
    @Override
    public Uni<PaymentStatus> process(PaymentRecord input) {
        // Implementation
    }
}
```

## Using operator Delegation

### Option 1 — Use Operator Types Directly

When you want to use the operator's types directly without transformation:

```yaml
steps:
  - name: send-email
    delegate: com.example.email.service.EmailService
    input: com.example.email.dto.EmailRequest
    output: com.example.email.dto.EmailResponse
```

Requirements:
- Operator must provide inbound/outbound transport mappers
- Cardinality derived from ReactiveService subtype

### Option 2 — Use Domain Types

When you want to abstract away operator types using an external mapper:

```yaml
steps:
  - name: embed-text
    delegate: com.example.ai.sdk.service.EmbeddingService
    input: com.app.domain.TextChunk
    output: com.app.domain.Embedding
    externalMapper: com.app.mapper.TextEmbeddingMapper
```

Where the external mapper is defined as:

```java
public class TextEmbeddingMapper implements ExternalMapper<
    TextChunk,           // Application input type
    EmbeddingRequest,    // Operator input type
    Embedding,           // Application output type
    EmbeddingResult      // Operator output type
> {
    @Override
    public EmbeddingRequest toOperatorInput(TextChunk applicationInput) {
        // Convert from application domain type to operator entity type
        return new EmbeddingRequest(applicationInput.text);
    }

    @Override
    public Embedding toApplicationOutput(EmbeddingResult operatorOutput) {
        // Convert from operator entity type to application domain type
        Embedding result = new Embedding();
        result.vector = operatorOutput.getEmbeddingVector();
        return result;
    }
}
```

## Creating Operator Services

### 1. Execution Service

A plain service implementing one of the reactive service interfaces:

```java
public class EmbeddingService implements ReactiveService<OperatorTextInput, OperatorEmbeddingOutput> {
    @Override
    public Uni<OperatorEmbeddingOutput> process(OperatorTextInput input) {
        // Implementation here
        return Uni.createFrom().item(calculateEmbedding(input));
    }
    
    private OperatorEmbeddingOutput calculateEmbedding(OperatorTextInput input) {
        // Actual embedding calculation
        return new OperatorEmbeddingOutput(new float[]{0.1f, 0.2f, 0.3f});
    }
}
```

**Important**: Operator services must NOT be annotated with @PipelineStep.

### 2. Entity / DTO / Proto Model

Operator must define:
- Entity (business-level contract)
- DTO
- Proto (or transport model)

### 3. Transport Mappers

Operator must ship:
- InboundMapper (Proto → DTO → Entity)
- OutboundMapper (Entity → DTO → Proto)

Exactly like current TPF-generated mappers.

These mappers are owned by the operator.

### 4. Operator Self-Containment

The operator must be fully transport-ready. It must not depend on:
- Application types
- Application mappers
- TPF annotation processing

It is a pure execution module.

## Validation Rules

At compile time, TPF validates:

| Scenario | Expected |
|----------|----------|
| YAML references internal service without annotation | Fail |
| YAML references annotated service | OK |
| YAML references delegate not implementing ReactiveService | Fail |
| Delegate without transport mappers | Fails in strict validation mode and may be a warning in relaxed mode, depending on processor settings and mapper discovery |
| Missing externalMapper when types differ | Fail |
| Annotated service not referenced in YAML | No generation (warning if `pipeline.warnUnreferencedSteps=true`) |

## Processing Options

The following annotation processor options control YAML-driven generation:

| Option | Default | Description |
|--------|---------|-------------|
| `pipeline.config` | (none) | Path to the pipeline YAML configuration file |
| `pipeline.warnUnreferencedSteps` | `true` | Whether to warn about @PipelineStep classes not referenced in YAML |

Example Maven configuration:
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <annotationProcessorPaths>
            <path>
                <groupId>org.pipelineframework</groupId>
                <artifactId>pipelineframework-deployment</artifactId>
                <version>${tpf.version}</version>
            </path>
        </annotationProcessorPaths>
        <compilerArgs>
            <arg>-Apipeline.config=${project.basedir}/src/main/resources/pipeline.yaml</arg>
            <arg>-Apipeline.warnUnreferencedSteps=true</arg>
        </compilerArgs>
    </configuration>
</plugin>
```

## ExternalMapper Interface

The `ExternalMapper` interface is located in:
```
org.pipelineframework.mapper.ExternalMapper
```

When implementing an ExternalMapper:
- All four type parameters must be specified
- The `toOperatorInput` method must not return null
- The `toApplicationOutput` method must not return null
- The mapper class should be public and have a public no-arg constructor

## Migration Guide

### From Annotation-Driven to YAML-Driven

**Old approach** (legacy, not used by strict YAML-driven generation):
```java
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class
)
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
    // Implementation
}
```

**New approach**:
1. Define the service class (with or without @PipelineStep annotation)
2. Reference it in pipeline.yaml:

```yaml
steps:
  - name: process-payment
    service: com.app.payment.ProcessPaymentService
```

## Example

Here's a complete example showing both internal and delegated steps:

**pipeline.yaml**:
```yaml
appName: "Payment Processing Pipeline"
basePackage: "com.app.payment"
transport: "GRPC"
steps:
  # Internal step
  - name: validate-payment
    service: com.app.payment.ValidatePaymentService
    
  # Delegated step to external fraud detection service (using domain types with external mapper)
  - name: detect-fraud
    delegate: com.fraud.detection.FraudDetectionService
    input: com.app.domain.PaymentRequest
    output: com.app.domain.FraudCheckResult
    externalMapper: com.app.mapper.PaymentFraudMapper
    
  # Delegated step to external notification service (using operator types directly)
  - name: send-notification
    delegate: com.notification.service.NotificationService
    input: com.notification.dto.NotificationRequest
    output: com.notification.dto.NotificationResponse
```

**ValidatePaymentService.java**:
```java
@PipelineStep(
   inputType = PaymentRequest.class,
   outputType = PaymentRequest.class
)
public class ValidatePaymentService implements ReactiveService<PaymentRequest, PaymentRequest> {
    @Override
    public Uni<PaymentRequest> process(PaymentRequest input) {
        // Validation logic
        return Uni.createFrom().item(input);
    }
}
```

## Summary

The YAML-driven architecture provides a more flexible and controlled approach to defining pipeline steps. It separates the concern of step definition from implementation, allows for easy integration of operator services, and maintains all the benefits of the previous annotation-driven approach while adding new capabilities for delegation.
