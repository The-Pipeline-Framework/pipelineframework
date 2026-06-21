# Build Integration

## Build Process Integration

### Maven Configuration

The pipeline framework integrates with the Maven build process. Both runtime and deployment components are bundled in a single dependency:

```xml
<!-- pom.xml dependencies -->
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>pipelineframework</artifactId>
</dependency>
```

### Annotation Processor Execution

The annotation processor runs during the `compile` phase:

```bash
# During mvn compile
[INFO] --- quarkus:3.28.0.CR1:generate-code (default) @ service-module ---
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Loading pipeline.yaml
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Resolving YAML-declared services and operators
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessPaymentGrpcClientStep
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated SendPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated SendPaymentGrpcClientStep
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessAckPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessAckPaymentGrpcClientStep
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated step implementations and service adapters
```

### Required gRPC Descriptor Set Generation

The annotation processor resolves gRPC bindings from a protobuf descriptor set. Configure your build to emit a
descriptor set (for example via Quarkus gRPC codegen) or pass `protobuf.descriptor.file`/`protobuf.descriptor.path`
to the annotation processor if you have a custom descriptor location.

## Customization Points

### Extending Generated Classes

While generated classes are typically not modified directly, you can extend them:

```java
// Custom extension of generated step
@ApplicationScoped
public class CustomProcessPaymentGrpcClientStep extends ProcessPaymentGrpcClientStep {

    @Override
    public Uni<PaymentStatus> applyOneToOne(PaymentRecord input) {
        // Add custom logic before/after calling super
        return super.applyOneToOne(input)
            .onItem().invoke(status -> {
                // Custom post-processing
                logPaymentStatus(status);
            });
    }

    private void logPaymentStatus(PaymentStatus status) {
        // Custom logging logic
    }
}
```

### Customizing Generation

Use configuration and transport settings instead of transport-specific annotation fields:

- Set `transport: GRPC` or `transport: REST` in `pipeline.yaml`.
- Override REST paths with `pipeline.rest.path.<ServiceName>` in `application.properties`.
