---
search: false
---

# Pipeline Compilation and Generation

This guide explains how The Pipeline Framework's annotation processor works to automatically generate pipeline applications and adapters at build time.

For the architecture overview of the processor, see [Annotation Processor Architecture](../../evolve/annotation-processor-architecture.md)

For a build-phase walkthrough of the generated parent POM, see:
- [Pipeline Parent POM Lifecycle](pipeline-parent-pom-lifecycle.md)
- [CSV Payments POM Lifecycle](csv-payments-pom-lifecycle.md)

## Overview

The Pipeline Framework uses annotation processing to automatically generate the necessary infrastructure for pipeline execution. When you annotate your services with `@PipelineStep`, the framework's annotation processor:

1. Discovers all annotated services at build time
2. Generates transport adapters and client steps for the configured transport (gRPC or REST)
3. Expands configured aspects into synthetic side-effect steps when a plugin host is present
4. Registers all generated components with the dependency injection container

This eliminates the need for manual configuration and ensures consistency across your pipeline.

## Annotation Processing Workflow

### 0. Proto Generation (Pre-Processing)
Before annotation processing, pipeline protobuf descriptors are generated from the pipeline template. The authoritative source is:

- `framework/runtime/src/main/java/org/pipelineframework/proto/PipelineProtoGenerator.java`

This replaces hand-authored `.proto` files in the `common` module (those will be removed).

### Build Timeline (gRPC)

```
Pipeline template
      |
      v
PipelineProtoGenerator
      |
      v
protoc -> descriptor set (.desc)
      |
      v
Annotation processor -> adapters/clients/resources/CLI
      |
      v
CDI registration
```

### 1. Build-Time Discovery
During the Maven build process, the annotation processor scans for `@PipelineStep` annotations:

```java
// At build time, the processor finds this annotation
@PipelineStep(
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    inboundMapper = PaymentRecordInboundMapper.class,
    outboundMapper = PaymentStatusOutboundMapper.class
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
    @Override
    public Uni<PaymentStatus> process(PaymentRecord input) {
        // Implementation
    }
}
```

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

## Generated Classes in Detail

## Role-Specific Output Directories

Generated sources are written into role-specific directories under `target/generated-sources/pipeline`, one per deployment role. Packaging relies on these directories instead of class-name patterns.

Build configuration notes:
- Pass `-Apipeline.generatedSourcesDir=target/generated-sources/pipeline` to the compiler during `compile` only, to avoid warnings during `testCompile`.
- Register the role directories as sources for IDEs via `build-helper-maven-plugin`.
- If tests reference generated classes (e.g., REST resources), register the same directories as test sources in `generate-test-sources`.

### gRPC Adapter Generation

The gRPC adapter acts as a server-side endpoint that:

1. Receives gRPC requests
2. Uses the inbound mapper to convert gRPC objects to domain objects
3. Calls the actual service implementation
4. Uses the outbound mapper to convert domain objects to gRPC responses

```java
// Generated class structure (simplified)
public class ServiceNameGrpcService extends ServiceNameGrpc.ServiceNameImplBase {

    @Inject
    PaymentRecordInboundMapper inboundMapper;

    @Inject
    PaymentStatusOutboundMapper outboundMapper;

    @Inject
    ServiceName service;  // Your actual service implementation

    @Override
    public Uni<PaymentGrpcOut> remoteProcess(PaymentGrpcIn request) {
        // Delegates to an inline GrpcReactiveServiceAdapter based on the streaming shape
        return /* adapter */.remoteProcess(request);
    }
}
```

### gRPC Step Class Generation

The step class acts as a client-side component that:

1. Connects to the gRPC service
2. Implements the pipeline step interface
3. Handles the conversion between domain objects and gRPC calls

```java
// Generated class structure
@ApplicationScoped
public class ServiceNameGrpcClientStep implements StepOneToOne<DomainIn, DomainOut> {
    
    @Inject
    @GrpcClient("service-name")
    StubClass grpcClient;
    
    public Uni<DomainOut> applyOneToOne(DomainIn input) {
        // Convert domain to gRPC
        GRpcIn grpcInput = convertDomainToGrpc(input);

        // Call remote service
        return grpcClient.remoteProcess(grpcInput);
    }
}
```

### Orchestrator Application Structure

The orchestrator application coordinates pipeline execution by using the PipelineExecutionService to connect all generated steps:

```java
// Orchestrator application that coordinates execution
@CommandLine.Command(...)
public class OrchestratorApplication implements QuarkusApplication, Callable<Integer> {
    
    @Inject
    PipelineExecutionService pipelineExecutionService;
    
    public Integer call() {
        // Create input stream from input parameter
        Multi<DomainInput> inputStream = createInputStream(input);
        
        // Execute pipeline using the injected service
        // The service discovers all registered step implementations through dependency injection
        pipelineExecutionService.executePipeline(inputStream)
            .collect().asList()
            .await().indefinitely();
            
        return CommandLine.ExitCode.OK;
    }
}
```

The actual pipeline execution is handled by the PipelineExecutionService which discovers all available step implementations through the StepsRegistry.

## Module Ownership and Dependencies

This build is organized into three groups plus shared scaffolding:

- `common` (scaffolded): DTOs and mappers used by services and orchestrator
- `services`: step implementations + server adapters (gRPC or REST)
- `orchestrator`: orchestrator endpoints + orchestrator CLI + client steps

Client steps are *only* used by the orchestrator (there is no direct step-to-step communication).

### Ownership Matrix (Generated Artifacts)

| Artifact | Owner Module | Consumers |
| --- | --- | --- |
| DTOs + mappers | `common` | `services`, `orchestrator` |
| gRPC server adapters | `services` | runtime/CDI |
| REST resources | `services` | runtime/CDI |
| gRPC client steps | `orchestrator` | orchestrator runtime |
| REST client steps | `orchestrator` | orchestrator runtime |
| Orchestrator endpoints | `orchestrator` | runtime/CDI |
| Orchestrator CLI | `orchestrator` | user |

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
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generating adapters for annotated services
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Found 3 @PipelineStep annotated services
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

## Generated Code Verification

### Viewing Generated Sources

Generated sources can be found in the target directory:

```bash
# Generated sources location
target/generated-sources/annotations/

# Generated classes location  
target/classes/
```

### Debugging Generation Issues

Enable verbose logging to debug generation issues:

```properties
# application.properties
quarkus.log.category."org.pipelineframework.processor".level=DEBUG
```

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

## Troubleshooting

### Common Issues

#### 1. Missing Dependencies
Ensure the required dependency is present. Both runtime and deployment components are bundled in a single dependency:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>pipelineframework</artifactId>
</dependency>
```

#### 2. Annotation Processing Not Running
Verify the processor is on the classpath:

```bash
# Check that deployment module is included
mvn dependency:tree | grep pipeline-framework
```

#### 3. Generated Classes Not Found
Check the generated sources directory:

```bash
# List generated classes
find target/generated-sources -name "*.java" | grep -i pipeline
```

### Debugging Tips

#### Enable Detailed Logging
```properties
# application.properties
quarkus.log.category."org.pipelineframework".level=DEBUG
quarkus.log.category."org.pipelineframework.processor".level=TRACE
```

#### Verify Generated Classes
```bash
# Check that step classes were generated
find target/classes -name "*Step.class" | head -5
# Check that gRPC service classes were generated
find target/classes -name "*GrpcService.class" | head -5
```

#### Clean and Rebuild
```bash
# Clean build to force regeneration
mvn clean compile
```

## Best Practices

### Development Workflow

1. **Annotate Services**: Add `@PipelineStep` to your service classes
2. **Build Project**: Run `mvn compile` to trigger generation
3. **Verify Generation**: Check that generated classes are created
4. **Test Integration**: Run integration tests to verify the pipeline works
5. **Deploy**: Deploy the complete application with generated components

### Maintenance

1. **Keep Annotations Updated**: Update `@PipelineStep` when changing service interfaces
2. **Review Generated Code**: Periodically review generated code for correctness
3. **Monitor Build Logs**: Watch for generation warnings or errors
4. **Test Changes**: Thoroughly test after making changes to annotated services

### Performance Considerations

1. **Minimize Regeneration**: Only rebuild when annotations change
2. **Optimize Mappers**: Ensure mappers are efficient
3. **Configure Retries**: Set appropriate retry limits and wait times
4. **Monitor Resource Usage**: Watch memory and CPU usage of generated components

The Pipeline Framework's annotation processing provides a powerful way to automatically generate pipeline infrastructure while maintaining type safety and reducing boilerplate code. By understanding how this process works, you can leverage its full potential while troubleshooting any issues that may arise.
