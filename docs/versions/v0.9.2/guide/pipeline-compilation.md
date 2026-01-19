---
search: false
---

# Pipeline Compilation and Generation

This guide explains how The Pipeline Framework's annotation processor works to automatically generate pipeline applications and adapters at build time.

## Overview

The Pipeline Framework uses annotation processing to automatically generate the necessary infrastructure for pipeline execution. When you annotate your services with `@PipelineStep`, the framework's annotation processor:

1. Discovers all annotated services at build time
2. Generates gRPC and REST adapters for each service
3. Creates a complete pipeline application that orchestrates all steps
4. Registers all generated components with the dependency injection container

This eliminates the need for manual configuration and ensures consistency across your pipeline.

## Annotation Processing Workflow

### 1. Build-Time Discovery
During the Maven build process, the annotation processor scans for `@PipelineStep` annotations:

```java
// At build time, the processor finds this annotation
@PipelineStep(
    order = 1,
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    backendType = GenericGrpcReactiveServiceAdapter.class,
    grpcStub = MutinyProcessPaymentServiceGrpc.MutinyProcessPaymentServiceStub.class,
    grpcImpl = MutinyProcessPaymentServiceGrpc.ProcessPaymentServiceImplBase.class,
    inboundMapper = PaymentRecordInboundMapper.class,
    outboundMapper = PaymentStatusOutboundMapper.class,
    grpcClient = "process-payment",
    autoPersist = true
)
@ApplicationScoped
public class ProcessPaymentService implements StepOneToOne<PaymentRecord, PaymentStatus> {
    // Implementation
}
```

### 2. Code Generation
The processor generates several classes:

#### a) gRPC Adapter Class
A service endpoint that handles gRPC requests:

```java
// Generated: ProcessPaymentServiceGrpcService.java
@GrpcService
public class ProcessPaymentServiceGrpcService extends GenericGrpcReactiveServiceAdapter<...> {
    @Inject
    PaymentRecordInboundMapper inboundMapper;
    
    @Inject
    PaymentStatusOutboundMapper outboundMapper;
    
    @Inject
    ProcessPaymentService service;
    
    @Inject
    PersistenceManager persistenceManager;
    
    // Generated methods for gRPC endpoint
}
```

#### b) Step Class
A client-side step implementation for pipeline execution:

```java
// Generated: ProcessPaymentServiceStep.java
@ApplicationScoped
public class ProcessPaymentServiceStep implements StepOneToOne<PaymentRecord, PaymentStatus> {
    @Inject
    MutinyProcessPaymentServiceGrpc.MutinyProcessPaymentServiceStub grpcClient;
    
    @GrpcClient("process-payment")
    // Generated methods for pipeline step execution
}
```

#### c) Orchestrator Application
The orchestrator is generated from a template that implements QuarkusApplication and Callable:

```java
// Generated: OrchestratorApplication.java (from template)
@CommandLine.Command(
    name = "orchestrator",
    mixinStandardHelpOptions = true,
    version = "1.0.0",
    description = "{{appName}} Orchestrator Service")
@Dependent
public class OrchestratorApplication implements QuarkusApplication, Callable<Integer> {

    @Option(
        names = {"-i", "--input"}, 
        description = "Input value for the pipeline",
        defaultValue = ""
    )
    String input;

    @Inject
    PipelineExecutionService pipelineExecutionService;

    @Override
    public int run(String... args) {
        return new CommandLine(this).execute(args);
    }

    public Integer call() {
        // Use command line option if provided, otherwise fall back to environment variable
        String actualInput = input;
        if (actualInput == null || actualInput.trim().isEmpty()) {
            actualInput = System.getenv("PIPELINE_INPUT");
        }
        
        if (actualInput == null || actualInput.trim().isEmpty()) {
            System.err.println("Input parameter is required");
            return CommandLine.ExitCode.USAGE;
        }
        
        Multi<{{firstInputTypeName}}> inputMulti = getInputMulti(actualInput);

        // Execute the pipeline with the processed input using injected service
        pipelineExecutionService.executePipeline(inputMulti)
            .collect().asList()
            .await().indefinitely();

        System.out.println("Pipeline execution completed");
        return CommandLine.ExitCode.OK;
    }
    
    // This method needs to be implemented by the user after template generation
    // based on their specific input type and requirements
    private Multi<{{firstInputTypeName}}> getInputMulti(String input) {
        // User implementation required
    }
}
```

### 3. Dependency Injection Registration
All generated classes are automatically registered with the CDI container, making them available for injection.

## Generated Classes in Detail

### gRPC Adapter Generation

The gRPC adapter acts as a server-side endpoint that:

1. Receives gRPC requests
2. Uses the inbound mapper to convert gRPC objects to domain objects
3. Calls the actual service implementation
4. Uses the outbound mapper to convert domain objects to gRPC responses
5. Handles persistence if auto-persist is enabled

```java
// Generated class structure
public class ServiceNameGrpcService extends GenericGrpcReactiveServiceAdapter<GRpcIn, DomainIn, DomainOut, GRpcOut> {
    
    @Inject
    InboundMapper<GRpcIn, DomainIn> inboundMapper;
    
    @Inject
    OutboundMapper<DomainOut, GRpcOut> outboundMapper;
    
    @Inject
    ServiceName service;  // Your actual service implementation
    
    @Inject
    PersistenceManager persistenceManager;
    
    public Uni<GRpcOut> remoteProcess(GRpcIn grpcRequest) {
        // Convert gRPC to domain
        DomainIn domainInput = inboundMapper.fromGrpcFromDto(grpcRequest);
        
        // Auto-persist if enabled
        Uni<DomainIn> persistedInput = getPersistedUni(domainInput);
        
        // Process through service
        Uni<DomainOut> domainOutput = persistedInput
            .onItem().transformToUni(service::process);
            
        // Convert domain to gRPC
        return domainOutput.onItem().transform(outboundMapper::toDtoToGrpc);
    }
}
```

### Step Class Generation

The step class acts as a client-side component that:

1. Connects to the gRPC service
2. Implements the pipeline step interface
3. Handles the conversion between domain objects and gRPC calls

```java
// Generated class structure
@ApplicationScoped
public class ServiceNameStep implements StepOneToOne<DomainIn, DomainOut> {
    
    @Inject
    @GrpcClient("grpc-client-name")
    StubClass grpcClient;
    
    public Uni<DomainOut> applyOneToOne(Uni<DomainIn> input) {
        return input.onItem().transformToUni(domainInput -> {
            // Convert domain to gRPC
            GRpcIn grpcInput = convertDomainToGrpc(domainInput);
            
            // Call remote service
            return grpcClient.remoteProcess(grpcInput);
        });
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
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generating adapters for annotated services
[INFO] [org.pipelineframework.processor.PipelineProcessor] Found 3 @PipelineStep annotated services
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated ProcessPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated ProcessPaymentServiceStep
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated SendPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated SendPaymentServiceStep
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated ProcessAckPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated ProcessAckPaymentServiceStep
[INFO] [org.pipelineframework.processor.PipelineProcessor] Generated step implementations and service adapters
```

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
public class CustomProcessPaymentServiceStep extends ProcessPaymentServiceStep {
    
    @Override
    public Uni<PaymentStatus> applyOneToOne(Uni<PaymentRecord> input) {
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

The annotation processor can be customized through annotation parameters:

```java
@PipelineStep(
    order = 1,
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    backendType = CustomGrpcReactiveServiceAdapter.class,  // Custom adapter
    grpcStub = MutinyProcessPaymentServiceGrpc.MutinyProcessPaymentServiceStub.class,
    grpcImpl = MutinyProcessPaymentServiceGrpc.ProcessPaymentServiceImplBase.class,
    inboundMapper = PaymentRecordInboundMapper.class,
    outboundMapper = PaymentStatusOutboundMapper.class,
    grpcClient = "process-payment",
    autoPersist = true,
    retryLimit = 5,
    retryWait = "PT1S",
    maxBackoff = "PT30S",
    jitter = true
)
```

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