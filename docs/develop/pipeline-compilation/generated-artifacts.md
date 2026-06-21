# Generated Artifacts

## Generated Classes in Detail

## Role-Specific Output Directories

Generated sources are written into role-specific directories under `target/generated-sources/pipeline`, one per deployment role. Packaging relies on these directories instead of class-name patterns.

Build configuration notes:
- Pass `-Apipeline.generatedSourcesDir=target/generated-sources/pipeline` to the compiler during `compile` only, to avoid warnings during `testCompile`.
- Register the role directories as sources for IDEs via `build-helper-maven-plugin`.
- If tests reference generated classes (e.g., REST resources), register the same directories as test sources in `generate-test-sources`.

## Generated Pipeline Metadata

The build also emits runtime metadata under `META-INF/pipeline/`:

- `order.json`: resolved runtime execution order
- `telemetry.json`: item-boundary and parent-step telemetry metadata
- `replay-topology.json`: replay and live-topology metadata used by execution-event playback and trace-aware topology surfaces

If you package a grouped runtime such as monolith or pipeline-runtime, keep these resources aligned with the runtime artifact that will execute the pipeline.

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

## Generated Code Verification

### Viewing Generated Sources

Generated sources can be found in the target directory:

```bash
# Generated pipeline sources location
target/generated-sources/pipeline/

# Generated classes location
target/classes/
```

### Debugging Generation Issues

Enable verbose logging to debug generation issues:

```properties
# application.properties
quarkus.log.category."org.pipelineframework.processor".level=DEBUG
```
