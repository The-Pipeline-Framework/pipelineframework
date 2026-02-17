# Annotations

The Pipeline Framework uses annotations to simplify configuration and automatic generation of pipeline components.

## @PipelineStep

The `@PipelineStep` annotation marks a class as a pipeline step and enables automatic generation of adapters for the configured transport (gRPC or REST). The framework encourages append-only persistence via plugins, but step implementations can perform any storage behavior required by your application.

### Parameters

- `inputType`: The input type for this step (domain type)
- `outputType`: The output type for this step (domain type)
- `stepType`: The step type (StepOneToOne, StepOneToMany, StepManyToOne, StepManyToMany, StepSideEffect)
- `inboundMapper`: The inbound mapper class for this pipeline service/step - handles conversion from gRPC to domain types (using MapStruct-based unified Mapper interface). **Not required when `externalMapper` is provided.**
- `outboundMapper`: The outbound mapper class for this pipeline service/step - handles conversion from domain to gRPC types (using MapStruct-based unified Mapper interface). **Not required when `externalMapper` is provided.**
- `sideEffect`: Optional plugin service type used to generate side-effect client/server adapters
- `ordering`: Ordering requirement for the generated client step
- `threadSafety`: Thread safety declaration for the generated client step
- `delegate`: Specifies the delegate service class that provides the actual execution implementation. When present, the annotated class becomes a client-only step that delegates to the specified service. When absent (defaults to Void.class), the annotated class is treated as a traditional internal step. The delegate service must implement one of the reactive service interfaces: `ReactiveService`, `ReactiveStreamingService`, or `ReactiveBidirectionalStreamingService`.
- `externalMapper`: Specifies the external mapper class that maps between the step's domain types and the delegate service's entity types. This is used when the step's input/output types differ from the delegate service's types. When absent (defaults to Void.class), no external mapping is performed and the step's types must match the delegate's types. **When `externalMapper` is provided, it performs both inbound and outbound mapping, so `inboundMapper` and `outboundMapper` are not required.** The `ExternalMapper` interface is defined in `org.pipelineframework.mapper.ExternalMapper` and requires four type parameters: `<TAppIn, TLibIn, TAppOut, TLibOut>`.

`backendType` is a legacy annotation field and is ignored by the current processor.

### Example

```java
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class,
   stepType = StepOneToOne.class,
   inboundMapper = PaymentRecordMapper.class,
   outboundMapper = PaymentStatusMapper.class,
   ordering = OrderingRequirement.RELAXED,
   threadSafety = ThreadSafety.SAFE
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
    @Override
    public Uni<PaymentStatus> process(PaymentRecord input) {
        // Implementation
    }
}

// Example with delegation to an external library service
// Use delegation when integrating with external libraries or services that already provide the required functionality
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class,
   stepType = StepOneToOne.class,
   delegate = ExternalPaymentService.class,  // Delegates to external library service (must implement a supported Reactive*Service interface)
   externalMapper = PaymentExternalMapper.class  // Maps between domain and library types
)
@ApplicationScoped
public class DelegatedPaymentService {
    // This service delegates to ExternalPaymentService
    // and uses PaymentExternalMapper to convert between types
}

// Example with delegation but without externalMapper (when types already match)
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class,
   stepType = StepOneToOne.class,
   delegate = ExternalPaymentService.class  // Delegates to external library service with matching types
   // No externalMapper needed when input/output types match the delegate's types
)
@ApplicationScoped
public class SimpleDelegatedPaymentService {
    // This service delegates to ExternalPaymentService directly
    // because the input/output types already match
}
```

## Usage

Developers only need to:

1. Annotate their service class with `@PipelineStep`
2. Create MapStruct-based mapper interfaces that extend the `Mapper<Grpc, Dto, Domain>` interface
3. Implement the service interface (`ReactiveService`, `ReactiveStreamingService`, `ReactiveStreamingClientService`, or `ReactiveBidirectionalStreamingService`)

Parallelism is configured at the pipeline level (`pipeline.parallelism` and `pipeline.max-concurrency`).
The `ordering` and `threadSafety` values on `@PipelineStep` are propagated to the generated client step,
which the runtime uses to decide parallelism under `AUTO`.

Transport selection (gRPC vs REST) is configured globally in `pipeline.yaml`, not on the annotation.

The framework automatically generates and registers the adapter beans at build time.
