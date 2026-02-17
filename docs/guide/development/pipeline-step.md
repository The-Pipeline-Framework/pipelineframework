# Annotations

The Pipeline Framework uses annotations to describe internal step service metadata used by YAML-driven generation.

## @PipelineStep

The `@PipelineStep` annotation marks an internal execution service and provides compile-time metadata. Step generation is driven by `pipeline.yaml`; `@PipelineStep` alone does not create a generated step.

### Parameters

- `inputType`: The input type for this step (domain type)
- `outputType`: The output type for this step (domain type)
- `stepType`: The step type (StepOneToOne, StepOneToMany, StepManyToOne, StepManyToMany, StepSideEffect)
- `inboundMapper`: The inbound mapper class for this pipeline service/step - handles conversion from gRPC to domain types (using MapStruct-based unified Mapper interface). **Not required when `externalMapper` is provided.**
- `outboundMapper`: The outbound mapper class for this pipeline service/step - handles conversion from domain to gRPC types (using MapStruct-based unified Mapper interface). **Not required when `externalMapper` is provided.**
- `sideEffect`: Optional plugin service type used to generate side-effect client/server adapters
- `ordering`: Ordering requirement for the generated client step
- `threadSafety`: Thread safety declaration for the generated client step
- `delegate`: Specifies the delegate service class used for delegated execution when `delegate() != Void.class`. The delegate service must implement one of the supported reactive service interfaces (`ReactiveService`, `ReactiveStreamingService`, `ReactiveStreamingClientService`/`ReactiveClientStreamingService`, `ReactiveBidirectionalStreamingService`).
- `externalMapper`: Specifies the external mapper class that maps between application and delegate/operator types. `externalMapper()` is only considered when `delegate() != Void.class`; it is ignored when `delegate() == Void.class`. When delegated input/output types differ, `externalMapper()` is required; when types match, it is optional.

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

// Example with delegation to an operator service
// Use delegation when integrating with external libraries or services that already provide the required functionality
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class,
   stepType = StepOneToOne.class,
   delegate = ExternalPaymentService.class,  // Delegates to operator service (must implement a supported Reactive*Service interface)
   externalMapper = PaymentExternalMapper.class  // Maps between domain and operator types
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
   delegate = ExternalPaymentService.class  // Delegates to operator service with matching types
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

1. Define steps in `pipeline.yaml` (`service` for internal, `delegate` for delegated)
2. For internal services, annotate the execution class with `@PipelineStep`
2. Create MapStruct-based mapper interfaces that extend the `Mapper<Grpc, Dto, Domain>` interface
3. Implement the service interface (`ReactiveService`, `ReactiveStreamingService`, `ReactiveStreamingClientService`, or `ReactiveBidirectionalStreamingService`)

Parallelism is configured at the pipeline level (`pipeline.parallelism` and `pipeline.max-concurrency`).
The `ordering` and `threadSafety` values on `@PipelineStep` are propagated to the generated client step,
which the runtime uses to decide parallelism under `AUTO`.

Transport selection (gRPC vs REST) is configured globally in `pipeline.yaml`, not on the annotation.

The framework automatically generates and registers the adapter beans at build time.
