# Annotations

The Pipeline Framework uses annotations to simplify configuration and automatic generation of pipeline components.

## @PipelineStep

The `@PipelineStep` annotation marks a class as a pipeline step and enables automatic generation of adapters for the configured transport (gRPC or REST). The framework encourages append-only persistence via plugins, but step implementations can perform any storage behavior required by your application.

### Parameters

- `inputType`: The input type for this step (domain type)
- `outputType`: The output type for this step (domain type)
- `stepType`: The step type (StepOneToOne, StepOneToMany, StepManyToOne, StepManyToMany, StepSideEffect, or blocking variants)
- `inboundMapper`: The inbound mapper class for this pipeline service/step - handles conversion from gRPC to domain types (using MapStruct-based unified Mapper interface)
- `outboundMapper`: The outbound mapper class for this pipeline service/step - handles conversion from domain to gRPC types (using MapStruct-based unified Mapper interface)
- `runOnVirtualThreads`: Whether to offload server processing to virtual threads, i.e. for I/O-bound operations (defaults to `false`)
- `sideEffect`: Optional plugin service type used to generate side-effect client/server adapters
- `ordering`: Ordering requirement for the generated client step
- `threadSafety`: Thread safety declaration for the generated client step

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
