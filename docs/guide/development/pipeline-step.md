# Annotations

The Pipeline Framework uses annotations to simplify configuration and automatic generation of pipeline components.

## @PipelineStep

The `@PipelineStep` annotation marks a class as a pipeline step and enables automatic generation of gRPC and REST adapters. The framework follows an immutable architecture where no database updates occur during pipeline execution - only appends/preserves.

### Parameters

- `inputType`: The input type for this step (domain type)
- `outputType`: The output type for this step (domain type)
- `stepType`: The step type (StepOneToOne, StepOneToMany, StepManyToOne, StepManyToMany, StepSideEffect, or blocking variants)
- `backendType`: The backend adapter type (GenericGrpcReactiveServiceAdapter, etc.)
- `inboundMapper`: The inbound mapper class for this pipeline service/step - handles conversion from gRPC to domain types (using MapStruct-based unified Mapper interface)
- `outboundMapper`: The outbound mapper class for this pipeline service/step - handles conversion from domain to gRPC types (using MapStruct-based unified Mapper interface)
- `runOnVirtualThreads`: Whether to offload server processing to virtual threads, i.e. for I/O-bound operations (defaults to `false`)
- `grpcEnabled`: Whether to enable gRPC adapter generation for this step (defaults to `true`)
- `restEnabled`: Whether to enable REST adapter generation for this step (defaults to `false`)

### Example

```java
@PipelineStep(
   inputType = PaymentRecord.class,
   outputType = PaymentStatus.class,
   stepType = StepOneToOne.class,
   backendType = GenericGrpcReactiveServiceAdapter.class,
   inboundMapper = PaymentRecordMapper.class,
   outboundMapper = PaymentStatusMapper.class
)
@ApplicationScoped
public class ProcessPaymentService implements StepOneToOne<PaymentRecord, PaymentStatus> {
    // Implementation
}
```

## Usage

Developers only need to:

1. Annotate their service class with `@PipelineStep`
2. Create MapStruct-based mapper interfaces that extend the `Mapper<Grpc, Dto, Domain>` interface
3. Implement the service interface (`StepOneToOne`, etc.)

Parallelism is configured at runtime (StepConfig or `application.properties`), not via `@PipelineStep`.

The framework automatically generates and registers the adapter beans at build time.
