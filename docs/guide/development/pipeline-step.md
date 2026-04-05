# Annotations

The Pipeline Framework uses `@PipelineStep` to mark internal execution services that are referenced from YAML.

## `@PipelineStep`

`@PipelineStep` is the discovery marker for internal `service:` steps. It does not define the step contract by itself.
Current internal-step contract metadata belongs in `pipeline.yaml`.

For internal services, YAML is the canonical source of truth for:

- `service`
- `cardinality`
- `input`
- `output`
- `inboundMapper`
- `outboundMapper`

### Current Usage

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
@PipelineStep(
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

### Keep on the Annotation

Use `@PipelineStep` for Java-local execution concerns:

- `cacheKeyGenerator`
- `ordering`
- `threadSafety`
- `sideEffect`
- delegated/operator-only fields such as `operator`, `delegate`, `operatorMapper`, and `externalMapper`

### Compatibility-Only Members

The following `@PipelineStep` members remain supported for legacy internal services, but are deprecated for current authoring:

- `inputType`
- `outputType`
- `inboundMapper`
- `outboundMapper`
- `stepType`
- `backendType`

New internal services should not author those members.

## Usage

1. Define the internal step contract in `pipeline.yaml`.
2. Annotate the implementation class with `@PipelineStep`.
3. Implement the matching reactive service interface:
   - `ReactiveService<I, O>`
   - `ReactiveStreamingService<I, O>`
   - `ReactiveStreamingClientService<I, O>`
   - `ReactiveBidirectionalStreamingService<I, O>`
4. Keep YAML cardinality aligned with the implemented reactive interface.

Parallelism is configured at the pipeline level (`pipeline.parallelism` and `pipeline.max-concurrency`).
The `ordering` and `threadSafety` values on `@PipelineStep` are propagated to the generated client step,
which the runtime uses to decide parallelism under `AUTO`.

Transport selection (gRPC vs REST) is configured in `pipeline.yaml`, not on the annotation.
