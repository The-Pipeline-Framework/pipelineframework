---
search: false
---

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
- `delegate` (for operator steps only)

### Compatibility-Only Members

The following `@PipelineStep` members remain supported for legacy internal services, but are deprecated for current authoring:

- `inputType`
- `outputType`
- `inboundMapper`
- `outboundMapper`
- `stepType`
- `backendType`

New internal services should not author those members.

### Operator-Specific YAML Fields (Legacy)

For operator/delegated steps, the following fields were previously supported on the `@PipelineStep` annotation but are now configured exclusively in YAML:

- `operator` (use a `class::method` reference, e.g., `operator: com.example.OperatorClass::method`)
- `operatorMapper`
- `externalMapper`

These operator-specific fields must use fully-qualified class::method references in YAML and are resolved/validated at build time. They are not part of the current `@PipelineStep` authoring surface for internal services.

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
