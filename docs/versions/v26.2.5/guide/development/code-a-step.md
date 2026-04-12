---
search: false
---

# Code a Step

This guide shows how to implement a pipeline step service and the supporting mappers.

<Callout type="tip" title="Canvas First">
Use the Canvas designer at <a href="https://app.pipelineframework.org" target="_blank" rel="noopener noreferrer">https://app.pipelineframework.org</a> for the fastest path, then refine the generated services in code.
</Callout>

## 1) Pick the Service Interface

Choose the reactive interface that matches your data flow:

- `ReactiveService<I, O>`: one input → one output
- `ReactiveStreamingService<I, O>`: one input → stream of outputs
- `ReactiveStreamingClientService<I, O>`: stream of inputs → one output

## 2) Define the Step Contract in YAML

For internal `service:` steps, declare the contract in `pipeline.yaml`:

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

The `input` and `output` fields specify the service domain types and must match the generic parameters of your service interface (`ReactiveService<I, O>`). The `inboundMapper` and `outboundMapper` fields reference mappers that translate between Domain ↔ External (e.g., `Mapper<Domain, External>`). Mappers should be provided as paired `Mapper<Domain, External>` implementations to validate boundaries and avoid build-time type mismatches.

## 3) Implement the Service

Annotate the class with `@PipelineStep` so build-time generation can discover it.
Keep Java-local concerns such as ordering, thread safety, cache keys, and side effects on the annotation.

```java
@PipelineStep
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Override
    public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
        return Uni.createFrom().item(/* processed payment status */);
    }
}
```

## 4) Add Mappers

Create pair-based MapStruct mappers using TPF's `Mapper<Domain, External>` interface.
Use one mapper per boundary.

Note: The Java type names you choose in your pipeline YAML (or the web UI) drive the DTO/domain fields and the generated proto mappings. See [Data Types](/versions/v26.2.5/guide/evolve/data-types) for the full list and defaults.

```java
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN
)
public interface PaymentRecordMapper extends Mapper<PaymentRecord, PaymentRecordDto> {

    @Override
    PaymentRecord fromExternal(PaymentRecordDto dto);

    @Override
    PaymentRecordDto toExternal(PaymentRecord domain);
}
```

## 5) Handle Errors

Use Mutiny error handling in your reactive chain:

```java
return processPayment(paymentRecord)
    .onItem().transform(result -> createPaymentStatus(paymentRecord, result))
    .onFailure().recoverWithUni(error -> Uni.createFrom().item(createErrorStatus(paymentRecord, error)));
```

Use Item Reject Sink flows for per-item recoverable failures that should be audited and handled later:

```java
return processBatch(batchItem)
    .onFailure().recoverWithUni(error ->
        rejectItem(batchItem, error)
            .replaceWith(createSkippedStatus(batchItem)));
```

`rejectItem(...)` and `rejectStream(...)` are for expected per-item business rejections
that must be tracked and re-driven without failing the full execution.
See [Item Reject Sink](/versions/v26.2.5/guide/development/item-reject-sink) for the canonical model and wiring.

## 6) Test in Isolation

```java
@QuarkusTest
class ProcessPaymentServiceTest {

    @Inject
    ProcessPaymentService service;

    @Test
    void testSuccessfulPaymentProcessing() {
        PaymentRecord record = createTestPaymentRecord();
        Uni<PaymentStatus> result = service.process(record);
        UniAssertSubscriber<PaymentStatus> subscriber =
            result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();
    }
}
```

## Best Practices

1. Keep step logic focused on a single responsibility.
2. Prefer non-blocking I/O and reactive composition.
3. Choose failure handling by intent: use domain responses for expected business outcomes (for example validation/state conflicts), use Item Reject Sink (`rejectItem` / `rejectStream`) for per-item recoverable processing failures that must be tracked for downstream handling, and use execution DLQ for systemic or unrecoverable pipeline/execution failures.
4. Validate input early and consistently.
