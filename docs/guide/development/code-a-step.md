# Code a Step

This guide shows how to implement a pipeline step service and the supporting mappers.

<Callout type="tip" title="Canvas First">
Use the Canvas designer at <a href="https://app.pipelineframework.org" target="_blank" rel="noopener noreferrer">https://app.pipelineframework.org</a> for the fastest path, then refine the generated services in code.
</Callout>

## 1) Pick the Service Interface

Choose the interface family that matches your data flow:

- `ReactiveService<I, O>`: one input → one output
- `ReactiveStreamingService<I, O>`: one input → stream of outputs
- `ReactiveStreamingClientService<I, O>`: stream of inputs → one output
- `ReactiveBidirectionalStreamingService<I, O>`: stream of inputs → stream of outputs
- `BlockingService<I, O>`: one input → one output, synchronous user code
- `BlockingStreamingService<I, O>`: one input → many outputs, returns `List<O>` and materializes the full output
- `BlockingIteratorService<I, O>`: one input → many outputs, returns `CloseableIterator<O>` for incremental blocking streaming
- `BlockingStreamingClientService<I, O>`: many inputs → one output, consumes `List<I>`
- `BlockingBidirectionalStreamingService<I, O>`: many inputs → many outputs, transforms `List<I>` to `List<O>`

Use the blocking family in two explicit modes:

| Need | Recommended contract |
| --- | --- |
| Bounded batch or batch-oriented SDKs | `BlockingStreamingService`, `BlockingStreamingClientService`, `BlockingBidirectionalStreamingService` |
| Blocking library with a cursor, iterator, reader, or JDBC-style incremental API | `BlockingIteratorService` |
| Async or naturally non-blocking library | Reactive service interfaces |

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

When a step can produce several closed business outcomes, keep the single-output shape and declare the output as a typed union. See [Typed Union Outputs](/guide/development/typed-union-outputs).

## 3) Implement the Service

Annotate the class with `@PipelineStep` so build-time generation can discover it.
Keep Java-local concerns such as ordering, thread safety, cache keys, and side effects on the annotation.

Reactive authoring:

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

Blocking authoring:

```java
@PipelineStep(runOnVirtualThreads = true)
@ApplicationScoped
public class ProcessCsvService implements BlockingIteratorService<CsvFile, PaymentRecord> {

    @Override
    public CloseableIterator<PaymentRecord> iterateBlocking(CsvFile input) {
        return openCsvIterator(input);
    }
}
```

The framework keeps transport adapters reactive in both cases. Blocking service interfaces expose synchronous methods such as `processBlocking(...)`, and the framework supplies the reactive `process(...)` adapter. Blocking services are wrapped in a generated reactive bridge and offloaded to worker threads by default, or to virtual threads when `runOnVirtualThreads = true`.

`BlockingStreamingService`, `BlockingStreamingClientService`, and `BlockingBidirectionalStreamingService` are materializing contracts. They trade away automatic backpressure and also increase heap usage, GC pressure, first-item latency, and whole-batch retry cost. `BlockingIteratorService` reduces those materialization costs, but it is still blocking work and should be used only when synchronous authoring is worth the throughput tradeoff.

## 4) Add Mappers

Create pair-based MapStruct mappers using TPF's `Mapper<Domain, External>` interface.
Use one mapper per boundary.

Note: The Java type names you choose in your pipeline YAML (or the web UI) drive the DTO/domain fields and the generated proto mappings. See [Data Types](/guide/evolve/data-types) for the full list and defaults.

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
See [Item Reject Sink](/guide/development/item-reject-sink) for the canonical model and wiring.

Blocking services can throw normally. The generated bridge feeds those failures into the same retry, reject, DLQ, and telemetry paths that the reactive step APIs use.

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
2. Prefer non-blocking I/O and reactive composition when you need streaming efficiency or backpressure.
3. Use blocking services for straightforward synchronous code, but keep them isolated and explicit.
4. Choose failure handling by intent: use domain responses for expected business outcomes (for example validation/state conflicts), use Item Reject Sink (`rejectItem` / `rejectStream`) for per-item recoverable processing failures that must be tracked for downstream handling, and use execution DLQ for systemic or unrecoverable pipeline/execution failures.
5. Validate input early and consistently.
