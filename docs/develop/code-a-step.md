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
- `BlockingStreamingService<I, O>`: one input → many outputs, returns `List<O>` and materialises the full output
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
types:
  PaymentRecord:
    fields:
      - [id, uuid]
  PaymentStatus:
    fields:
      - [status, string]

steps:
  - name: process-payment
    service: com.app.payment.ProcessPaymentService
    cardinality: ONE_TO_ONE
    input: PaymentRecord
    output: PaymentStatus
    java:
      input: com.app.domain.PaymentRecord
      output: com.app.domain.PaymentStatus
    inboundMapper: com.app.payment.PaymentRecordMapper
    outboundMapper: com.app.payment.PaymentStatusMapper
```

The `input` and `output` fields name logical pipeline contracts. For a locally inspectable service or operator, the optional `java` block asserts inferred types or resolves an ambiguity; for remote or cross-module steps it supplies the coordinator-side domain binding. An unexpected local inference failure remains a compile-time error. The `inboundMapper` and `outboundMapper` fields reference mappers that translate between Domain ↔ External (for example, `Mapper<Domain, External>`). Mappers are paired at a step boundary because the inbound and outbound external types can differ.

When a step can produce several closed business outcomes, keep the single-output shape and declare the output as a typed union.

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
@PipelineStep
@ApplicationScoped
public class ProcessCsvService implements BlockingIteratorService<CsvFile, PaymentRecord> {

    @Override
    public CloseableIterator<PaymentRecord> iterateBlocking(CsvFile input) {
        return openCsvIterator(input);
    }
}
```

The framework keeps transport adapters reactive in both cases. Blocking service interfaces expose synchronous methods such as `processBlocking(...)`, and the framework supplies the reactive `process(...)` adapter. Blocking services are wrapped in a generated reactive bridge and offloaded to worker threads by default.

Virtual-thread offload is configured in YAML, not on `@PipelineStep`.
For Quarkus, YAML-declared internal blocking services that implement the existing blocking service interfaces can opt in; the generated reactive bridge passes `true` to `BlockingExecutionSupport`.
For Spring, YAML-only `REST` or `LOCAL` + `COMPUTE` unary blocking internal steps can set the same YAML flag when authored as `processBlocking(In): Out`.

```yaml
types:
  CsvFile:
    fields:
      - [path, string]
  PaymentRecord:
    fields:
      - [id, uuid]

steps:
  - name: "process csv"
    service: "com.example.ProcessCsvService"
    input: CsvFile
    output: PaymentRecord
    java:
      input: "com.example.CsvFile"
      output: "com.example.PaymentRecord"
    runOnVirtualThreads: true
```

Quarkus generated REST/gRPC entrypoints for virtual-thread steps also receive `@RunOnVirtualThread`.
Spring generated unary steps adapt `processBlocking(In): Out` through `RuntimeAdapters.executeBlocking(..., true)`.

`BlockingStreamingService`, `BlockingStreamingClientService`, and `BlockingBidirectionalStreamingService` are materialising contracts. They trade away automatic backpressure and also increase heap usage, GC pressure, first-item latency, and whole-batch retry cost. `BlockingIteratorService` reduces those materialisation costs, but it is still blocking work and should be used only when synchronous authoring is worth the throughput trade-off.

## 4) Add Mappers

Create pair-based MapStruct mappers using TPF's `Mapper<Domain, External>` interface.
Use one mapper per boundary.

The logical type names in pipeline YAML drive the generated contract. Java bindings and application-owned mappers connect that contract to DTO and domain types. See the [Pipeline Template DSL](/develop/pipeline-template-dsl) for the type model and defaults.

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
See [Item Reject Sink](/develop/item-reject-sink) for the canonical model and wiring.

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
