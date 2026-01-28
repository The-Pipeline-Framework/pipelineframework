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

## 2) Implement the Service

Annotate the class with `@PipelineStep` so build-time generation can produce adapters.

```java
@PipelineStep(
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    inboundMapper = PaymentRecordMapper.class,
    outboundMapper = PaymentStatusMapper.class
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Override
    public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
        return Uni.createFrom().item(/* processed payment status */);
    }
}
```

## 3) Add Mappers

Create MapStruct mappers that convert between gRPC, DTO, and domain types.

```java
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN
)
public interface PaymentRecordMapper extends Mapper<PaymentGrpc, PaymentDto, PaymentRecord> {

    @Override
    PaymentDto toDto(PaymentRecord domain);

    @Override
    PaymentRecord fromDto(PaymentDto dto);

    @Override
    PaymentGrpc toGrpc(PaymentDto dto);

    @Override
    PaymentDto fromGrpc(PaymentGrpc grpc);
}
```

## 4) Handle Errors

Use Mutiny error handling in your reactive chain:

```java
return processPayment(paymentRecord)
    .onItem().transform(result -> createPaymentStatus(paymentRecord, result))
    .onFailure().recoverWithUni(error -> Uni.createFrom().item(createErrorStatus(paymentRecord, error)));
```

## 5) Test in Isolation

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
3. Map errors to domain responses or DLQ flows.
4. Validate input early and consistently.
