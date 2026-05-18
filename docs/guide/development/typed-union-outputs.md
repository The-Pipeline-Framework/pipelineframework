# Typed Union Outputs

Use typed union outputs when one step can complete with one of several business outcomes and each outcome has a different contract.

The step still has one declared output type. That output can be a closed union whose variants map to Java sealed interfaces, protobuf `oneof`, and REST discriminator JSON.

## Define the Contract

Declare the variant payloads as normal messages, then declare a top-level union:

```yaml
version: 2
messages:
  PaymentCaptured:
    fields:
      - number: 1
        name: orderId
        type: uuid
      - number: 2
        name: paymentId
        type: uuid
  PaymentRejected:
    fields:
      - number: 1
        name: orderId
        type: uuid
      - number: 2
        name: failureCode
        type: string

unions:
  PaymentOutcome:
    variants:
      captured:
        type: PaymentCaptured
        number: 1
      rejected:
        type: PaymentRejected
        number: 2
```

Use the union name as the step output:

```yaml
steps:
  - name: Capture Payment
    service: com.example.payment.CapturePaymentService
    cardinality: ONE_TO_ONE
    input: com.example.domain.PaymentRequest
    inputTypeName: PaymentRequest
    output: com.example.domain.PaymentOutcome
    outputTypeName: PaymentOutcome
```

For protobuf-backed boundaries, provide normal mappers for each variant payload. TPF generates the union wrapper mapper and composes those variant mappers, so application code does not need to write or declare a `PaymentOutcome` mapper. REST, function-over-REST, checkpoint JSON, and local transport use the sealed union type directly.

## Implement the Domain Type

Model the Java output as a sealed interface:

```java
public sealed interface PaymentOutcome
    permits PaymentCaptured, PaymentRejected {
}

public record PaymentCaptured(UUID orderId, UUID paymentId)
    implements PaymentOutcome {
}

public record PaymentRejected(UUID orderId, String failureCode)
    implements PaymentOutcome {
}
```

The service signature remains a normal one-output step:

```java
@PipelineStep
@ApplicationScoped
public class CapturePaymentService
    implements ReactiveService<PaymentRequest, PaymentOutcome> {

    @Override
    public Uni<PaymentOutcome> process(PaymentRequest request) {
        return capture(request)
            ? Uni.createFrom().item(new PaymentCaptured(request.orderId(), newPaymentId()))
            : Uni.createFrom().item(new PaymentRejected(request.orderId(), "PAYMENT_REJECTED"));
    }
}
```

## Transport Shape

For gRPC, TPF generates a protobuf wrapper with `oneof`:

```proto
message PaymentOutcome {
  oneof outcome {
    PaymentCaptured captured = 1;
    PaymentRejected rejected = 2;
  }
}
```

For REST and checkpoint JSON, use a discriminated JSON object:

```json
{
  "type": "captured",
  "orderId": "11111111-1111-1111-1111-111111111111",
  "paymentId": "22222222-2222-2222-2222-222222222222"
}
```

The TPFGo example uses Jackson polymorphic annotations on the sealed interface and its variants to produce this shape. Framework-generated sealed-type scaffolding can provide that wiring later.

For gRPC, field-level mapping stays in ordinary variant mappers such as `Mapper<PaymentCaptured, PipelineTypes.PaymentCaptured>`. The generated union wrapper mapper only selects the protobuf `oneof` variant and delegates the payload conversion to those mappers.

## Constraints

- Union names must not collide with message names or built-in semantic types.
- Each variant must reference a top-level message.
- Variant names and protobuf field numbers must be unique.
- A union can be used as a step input or output type.
- A union cannot be used as a field inside a normal message in this first version.
- TPF does not route variants automatically; downstream steps receive the union and handle it through normal polymorphic domain behavior.

TPFGo uses this shape in the payment capture pipeline, where `PaymentOutcome` replaces a status-field result record while keeping the pipeline linear.
