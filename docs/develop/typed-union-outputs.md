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
    input: PaymentRequest
    output: PaymentOutcome
```

For protobuf-backed boundaries, provide normal mappers for each variant payload. TPF generates the union wrapper mapper and composes those variant mappers, so application code does not need to write or declare a `PaymentOutcome` mapper. The FUNCTION platform mode (over REST, gRPC, or LOCAL transport), checkpoint JSON, and REST boundaries use the sealed union type directly.

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
- Union-routed branching is opt-in. TPF detects branch awareness from `accepts`, `terminal: true`, or a step's logical `input` alone.

TPFGo uses this shape in the payment capture pipeline, where `PaymentOutcome` replaces a status-field result record while keeping the pipeline linear.

## Union-Routed Branching

TPF can route a linear pipeline by contract type without turning the DSL into a graph workflow.

The decision stays in Java. YAML only declares which concrete contract types a step accepts:

```yaml
steps:
  - name: Classify Order
    cardinality: ONE_TO_ONE
    input: OrderRequest
    output: OrderDecision

  - name: Reserve Stock
    cardinality: ONE_TO_ONE
    input: OrderDecision
    output: StockReserved
    accepts:
      - PhysicalOrder

  - name: Provision License
    cardinality: ONE_TO_ONE
    input: OrderDecision
    output: LicenseProvisioned
    accepts:
      - DigitalOrder

  - name: Request Manual Review
    cardinality: ONE_TO_ONE
    input: OrderDecision
    output: ManualReviewRequested
    accepts:
      - ManualReviewOrder

  - name: Finalize Order
    cardinality: ONE_TO_ONE
    input: OrderCompletion
    output: FinalizedOrder
    terminal: true
```

Rules:

- The pipeline stays a linear sequence of authored steps.
- `accepts` entries must be concrete contract types, not predicates and not union names.
- If `accepts` is omitted, TPF implicitly accepts every concrete leaf type resolved from logical `input`.
- A union input therefore accepts all of its variants by default. Use explicit `accepts` when the step handles only a subset.
- Branch-aware routing currently supports `ONE_TO_ONE` steps only.
- A branch-aware pipeline must declare exactly one `terminal: true` step, and it must be last.
- A publish sink after the step sequence does not satisfy or infer the terminal merge; author the merge step explicitly.

Runtime behavior:

- If the current item matches a step's accepted type set, TPF executes the step normally.
- If it does not match, TPF skips the step as `not_applicable`, records a replay event, and passes the item through unchanged.
- The terminal merge step must accept every reachable branch-end alternative. Otherwise the build fails.

When a step's logical `input` references a concrete message, omitting `accepts` implicitly accepts that message:

```yaml
steps:
  - name: Reserve Stock
    input: PhysicalOrder
    output: StockReserved
    # no accepts — implicitly accepts PhysicalOrder

  - name: Provision License
    input: DigitalOrder
    output: LicenseProvisioned
    # no accepts — implicitly accepts DigitalOrder
```

This is equivalent to writing `accepts: [PhysicalOrder]` and `accepts: [DigitalOrder]`.

When logical `input` references a union, omitting `accepts` implicitly accepts every variant. That is normally the clearest declaration for a terminal merge:

```yaml
- name: Finalize Order
  cardinality: ONE_TO_ONE
  input: OrderCompletion
  output: FinalizedOrder
  terminal: true
```

Here `Finalize Order` accepts `StockReserved`, `LicenseProvisioned`, and `ManualReviewRequested` because those are the variants of `OrderCompletion`. Listing all three under `accepts` is equivalent but unnecessary. Use explicit `accepts` for branch-specific steps that handle only part of a union.
