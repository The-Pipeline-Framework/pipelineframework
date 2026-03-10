# Item Reject Sink

Item Reject Sink is a step-level business outcome channel for recoverable, per-item failures.
It is not the same as execution failure handling.

Use it when a step can continue processing after rejecting specific items
(for example, malformed records in a batch) and you still need auditability and re-drive.

## When To Use It

Choose failure handling by intent:

1. Use domain responses when the outcome is expected and can be represented directly in business response types.
2. Use Item Reject Sink when individual items must be rejected, tracked, and re-driven while the execution continues.
3. Use execution DLQ only for terminal orchestration failures in queue-async mode.

## Step APIs

Step contracts expose:

1. `rejectItem(...)` for single-item rejection.
2. `rejectStream(...)` for stream-level rejection with sample and count metadata.

Example (`StepOneToOne` style):

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    return processPayment(paymentRecord)
        .onItem().transform(result -> PaymentStatus.approved(result))
        .onFailure().recoverWithUni(error ->
            rejectItem(paymentRecord, error)
                .replaceWith(PaymentStatus.rejected("REJECTED_FOR_REDRIVE")));
}
```

Example (`StepManyToOne` style):

```java
@Override
public Uni<BatchResult> process(Multi<PaymentRecord> input) {
    return aggregate(input)
        .onFailure().recoverWithUni(error ->
            rejectStream(List.of(), 0L, error)
                .replaceWith(BatchResult.completedWithRejects()));
}
```

## What Gets Published

Default reject envelopes are metadata-only:

1. execution/correlation/idempotency metadata when available,
2. step identity,
3. retry/attempt metadata,
4. error class/message,
5. timestamp,
6. deterministic fingerprint.

Payload inclusion is opt-in:

```properties
pipeline.item-reject.include-payload=true
```

Implication:

1. With the default `include-payload=false`, sink entries are designed for audit/triage, not automatic replay payload reconstruction.
2. If you need automated re-drive from reject messages, that replay path is application-owned and must be implemented explicitly.

## Provider Model

Configure with `pipeline.item-reject.*`:

1. `provider`: `log`, `memory`, `sqs`
2. `strict-startup`
3. `publish-failure-policy`
4. provider-specific settings (`memory-capacity`, `sqs.queue-url`, `sqs.region`, `sqs.endpoint-override`)

Production guard:

1. If effective step config enables `recoverOnFailure=true`,
2. and selected sink is non-durable (`log` or `memory`),
3. startup fails in production launch mode.

This protects recover-and-continue flows from silent loss of reject events.

## Relationship To Execution DLQ

Item Reject Sink and execution DLQ serve different scopes:

1. Item Reject Sink: item-level, expected recover-and-continue path.
2. Execution DLQ: execution-level, terminal failure path.

For operational triage and queue-async crash recovery, see
[Error Handling and Recovery](/guide/operations/error-handling).
