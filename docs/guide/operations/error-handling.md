# Error Handling and Recovery

This guide describes how TPF handles failures at two different levels:

1. step-level item recovery (item reject sink), and
2. orchestrator execution-level terminal failures (execution DLQ).

## Failure Channels

| Channel | Scope | Trigger | Default provider | Durable provider |
|---|---|---|---|---|
| Item Reject Sink | individual failed item/stream in a step with `recoverOnFailure=true` | step retries exhausted or non-retryable failure | `log` | `sqs` |
| Execution DLQ | whole async execution in `QUEUE_ASYNC` mode | execution reaches terminal failure path | `log` | `sqs` |

Use Item Reject Sink for selective recover-and-continue behaviour.
Use execution DLQ for terminal orchestration failures.

## Step-Level Recovery: Item Reject Sink

When `recoverOnFailure=true`, step runtime routes failures to the Item Reject Sink and continues according to step semantics.

- `StepOneToOne` / `StepSideEffect`: failed item is rejected and flow continues with the step's reject output.
- `StepManyToOne`: failed stream is rejected via stream metadata and flow continues with reject output.
- `StepOneToMany` / `StepManyToMany`: expose the same reject API and can use the same sink contract.

### Step API

Legacy step-level `deadLetter(...)` / `deadLetterStream(...)` has been replaced with:

- `rejectItem(...)`
- `rejectStream(...)`

Example flow that emits a reject envelope and still returns step-specific status:

```java
public Uni<PaymentStatus> processPayment(PaymentRecord paymentRecord) {
    return process(paymentRecord)
        .onFailure().recoverWithUni(error ->
            rejectItem(paymentRecord, error)
                .replaceWith(PaymentStatus.rejected()));
}
```

If you override `rejectItem(...)`, delegate to `super.rejectItem(...)` so sink publication still happens.

### Reject Envelope

By default, reject envelopes are metadata-only and include:

1. execution/correlation/idempotency metadata when present,
2. step identity,
3. retry/attempt information,
4. error class/message,
5. timestamp,
6. deterministic fingerprint.

Payload capture is opt-in (`pipeline.item-reject.include-payload=true`).

### Item Reject Sink Configuration

Prefix: `pipeline.item-reject`

| Property | Type | Default | Description |
|---|---|---|---|
| `pipeline.item-reject.provider` | string | `log` | Sink provider selector (`log`, `memory`, `sqs`). |
| `pipeline.item-reject.strict-startup` | boolean | `true` | Fail startup on invalid selected sink provider configuration. |
| `pipeline.item-reject.include-payload` | boolean | `false` | Include rejected payload in envelope. |
| `pipeline.item-reject.memory-capacity` | int | `512` | Ring buffer capacity when `provider=memory`. |
| `pipeline.item-reject.publish-failure-policy` | enum | `CONTINUE` | `CONTINUE` or `FAIL_PIPELINE` when sink publish fails. |
| `pipeline.item-reject.sqs.queue-url` | string | none | Queue URL when `provider=sqs`. |
| `pipeline.item-reject.sqs.region` | string | none | Optional AWS region override for SQS sink. |
| `pipeline.item-reject.sqs.endpoint-override` | string | none | Optional endpoint override (local/dev). |

Production guard:

1. if effective step recovery is enabled (`recoverOnFailure=true`),
2. and the selected sink is non-durable (`log` or `memory`),
3. startup fails in production launch mode.

In non-production, non-durable sinks are allowed with warning logs.

## Execution-Level DLQ (Queue-Async)

Execution DLQ is unchanged and still uses orchestrator SPI:

- `DeadLetterPublisher`
- `DeadLetterEnvelope`

This applies only to terminal execution failures in `pipeline.orchestrator.mode=QUEUE_ASYNC`.

### Execution DLQ Configuration

```properties
pipeline.orchestrator.mode=QUEUE_ASYNC
pipeline.orchestrator.dlq-provider=sqs
pipeline.orchestrator.dlq-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-dlq
```

## Queue-Async Crash Matrix

For `QUEUE_ASYNC`, crash behaviour remains:

| Crash point | Behaviour after restart/recovery | Duplicate risk | Required safeguard |
|---|---|---|---|
| Before transition state commit | Work is redelivered and re-executed from last durable version | High | Idempotent operator boundary (`executionId:stepIndex:attempt`) |
| After state commit, before next enqueue | Transition is durable, but next dispatch can stall until due sweeper re-dispatches | Low | Due-execution sweeper + durable state |
| During retry scheduling | Retry can replay from last durable version if scheduling metadata was not committed | Medium | Persist retry intent (`attempt`, `nextDue`) before enqueue |
| After external side effect, before commit | Side effect may repeat on replay because effect and commit are not one transaction | High | Downstream dedupe keyed by transition identity |
| Worker dies while lease held | Lease expires and another worker can claim execution | Low | Short lease window + conditional lease claim (OCC) |

Semantics summary:

1. committed execution state transitions are exactly-once,
2. operator invocation and dispatch are at-least-once,
3. replay is deterministic for control-plane state, not for non-idempotent external systems.

## Retry and Idempotency

Recommended defaults:

```properties
pipeline.defaults.retry-limit=5
pipeline.defaults.retry-wait-ms=1000
pipeline.defaults.max-backoff=30000
pipeline.defaults.jitter=true
```

Use `NonRetryableException` to fail fast for non-transient failures.

For at-least-once boundaries (queue delivery, operator invocation, re-drive), enforce idempotency using stable transition identity (`executionId:stepIndex:attempt`) in downstream systems.

## Operations Runbook

1. Check whether the failure belongs to Item Reject Sink (step-level selective reject) or execution DLQ (terminal execution).
2. For Item Reject Sink on `sqs`, inspect reject queue depth and message fingerprint patterns.
3. For execution DLQ, triage terminal cause (`FAILED` vs `DLQ`) and replay only after validating downstream idempotency.
4. If due executions stall, verify sweeper activity and dispatcher health.
5. Re-drive in bounded batches, monitor duplicate suppression and retry saturation.
