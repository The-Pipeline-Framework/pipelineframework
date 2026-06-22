# Queue-Async Runtime

`QUEUE_ASYNC` is the background execution path. The caller submits work and receives an execution id instead of waiting for the whole pipeline result. TPF stores the execution, dispatches work through the configured dispatcher, retries failed transitions, and exposes status/result endpoints for follow-up.

It can use in-process providers for local development or durable providers such as DynamoDB/SQS-backed implementations for production-style recovery. To get real HA behaviour, use durable providers and more than one worker-capable runtime instance. `memory` + `event` is useful for local development, but it does not give crash-surviving distributed recovery.

## Durable Execution

Durable execution means accepted work is recorded outside the current JVM or process before the runtime depends on it. If a worker crashes or the application restarts, another worker can recover the stored execution and run it again after the lease expires.

This is lease-based recovery, not mid-pipeline checkpoint resume. TPF persists execution state, lease ownership, retry timing, and terminal outcomes, but it does not currently persist a resumable "restart from step N" checkpoint inside one pipeline run.

Durability protects TPF execution state and dispatch/retry flow. External systems called by your business code still need idempotency, because a retry or takeover can call the same operator or downstream system again.

## Guarantees

In `QUEUE_ASYNC` mode:

1. committed execution state transitions are exactly-once (OCC/conditional-write guarded),
2. dispatch and operator invocation are at-least-once,
3. duplicate invocation can occur and must be handled with idempotency keys,
4. streaming outputs are rejected for async execution in the current release line,
5. persisted protobuf payload descriptors store `_tpf_message` as the protobuf schema full name.

## Crash Behaviour

Think about `QUEUE_ASYNC` in terms of safe re-execution:

| Scenario | What TPF preserves | What happens next | What your code must tolerate |
|---|---|---|---|
| Crash before async work is accepted | Nothing yet | Caller retries submission | duplicate submissions |
| Crash after acceptance but before completion | Stored execution row and due-work timing | a worker can claim the execution later and run it again | repeated business invocation |
| Worker dies while holding a lease | Stored execution row remains; lease expires | another worker can claim the execution and rerun it | at-least-once step execution |
| Crash after downstream side effect but before commit | downstream side effect may already have happened | TPF may rerun the same work item | idempotent external calls |
| Terminal failure after retries are exhausted | terminal status and failure details | execution moves to DLQ or failure state | replay or operator investigation process |

The important boundary is this: TPF makes the orchestrator state crash-surviving, but your business-side effects must still be safe when the same work is attempted again.

Use [Command Steps](/deploy/orchestrator-runtime/command) when the external side effect itself should be a managed pipeline boundary with a deterministic command id, effect log, duplicate policy, and recorded output replay.

## Re-execution, Re-entrancy, And Idempotency

These three ideas are related but not identical:

1. **Re-execution** means TPF may run the same accepted execution again after retry, redelivery, or lease takeover.
2. **Re-entrancy** means your step or downstream boundary behaves correctly when it is entered again for the same logical work item.
3. **Idempotency** means repeating the same operation does not create a second business effect.

In practice, teams should design `QUEUE_ASYNC` steps around these rules:

1. keep side effects at explicit boundaries, not hidden across several helper calls,
2. give external systems a stable business idempotency key when they support one,
3. make database writes upsert/merge-aware where possible,
4. treat retries and worker takeover as normal behaviour, not exceptional edge cases,
5. assume orchestrator state is exactly-once committed, but step invocation is still at-least-once.

### Example: Payment Capture

Suppose a payment-capture step calls an external provider and the worker crashes after the provider accepts the charge but before TPF commits success.

On recovery, TPF can run that work again. The safe design is:

1. send a provider idempotency key such as `orderId` or `paymentRequestId`,
2. persist the capture result after the provider confirms it,
3. make repeated callback handling return the same logical payment status instead of charging again.

Without that boundary-level idempotency, TPF can still recover the execution, but your payment side effect may be duplicated.

### Example: Queryable Read Model

Suppose a pipeline persists an order summary that the UI reads later.

The safe design is:

1. write by stable business key such as `orderId`,
2. update the existing record if the same execution is replayed,
3. keep expensive derived data in cache so replay can reuse stable upstream work.

That gives you crash recovery plus a queryable result without turning retries into duplicate rows.

## Runtime Components

Runtime provider choices:

1. `ExecutionStateStore`: `memory` (dev), `dynamo` (durable).
2. `WorkDispatcher`: `event` (in-process), `sqs` (durable queue).
3. `DeadLetterPublisher`: `log` (built-in fallback), `sqs` (durable DLQ).

Failure channel split:

1. Execution-level terminal failures use orchestrator DLQ (`DeadLetterPublisher`).
2. Step-level recover-and-continue failures use Item Reject Sink (`pipeline.item-reject.*`, `rejectItem` / `rejectStream`).

## Execution Lifecycle

One transition runs per worker claim:

```text
Submit(run-async)
  -> createOrGetExecution (dedupe key + execution row)
  -> enqueue work item
  -> worker claimLease (OCC + lease expiry)
  -> execute transition
  -> commit transition (markSucceeded / scheduleRetry / markTerminalFailure)
  -> enqueue next transition OR finalize terminal state
```

Recovery points:

1. crash before commit: queue redelivery replays the transition.
2. crash after commit before next enqueue: due sweeper re-dispatches.
3. worker death while leased: lease expiry allows takeover.

These guarantees are deterministic for orchestrator state, not for external side effects; downstream step boundaries must accept at-least-once invocation.

## HA Baseline

Use this as a minimum production baseline for crash-surviving background execution:

```properties
pipeline.orchestrator.mode=QUEUE_ASYNC
pipeline.orchestrator.state-provider=dynamo
pipeline.orchestrator.dispatcher-provider=sqs
pipeline.orchestrator.dlq-provider=sqs
pipeline.orchestrator.queue-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-work
pipeline.orchestrator.dlq-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-dlq
pipeline.orchestrator.idempotency-policy=CLIENT_KEY_REQUIRED
pipeline.orchestrator.strict-startup=true
```

Operational expectations for this baseline:

1. state transitions remain OCC-guarded and lease-claimed,
2. queue delivery and operator invocation remain at-least-once,
3. terminal dead-letter events are stored outside the current process, not process-local log-only.

CI confidence for this baseline:

1. `SYNC` remains the default runtime mode and the fast baseline configuration.
2. `QUEUE_ASYNC` remains opt-in and requires explicit provider configuration for crash recovery.
3. the HA gate exercises the checkout `deliver-order` recovery path against `dynamo` + `sqs` behaviour with `DynamoDB Local` + `ElasticMQ`.
4. this gate covers worker kill takeover, sweeper redispatch, duplicate submit determinism, and DLQ publication that survives process restarts.
