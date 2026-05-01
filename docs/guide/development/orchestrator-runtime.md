# Orchestrator Runtime

The orchestrator runtime is the generated part of a TPF application that starts the pipeline, calls each step in order, tracks execution state when configured, and exposes generated REST, gRPC, or function-style entry points.

## What durable execution means

Durable execution means accepted work is recorded outside the current JVM or process before the runtime depends on it. If a worker crashes or the application restarts, another worker can recover the recorded state and continue from the last committed transition.

Durability protects TPF execution state and dispatch/retry flow. External systems called by your business code still need idempotency, because a retry can call the same operator or downstream system again.

## What background async execution means

Background async execution means the caller submits work and receives an execution id instead of waiting for the whole pipeline result. TPF stores the execution, dispatches work through the configured dispatcher, retries failed transitions, and exposes status/result endpoints for follow-up.

`QUEUE_ASYNC` is the config value for this execution path. It can use in-process providers for local development or durable providers such as DynamoDB/SQS-backed implementations for production-style recovery.

## Runtime Modes

TPF supports two orchestrator runtime modes:

1. `SYNC` (default): in-process request/response execution.
2. `QUEUE_ASYNC`: background async execution with stored execution state, dispatch, retry, recovery, and dead-letter handling.

Set mode with:

```properties
pipeline.orchestrator.mode=SYNC
# or
pipeline.orchestrator.mode=QUEUE_ASYNC
```

## Generated Transport Entry Points

Generated orchestrator entry points use the selected transport:

1. REST:
   - `POST /pipeline/run`
   - `POST /pipeline/run-async`
   - `GET /pipeline/executions/{executionId}`
   - `GET /pipeline/executions/{executionId}/result`
   - `POST /pipeline/ingest`
   - `GET /pipeline/subscribe`
2. gRPC:
   - `Run`
   - `RunAsync`
   - `GetExecutionStatus`
   - `GetExecutionResult`
   - `Ingest`
   - `Subscribe`
3. Function/Lambda:
   - `PipelineRunFunctionHandler`
   - `PipelineRunAsyncFunctionHandler`
   - `PipelineExecutionStatusFunctionHandler`
   - `PipelineExecutionResultFunctionHandler`

## Checkpoint Publication

Reliable cross-pipeline handoff is orchestrator-owned and checkpoint-based.

Supported in this release:

1. source: final pipeline checkpoint publication from a `QUEUE_ASYNC` orchestrator,
2. target: downstream orchestrator background admission bound by a named logical publication,
3. idempotency: preserve incoming dispatch identifiers when present, otherwise derive a repeat-safe handoff key from configured checkpoint fields,
4. ownership: downstream retry/DLQ remains orchestrator-owned after background admission.

Declare reliable handoff in `pipeline.yaml`:

```yaml
input:
  subscription:
    publication: "checkout.orders.ready.v1"
    mapper: "com.example.pipeline.mapper.ReadyOrderMapper"

output:
  checkpoint:
    publication: "checkout.orders.dispatched.v1"
    idempotencyKeyFields: ["orderId", "customerId", "readyAt"]
```

Runtime behaviour:

1. build-time validation checks checkpoint boundary declarations and mapper compatibility,
2. runtime endpoint bindings come from `pipeline.handoff.bindings.<publication>.targets.*`,
3. publication is generated into existing orchestrator ownership; no separate connector runtime or deployment role is introduced,
4. subscriber admission is handled by framework-owned HTTP and gRPC checkpoint publication endpoints instead of runtime subscription discovery,
5. protobuf-over-HTTP and gRPC use the same framework-owned checkpoint protobuf envelope for transport-native admission,
6. reliable handoff is supported only for `QUEUE_ASYNC` orchestrators and is rejected for `FUNCTION` pipelines,
7. live `Subscribe` remains a weaker observer/tap API and is not the reliable checkpoint handoff path.

Not yet supported:

1. generic broker-message re-drive,
2. a separate durable checkpoint-publication service or publication-specific DLQ,
3. dynamic runtime publication discovery or runtime subscription registration,
4. broker-backed publication targets such as `SQS` or `KAFKA`.

## Queue-Async Guarantees

In `QUEUE_ASYNC` mode:

1. committed execution state transitions are exactly-once (OCC/conditional-write guarded),
2. dispatch and operator invocation are at-least-once,
3. duplicate invocation can occur and must be handled with idempotency keys,
4. streaming outputs are rejected for async execution in the current 26.4.x release line,
5. persisted protobuf payload descriptors store `_tpf_message` as the protobuf schema full name.

## Queue-Async Runtime Components

Runtime provider choices:

1. `ExecutionStateStore`: `memory` (dev), `dynamo` (durable).
2. `WorkDispatcher`: `event` (in-process), `sqs` (durable queue).
3. `DeadLetterPublisher`: `log` (built-in fallback), `sqs` (durable DLQ).

Failure channel split:

1. Execution-level terminal failures use orchestrator DLQ (`DeadLetterPublisher`).
2. Step-level recover-and-continue failures use Item Reject Sink (`pipeline.item-reject.*`, `rejectItem` / `rejectStream`).

Execution lifecycle (one transition per worker claim):

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

## Queue-Async HA Baseline

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
4. this gate covers:
   - worker kill takeover,
   - sweeper redispatch,
   - duplicate submit determinism,
   - DLQ publication that survives process restarts.

## Generated Structure

```text
orchestrator-svc/
├── src/main/java/<base>/orchestrator/service/
│   ├── PipelineRunResource.java
│   ├── OrchestratorGrpcService.java
│   ├── PipelineRunFunctionHandler.java
│   ├── PipelineRunAsyncFunctionHandler.java
│   ├── PipelineExecutionStatusFunctionHandler.java
│   └── PipelineExecutionResultFunctionHandler.java
└── src/main/resources/application.properties
```
