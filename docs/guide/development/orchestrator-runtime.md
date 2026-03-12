# Orchestrator Runtime

The orchestrator runtime coordinates step execution for a pipeline and exposes generated transport entrypoints.

## Runtime Modes

TPF supports two orchestrator runtime modes:

1. `SYNC` (default): in-process request/response execution.
2. `QUEUE_ASYNC`: queue-driven async job execution with durable execution state providers.

Set mode with:

```properties
pipeline.orchestrator.mode=SYNC
# or
pipeline.orchestrator.mode=QUEUE_ASYNC
```

## Transport Surfaces

Generated orchestrator endpoints are transport-native:

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

## Framework Connectors

Framework connectors provide the live handoff boundary between one pipeline output and another pipeline ingest target.

Current v1 scope:

1. source: live `PipelineOutputBus` stream,
2. target: live ingest adapter bean,
3. policies: idempotency, backpressure, failure handling, and metadata propagation,
4. ownership: downstream execution retry/DLQ remains orchestrator-owned after ingest acceptance.

Declare connectors in `pipeline.yaml`:

```yaml
connectors:
  - name: "orders-to-delivery"
    transport: "GRPC"
    idempotency: "PRE_FORWARD"
    backpressure: "BUFFER"
    failureMode: "PROPAGATE"
    source:
      kind: "OUTPUT_BUS"
      step: "Order Ready"
      type: "com.example.connector.ReadyOrderMessage"
    target:
      kind: "LIVE_INGEST"
      pipeline: "deliver-order"
      type: "com.example.connector.DispatchReadyOrderMessage"
      adapter: "com.example.connector.DispatchConnectorTarget"
    mapper: "com.example.connector.ReadyOrderConnectorMapper"
    idempotencyKeyFields: ["orderId", "customerId", "readyAt"]
```

Runtime behavior:

1. build-time validation checks source/target classes and mapper and target-adapter signatures,
2. generated bootstrap starts the connector on `StartupEvent` and cancels it on shutdown,
3. `TransportDispatchMetadata` fields are preserved when present; otherwise a deterministic handoff idempotency key is derived from the configured key fields,
4. manual application `Bridge` beans remain supported as compatibility mode.

Bridge beans are manually registered application components that keep an existing handoff implementation in place when you are not yet using the generated connector startup wiring from `pipeline.yaml`.

Current non-goals:

1. generic broker-message re-drive,
2. connector-owned durable state or connector DLQ,
3. framework-generated broker connector bootstrap.

## Queue-Async Semantics

In `QUEUE_ASYNC` mode:

1. committed execution state transitions are exactly-once (OCC/conditional-write guarded),
2. dispatch and operator invocation are at-least-once,
3. duplicate invocation can occur and must be handled with idempotency keys,
4. streaming outputs are rejected for async execution in the current 26.2.x release line,
5. persisted protobuf payload metadata stores `_tpf_message` as the protobuf schema full name.

## Queue-Async Control Plane

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

Use this as a minimum production baseline for queue-driven HA:

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
3. terminal dead-letter events are durable, not process-local log-only.

CI confidence for this baseline:

1. `SYNC` remains the default runtime mode and the fast baseline configuration.
2. `QUEUE_ASYNC` remains opt-in and requires explicit durable provider configuration.
3. the durable HA gate exercises the checkout `deliver-order` recovery path against `dynamo` + `sqs` semantics with `DynamoDB Local` + `ElasticMQ`.
4. this gate covers:
   - worker kill takeover,
   - sweeper redispatch,
   - duplicate submit determinism,
   - durable DLQ publication.

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
