# Await Unit Sequences

These diagrams show how the await unit model parks and resumes `QUEUE_ASYNC` executions.

## Unary Await

```mermaid
sequenceDiagram
    participant Worker as QueueAsync worker
    participant Step as Generated await step
    participant Coord as AwaitCoordinator
    participant UnitStore as AwaitUnitStore
    participant InteractionStore as AwaitInteractionStore
    participant Adapter as AwaitTransportAdapter
    participant ExecStore as ExecutionStateStore

    Worker->>Step: execute await step
    Step->>Coord: createOrGet(descriptor, input)
    Coord->>UnitStore: createOrGet unit
    Coord->>InteractionStore: createOrGet interaction
    Coord->>UnitStore: attachPrimaryInteraction
    Coord->>Adapter: dispatch request
    Adapter-->>Coord: dispatch metadata
    Coord-->>Step: interaction dispatched
    Step-->>Worker: AwaitSuspendedException(unitId)
    Worker->>ExecStore: persist WAITING_EXTERNAL(awaitUnitId)
```

Suspension is normal control flow. It should not be logged as a failed step or routed through recovery as an exception.

## One-To-One Over Stream

`ONE_TO_ONE` over a `Multi` is a stream of unary awaits inside one owning unit. This is the model used by `csv-payments`: each `PaymentRecord` is one input unit and each provider completion is one output unit.

For brokered await transports, the preferred queue-async path is live. `AwaitStepSupport` opens a live await session for the unit, source dispatch is bounded by the configured in-flight window, and each completion is recorded before it is emitted to the resumed suffix. If that live session is unavailable, the coordinator falls back to durable item continuations.

```mermaid
sequenceDiagram
    participant Source as Upstream Multi
    participant Step as AwaitStepSupport
    participant Coord as AwaitCoordinator
    participant Adapter as AwaitTransportAdapter
    participant UnitStore as AwaitUnitStore
    participant Queue as QueueAsyncCoordinator
    participant Live as LiveAwaitSession
    participant Suffix as Item continuation suffix
    participant ExecStore as ExecutionStateStore

    Source->>Step: item 0
    Step->>Coord: createOrGetItem(unitId, itemIndex=0)
    Coord->>Adapter: dispatch item 0
    Source->>Step: item 1
    Step->>Coord: createOrGetItem(unitId, itemIndex=1)
    Coord->>Adapter: dispatch item 1
    Adapter-->>Coord: complete item 1
    Coord->>UnitStore: recordItemCompleted
    Coord->>Queue: signal completion
    Queue->>Live: accept item 1 after durable record
    Live->>Suffix: emit item 1 when downstream requests
    Adapter-->>Coord: complete item 0
    Coord->>UnitStore: recordItemCompleted -> COMPLETED
    Coord->>Queue: signal completion
    Queue->>Live: accept item 0 after durable record
    Live->>Suffix: emit item 0 when downstream requests
    Step->>UnitStore: markDispatchComplete(expectedItemCount=2)
    alt live session unavailable
      Step-->>ExecStore: park execution with awaitUnitId
      Queue->>UnitStore: require dispatchComplete
      Queue->>ExecStore: require parent WAITING_EXTERNAL(awaitUnitId)
      Queue->>Suffix: dispatch durable item continuation
    end
```

Completion may arrive out of order. The live path can process accepted completions as they arrive; durable replay and aggregate release preserve item identity by reading completed item interactions by `itemIndex`.

## Await Unit Gatekeeper

The await unit is the durable shape for the boundary. In the live path, it is the identity, ordering, and dedupe anchor for item interactions. In the fallback path, it also gates release so completions cannot race ahead of durable parent suspension. For aggregate cardinalities, it defines what must be replayed together.

```mermaid
flowchart TD
    A["Authored await step"] --> B{"Cardinality + input shape"}
    B -->|ONE_TO_ONE scalar| C["One unit<br/>one primary interaction"]
    B -->|ONE_TO_ONE stream| D["One unit<br/>ordered item interactions"]
    B -->|ONE_TO_MANY| E["One unit<br/>one input, materialized output items"]
    B -->|MANY_TO_ONE| F["One unit<br/>materialized input items, one output"]
    B -->|MANY_TO_MANY| G["One unit<br/>materialized input and output items"]
    C --> H["Scalar resume from completion<br/>or parent wait fallback"]
    D --> I["Live session emits by demand<br/>fallback requires dispatchComplete + parent WAITING_EXTERNAL"]
    E --> J["Replay whole output unit"]
    F --> K["Replay one aggregate output"]
    G --> L["Replay whole output unit"]
```

For `ONE_TO_ONE` over a stream, the unit groups item interactions for ordering, dedupe, live-session identity, and fallback release. It is not provider-side batching. For aggregate cardinalities, the unit is the batch because the runtime materializes the relevant side of the boundary.

## CSV Payments Itemized Await

This is the concrete connector-first `csv-payments` shape. `Await Payment Provider` owns the Kafka boundary, but `Process Payment Status` can run per completed item through the live await session. Terminal Object Publish writes output chunks before success is committed.

```mermaid
sequenceDiagram
    participant Input as Process Csv Payments Input
    participant Runner as PipelineRunner
    participant Await as Await Payment Provider
    participant AwaitCoord as AwaitCoordinator
    participant Kafka as Kafka broker
    participant Provider as payments-processing-svc
    participant Exec as PipelineExecutionService
    participant Queue as QueueAsyncCoordinator
    participant Live as LiveAwaitSession
    participant Status as Process Payment Status
    participant Publish as Object Publish

    Input-->>Runner: PaymentRecord item 0
    Runner->>Await: execute item 0
    Await->>AwaitCoord: create item interaction(itemIndex=0)
    AwaitCoord->>Kafka: publish request envelope
    Kafka->>Provider: deliver payment request
    Provider-->>Kafka: PaymentStatus item 0
    Kafka-->>Exec: complete by correlationId
    Exec->>Queue: complete await interaction
    Queue->>AwaitCoord: record item 0 completed
    Queue->>Live: signal item 0 after durable record
    Live->>Status: emit item 0 when requested
    Status-->>Publish: PaymentOutput item 0 chunk

    Input-->>Runner: PaymentRecord item 1
    Runner->>Await: execute item 1
    Await->>AwaitCoord: create item interaction(itemIndex=1)
    AwaitCoord->>Kafka: publish request envelope
    Kafka->>Provider: deliver payment request

    Provider-->>Kafka: PaymentStatus item 1
    Kafka-->>Exec: complete by correlationId
    Exec->>Queue: complete await interaction
    Queue->>AwaitCoord: record item 1 completed
    Queue->>Live: signal item 1 after durable record
    Live->>Status: emit item 1 when requested
    Status-->>Publish: PaymentOutput item 1 chunk

    alt live session lost or worker restarted
      Await->>AwaitCoord: mark dispatchComplete(expectedItemCount=2)
      Await-->>Queue: suspend parent execution(awaitUnitId)
      Queue->>Queue: persist WAITING_EXTERNAL(awaitUnitId)
      Queue->>Status: dispatch durable item continuations
    end

    Publish-->>Queue: target sessions closed
    Queue->>Queue: commit execution success
```

The model is itemized until the next aggregate or terminal boundary. If an authored downstream step is `MANY_TO_ONE` or `MANY_TO_MANY`, durable fallback resumes the parent execution there with the collected ordered item outputs. If the suffix remains itemized through the terminal output, Object Publish owns final grouping and object writes.

```mermaid
sequenceDiagram
    participant Queue as QueueAsyncCoordinator
    participant AwaitCoord as AwaitCoordinator
    participant ExecStore as ExecutionStateStore
    participant Status as Process Payment Status
    participant Publish as Object Publish

    Queue->>AwaitCoord: completion already recorded, no live session
    Queue->>ExecStore: require WAITING_EXTERNAL(awaitUnitId)
    Queue->>AwaitCoord: require dispatchComplete
    Queue->>Status: continue item 1
    Status-->>Queue: PaymentOutput item 1
    Queue->>Publish: publish terminal output before success
    Queue->>Queue: commit execution success
```

## Aggregate Unit

`ONE_TO_MANY`, `MANY_TO_ONE`, and `MANY_TO_MANY` are aggregate interaction units. The runtime materializes the relevant side of the boundary so replay has one stable unit to restart.

```mermaid
sequenceDiagram
    participant Pipeline as Pipeline stream
    participant Step as Aggregate await step
    participant Coord as AwaitCoordinator
    participant Store as Await stores
    participant Adapter as Transport adapter
    participant Resume as Resumed suffix

    Pipeline->>Step: input item(s)
    Step->>Step: materialize aggregate input when required
    Step->>Coord: createOrGet aggregate unit and primary interaction
    Coord->>Store: persist unit + interaction snapshots
    Coord->>Adapter: dispatch aggregate request
    Step-->>Store: execution waits on awaitUnitId
    Adapter-->>Coord: complete primary interaction
    Coord->>Store: record unit COMPLETED with output snapshot
    Store-->>Resume: load continuation input
    Resume->>Resume: replay full output unit
```

This deliberately avoids partial-output checkpointing inside the interaction unit. TPF owns retry/replay of the unit as a whole.

## Timeout And Resume

```mermaid
sequenceDiagram
    participant Sweeper as QueueAsync sweeper
    participant Coord as AwaitCoordinator
    participant InteractionStore as AwaitInteractionStore
    participant UnitStore as AwaitUnitStore
    participant ExecStore as ExecutionStateStore
    participant Dispatcher as WorkDispatcher

    Sweeper->>Coord: findTimedOut(now)
    Coord->>InteractionStore: markTimedOut(interaction)
    Coord->>UnitStore: markTerminal(TIMED_OUT)
    Coord-->>Sweeper: terminal await record
    Sweeper->>ExecStore: load waiting execution
    alt execution still waits on same awaitUnitId
      Sweeper->>ExecStore: commit terminal failure / schedule failure handling
    else execution already resumed or terminal
      Sweeper-->>Dispatcher: no-op
    end
```

Completion admission follows the opposite path: complete the interaction, update the unit, and resume the execution only when the unit is complete.
