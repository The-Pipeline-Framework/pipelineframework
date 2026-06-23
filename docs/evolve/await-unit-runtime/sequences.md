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
    Worker->>ExecStore: mark WAITING_EXTERNAL(awaitUnitId)
```

Suspension is normal control flow. It should not be logged as a failed step or routed through recovery as an exception.

## One-To-One Over Stream

`ONE_TO_ONE` over a `Multi` is a stream of unary awaits inside one owning unit. This is the model used by `csv-payments`: each `PaymentRecord` is one input unit and each provider completion is one output unit.

```mermaid
sequenceDiagram
    participant Source as Upstream Multi
    participant Step as AwaitStepSupport
    participant Coord as AwaitCoordinator
    participant Adapter as AwaitTransportAdapter
    participant UnitStore as AwaitUnitStore
    participant ExecStore as ExecutionStateStore
    participant Queue as QueueAsyncCoordinator
    participant Suffix as Item continuation suffix

    Source->>Step: item 0
    Step->>Coord: createOrGetItem(unitId, itemIndex=0)
    Coord->>Adapter: dispatch item 0
    Source->>Step: item 1
    Step->>Coord: createOrGetItem(unitId, itemIndex=1)
    Coord->>Adapter: dispatch item 1
    Step->>UnitStore: markDispatchComplete(expectedItemCount=2)
    Step-->>ExecStore: park execution with awaitUnitId
    Adapter-->>Coord: complete item 1
    Coord->>UnitStore: recordItemCompleted
    Coord->>Queue: ask whether item 1 may continue
    Queue->>UnitStore: require dispatchComplete
    Queue->>ExecStore: require parent WAITING_EXTERNAL(awaitUnitId)
    Queue->>Suffix: continue item 1 when both gates pass
    Adapter-->>Coord: complete item 0
    Coord->>UnitStore: recordItemCompleted -> COMPLETED
    Coord->>Queue: release parent after item continuations complete
```

Completion may arrive out of order. Replay preserves input order by reading completed item interactions by `itemIndex`.

## Await Unit Gatekeeper

The await unit is both a durability gate and a unit-shape gate. It prevents completions from racing ahead of parent suspension, and it defines what must be replayed together.

```mermaid
flowchart TD
    A["Authored await step"] --> B{"Cardinality + input shape"}
    B -->|ONE_TO_ONE scalar| C["One unit<br/>one primary interaction"]
    B -->|ONE_TO_ONE stream| D["One unit<br/>ordered item interactions"]
    B -->|ONE_TO_MANY| E["One unit<br/>one input, materialized output items"]
    B -->|MANY_TO_ONE| F["One unit<br/>materialized input items, one output"]
    B -->|MANY_TO_MANY| G["One unit<br/>materialized input and output items"]
    C --> H["Release when completion recorded<br/>and parent waits on unit"]
    D --> I["Release item continuations only after<br/>dispatchComplete + parent WAITING_EXTERNAL"]
    E --> J["Replay whole output unit"]
    F --> K["Replay one aggregate output"]
    G --> L["Replay whole output unit"]
```

For `ONE_TO_ONE` over a stream, the unit groups item interactions for ordering, dedupe, and release. It is not provider-side batching. For aggregate cardinalities, the unit is the batch because the runtime materializes the relevant side of the boundary.

## CSV Payments Itemized Await

This is the concrete connector-first `csv-payments` shape. `Await Payment Provider` owns the Kafka boundary, but `Process Payment Status` can run per completed item. The parent execution is released after the itemized unit reaches the next aggregate or terminal boundary, then terminal Object Publish writes the output objects before success is committed.

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
    participant Status as Process Payment Status
    participant Publish as Object Publish

    Input-->>Runner: PaymentRecord item 0
    Runner->>Await: execute item 0
    Await->>AwaitCoord: create item interaction(itemIndex=0)
    AwaitCoord->>Kafka: publish request envelope
    Kafka->>Provider: deliver payment request
    Provider-->>Kafka: PaymentStatus item 0 early
    Kafka-->>Exec: complete by correlationId
    Exec->>Queue: complete await interaction
    Queue->>AwaitCoord: record item 0 completed
    Queue->>Queue: hold continuation until parent is WAITING_EXTERNAL

    Input-->>Runner: PaymentRecord item 1
    Runner->>Await: execute item 1
    Await->>AwaitCoord: create item interaction(itemIndex=1)
    AwaitCoord->>Kafka: publish request envelope
    Kafka->>Provider: deliver payment request

    Await->>AwaitCoord: mark dispatchComplete(expectedItemCount=2)
    Await-->>Queue: suspend parent execution(awaitUnitId)
    Queue->>Queue: mark WAITING_EXTERNAL(awaitUnitId)
    Queue->>Queue: release already completed item 0
    Queue->>Status: continue item 0
    Status-->>Queue: PaymentOutput item 0

    Provider-->>Kafka: PaymentStatus item 1
    Kafka-->>Exec: complete by correlationId
    Exec->>Queue: complete await interaction
    Queue->>Status: continue item 1
    Status-->>Queue: PaymentOutput item 1

    Queue->>Queue: all item continuations collected in itemIndex order
    Queue->>Queue: release parent execution at terminal boundary
    Queue->>Publish: publish terminal PaymentOutput objects
    Publish-->>Queue: write sessions closed
    Queue->>Queue: mark execution succeeded
```

The model is itemized until the next aggregate or terminal boundary. If an authored downstream step is `MANY_TO_ONE` or `MANY_TO_MANY`, the parent execution resumes there with the collected ordered item outputs. If the suffix remains itemized through the terminal output, Object Publish owns final grouping and object writes.

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
    Coord->>Store: mark unit COMPLETED with output snapshot
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
      Sweeper->>ExecStore: mark terminal failure / schedule failure handling
    else execution already resumed or terminal
      Sweeper-->>Dispatcher: no-op
    end
```

Completion admission follows the opposite path: complete the interaction, update the unit, and resume the execution only when the unit is complete.
