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
    Adapter-->>Coord: complete item 0
    Coord->>UnitStore: recordItemCompleted -> COMPLETED
    Coord-->>ExecStore: resume execution from ordered unit outputs
```

Completion may arrive out of order. Replay preserves input order by reading completed item interactions by `itemIndex`.

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
