# Await Unit Sequences

These diagrams show how the await unit model parks and resumes `QUEUE_ASYNC` executions.

`QueueAsyncCoordinator` is still the entrypoint and lifecycle facade. The diagrams below name the internal semantic owners when the distinction matters: `QueueAsyncExecutionFlow` composes claimed work as a `Uni`, `ItemizedAwaitStream` owns the stream-await `Multi`, `AwaitCompletionFlow` routes completions, `LiveAwaitSession` owns live demand and stream terminal signals, `ItemContinuationFlow` owns durable item fallback, `TransitionCommitFlow` commits suspended or completed transitions, and `TerminalPublicationFlow` owns publish-before-success effects.

## Queue-Async Transition Flow

Processing one execution work item is a reactive flow over durable facts. The coordinator admits and delegates; the flow claims the execution, prepares the transition command, invokes the worker, and passes the result to commit. The immutable `TransitionCommitPlan` decides whether the result is success, wait, or failure before effects are interpreted.

```mermaid
sequenceDiagram
    participant Dispatcher as WorkDispatcher
    participant Facade as QueueAsyncCoordinator facade
    participant Flow as QueueAsyncExecutionFlow
    participant ExecStore as ExecutionStateStore
    participant Worker as PipelineTransitionWorker
    participant Commit as TransitionCommitFlow
    participant Terminal as TerminalPublicationFlow

    Dispatcher->>Facade: execution work item
    Facade->>Flow: processExecutionWorkItem(item)
    Flow->>Flow: admit worker + tenant quota
    Flow->>ExecStore: claimLease(executionId, workerId)
    ExecStore-->>Flow: claimed ExecutionRecord(version)
    Flow->>Flow: prepare TransitionCommandEnvelope
    Flow->>Worker: executeTransition(command)
    Worker-->>Flow: TransitionResultEnvelope
    Flow->>Commit: commit(record, result)
    Commit->>Commit: TransitionCommitPlan.from(record, result)
    alt completed
      Commit->>Terminal: publishBeforeSuccess(record, result)
      Terminal-->>Commit: publication complete
      Commit->>ExecStore: markSucceeded(expectedVersion)
    else waiting external
      Commit->>ExecStore: mark WAITING_EXTERNAL(awaitUnitId)
    else failed
      Commit->>ExecStore: retry / fail / DLQ policy
    end
```

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

For brokered await transports, the preferred queue-async path is live. `AwaitStepSupport` delegates stream await work to `ItemizedAwaitStream`, which opens a live await session for the unit. Source dispatch is bounded by the configured in-flight window, and each completion is recorded before it is emitted to the resumed suffix. If that live session is unavailable, the coordinator falls back to durable item continuations.

```mermaid
sequenceDiagram
    participant Source as Upstream Multi
    participant Step as AwaitStepSupport
    participant Stream as ItemizedAwaitStream
    participant Coord as AwaitCoordinator
    participant Adapter as AwaitTransportAdapter
    participant UnitStore as AwaitUnitStore
    participant Queue as QueueAsyncCoordinator facade
    participant Completion as AwaitCompletionFlow
    participant Live as LiveAwaitSession
    participant Items as ItemContinuationFlow
    participant Suffix as Item continuation suffix
    participant Commit as TransitionCommitFlow
    participant ExecStore as ExecutionStateStore

    Source->>Step: item 0
    Step->>Stream: await item 0
    Stream->>Coord: createOrGetItem(unitId, itemIndex=0)
    Coord->>Adapter: dispatch item 0
    Source->>Step: item 1
    Step->>Stream: await item 1
    Stream->>Coord: createOrGetItem(unitId, itemIndex=1)
    Coord->>Adapter: dispatch item 1
    Adapter-->>Coord: complete item 1
    Coord->>Queue: complete await interaction
    Queue->>Completion: route completion
    Completion->>UnitStore: recordItemCompleted
    Completion->>Live: accept item 1 after durable record
    Live->>Suffix: emit item 1 when downstream requests
    Adapter-->>Coord: complete item 0
    Coord->>Queue: complete await interaction
    Queue->>Completion: route completion
    Completion->>UnitStore: recordItemCompleted -> COMPLETED
    Completion->>Live: accept item 0 after durable record
    Live->>Suffix: emit item 0 when downstream requests
    Stream->>UnitStore: markDispatchComplete(expectedItemCount=2)
    alt live session unavailable
      Stream-->>Commit: suspend parent execution(awaitUnitId)
      Commit->>ExecStore: mark WAITING_EXTERNAL(awaitUnitId)
      Completion->>Items: durable item fallback
      Items->>UnitStore: require dispatchComplete
      Items->>ExecStore: require parent WAITING_EXTERNAL(awaitUnitId)
      Items->>Suffix: dispatch durable item continuation
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
    participant Queue as QueueAsyncCoordinator facade
    participant Completion as AwaitCompletionFlow
    participant Live as LiveAwaitSession
    participant Items as ItemContinuationFlow
    participant Status as Process Payment Status
    participant Publish as Object Publish
    participant Commit as TransitionCommitFlow
    participant Terminal as TerminalPublicationFlow
    participant Store as ExecutionStateStore

    Input-->>Runner: PaymentRecord item 0
    Runner->>Await: execute item 0
    Await->>AwaitCoord: create item interaction(itemIndex=0)
    AwaitCoord->>Kafka: publish request envelope
    Kafka->>Provider: deliver payment request
    Provider-->>Kafka: PaymentStatus item 0
    Kafka-->>Exec: complete by correlationId
    Exec->>Queue: complete await interaction
    Queue->>Completion: route completion
    Completion->>AwaitCoord: record item 0 completed
    Completion->>Live: signal item 0 after durable record
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
    Queue->>Completion: route completion
    Completion->>AwaitCoord: record item 1 completed
    Completion->>Live: signal item 1 after durable record
    Live->>Status: emit item 1 when requested
    Status-->>Publish: PaymentOutput item 1 chunk

    alt live session lost or worker restarted
      Await->>AwaitCoord: mark dispatchComplete(expectedItemCount=2)
      Await-->>Commit: suspend parent execution(awaitUnitId)
      Commit->>Store: mark WAITING_EXTERNAL(awaitUnitId)
      Commit->>Items: release already-completed items
      Completion->>Items: dispatch later completions
      Items->>Status: dispatch durable item continuations
    end

    Commit->>Terminal: publishBeforeSuccess(result)
    Terminal->>Publish: complete target sessions
    Publish-->>Terminal: target sessions closed
    Terminal-->>Commit: publication complete
    Commit->>Store: mark execution succeeded
```

The model is itemized until the next aggregate or terminal boundary. If an authored downstream step is `MANY_TO_ONE` or `MANY_TO_MANY`, durable fallback resumes the parent execution there with the collected ordered item outputs. If the suffix remains itemized through the terminal output, Object Publish owns final grouping and object writes.

```mermaid
sequenceDiagram
    participant Queue as QueueAsyncCoordinator facade
    participant Completion as AwaitCompletionFlow
    participant Items as ItemContinuationFlow
    participant AwaitCoord as AwaitCoordinator
    participant ExecStore as ExecutionStateStore
    participant Status as Process Payment Status
    participant Commit as TransitionCommitFlow
    participant Terminal as TerminalPublicationFlow
    participant Publish as Object Publish

    Queue->>Completion: completion already recorded, no live session
    Completion->>Items: durable item fallback
    Items->>ExecStore: require WAITING_EXTERNAL(awaitUnitId)
    Items->>AwaitCoord: require dispatchComplete
    Items->>Status: continue item 1
    Status-->>Commit: PaymentOutput item 1
    Commit->>Terminal: publishBeforeSuccess(result)
    Terminal->>Publish: publish terminal output
    Publish-->>Terminal: publication complete
    Terminal-->>Commit: success barrier passed
    Commit->>ExecStore: mark execution succeeded
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
