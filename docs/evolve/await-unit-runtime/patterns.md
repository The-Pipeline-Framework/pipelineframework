# Await Unit Patterns

The await unit runtime is not a new transport. It is a persistence and replay model that keeps execution scheduling, boundary completion, and transport dispatch separate.

## Patterns Used

| Pattern | Where it appears | Why it matters |
| --- | --- | --- |
| Unit of Work | `AwaitUnitRecord` | One authored await boundary has one durable completion contract. |
| Durable continuation | `ExecutionRecord.awaitUnitId` | The execution parks without embedding response payload state in the execution row. |
| Adapter | `AwaitTransportAdapter` | `interaction-api`, `webhook`, and `kafka` differ at dispatch/admission edges, not in core resume semantics. |
| State machine | unit and interaction statuses | State transitions are explicit and terminal states are guarded. |
| Optimistic concurrency | versioned stores and conditional updates | Concurrent dispatch/completion cannot silently overwrite progress. |
| Lease-based recovery | `QUEUE_ASYNC` execution claiming | Worker failure is handled by re-claiming and replaying safe transitions. |
| Idempotent admission | idempotency and correlation lookups | Duplicate completions resolve to the same logical interaction where possible. |

## Semantic Flow Objects

The queue-async refactor names the flows and immutable decisions that were previously buried inside the coordinator.

| Owner | Owns | Does not own |
| --- | --- | --- |
| `QueueAsyncCoordinator` | API entrypoints, worker lifecycle, provider wiring, sweeper startup | completion routing, transition commit decisions |
| `QueueAsyncExecutionFlow` | work-item `Uni` chain: admission, lease claim, transition command, worker execution, commit routing | store semantics, await completion admission, publication mechanics |
| `ItemizedAwaitStream` | live stream-await `Multi`: demand-bounded dispatch, live completion emission, live session lifecycle | durable fallback, child continuation records, success commit |
| `AwaitCompletionFlow` | completion normalization, durable recording, live-session signalling, durable fallback routing | step execution, provider dispatch, terminal publication |
| `ItemContinuationFlow` | item-continuation readiness gates, durable fallback dispatch, retry/failure handling, child continuation records, ordered parent release | live stream demand, non-item await resume, terminal publication |
| `TransitionCommitFlow` | completed/suspended/failed transition commit ordering, result-shape validation, success/wait/failure store commits | await completion admission, live stream demand, terminal publication mechanics |
| `TerminalPublicationFlow` | checkpoint handoff, terminal Object Publish, portable output decoding, publish-before-success effects | success/wait/failure store commits, await completion admission |
| `LiveAwaitSession` | immutable live-session state, subscriber readiness, demand, duplicate completion handling, terminal stream signals | stores, dispatchers, object writers, telemetry policy |

The split is useful only because each owner has a narrow decision model. `AwaitCompletionOutcome`, `TransitionCommitPlan`, and `TerminalPublicationPlan` are immutable records, not service calls. Stores, dispatchers, publishers, and metrics are effects at the boundary; they are not hidden inside the state machine.

The naming matters. A flow object composes `Uni`/`Multi` work and interprets plans. It should not become a procedural bag of helpers. A plan or outcome object is data: it says what should happen, but it does not mutate a store, call a dispatcher, publish an object, or emit telemetry.

## Distributed Semantics

Queue-async is allowed to run on more than one process. The central store is the distributed source of truth:

1. execution records are claimed by lease and versioned writes,
2. await completions are recorded idempotently by interaction/unit facts,
3. stale execution versions lose optimistic-concurrency races,
4. duplicate completions either signal the same live session once or become record-only no-ops,
5. terminal publication still happens before success commit.

`LiveAwaitSession` is not distributed in this slice. It is a process-local fast path for a stream that is still alive. When a completion lands on a different node, or when the stream has already suspended or disappeared, `AwaitCompletionFlow` falls back to durable item continuations guarded by `dispatchComplete` and parent `WAITING_EXTERNAL`.

## Why The Unit Model Fixed The Design

Earlier attempts treated await as special continuation data on the execution record or as a barrier over stream items. That made three separate concerns fight each other:

1. step cardinality,
2. external dispatch/admission strategy,
3. execution suspension and continuation state.

The result was ambiguous behavior: `MANY_TO_MANY` could look like a batch interaction, a stream of unary interactions, or a barrier depending on hidden knobs. The unit model removed those hidden dimensions. Cardinality now defines the interaction unit, and transports only decide how that unit is sent out and completed.

The important simplification is not that await became smaller. It became layered:

1. execution state owns scheduling and waiting,
2. await unit state owns boundary completion,
3. interaction state owns transport-facing records,
4. adapters own protocol-specific dispatch and admission.

Each layer has one job and one persistence shape.

## Why CSV Backpressure Was Hard

CSV Payments exposed a gap between ordinary reactive backpressure and durable distributed coordination. The input parser could be made lazy, but the await step introduces an external chasm: requests leave the process, completions can arrive through a broker before the parent transition is parked, and a worker retry can resubscribe a cold source if suspension is misclassified as failure.

The first durable fix made the await unit the release gate. That prevented duplicate source reads and premature continuations, but it also made the healthy path look like a barrier: the parser could dispatch a large window of items, then status processing waited behind durable release.

The live-session design keeps the durable records but changes the main path. Kafka completions are recorded first, then signalled into a live await session that behaves like a downstream publisher. The source parser advances by reactive demand and the configured in-flight window. If the live session disappears, the durable await unit, parent `WAITING_EXTERNAL` state, and item continuation dispatcher remain the recovery path.

Object Publish removes the old final-file aggregate step from the default CSV path. Terminal rows can be written through a connector-owned streaming session, while the parent execution still commits success only after publication completes. The result is not a single magical `Multi` subscription that survives process loss; it is a live reactive path backed by durable coordination and a deterministic fallback.
