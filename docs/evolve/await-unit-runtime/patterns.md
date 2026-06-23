# Await Unit Patterns

The await unit runtime is not a new transport. It is a persistence and replay model that keeps execution scheduling, boundary completion, and transport dispatch separate.

## Patterns Used

| Pattern | Where it appears | Why it matters |
| --- | --- | --- |
| Unit of Work | `AwaitUnitRecord` | One authored await boundary has one durable completion contract. |
| Durable continuation | `ExecutionRecord.awaitUnitId` | The execution parks without embedding response payload state in the execution row. |
| Adapter | `AwaitTransportAdapter` | `interaction-api`, `webhook`, and `kafka` differ at dispatch/admission edges, not in core resume semantics. |
| State machine | unit and interaction statuses | Runtime transitions are explicit and terminal states are guarded. |
| Optimistic concurrency | versioned stores and conditional updates | Concurrent dispatch/completion cannot silently overwrite progress. |
| Lease-based recovery | `QUEUE_ASYNC` execution claiming | Worker failure is handled by re-claiming and replaying safe transitions. |
| Idempotent admission | idempotency and correlation lookups | Duplicate completions resolve to the same logical interaction where possible. |

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

The current design fixes that by making the await unit the release gate. A completed item is not enough to continue. The unit must finish dispatch, the parent execution must be durably `WAITING_EXTERNAL`, and the completion must be recorded against its item interaction. Only then does the queue-async coordinator dispatch the item continuation.

Object Publish removes the old final-file aggregate step from the default CSV path. Terminal rows can be written through a connector-owned streaming session, while the parent execution still commits success only after publication completes. This does not make one `Multi` demand signal cross the external provider boundary; it replaces that impossible promise with durable gates that prevent duplicate source reads and premature continuations.
