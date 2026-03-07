# Observer and Tap Contract (Diagnostics-First)

This cycle closes observer/tap scope at **contract, diagnostics, and tests** level only.
It does not ship a full runtime observer/tap execution engine.

## Contract terms

- **Checkpoint observer**: subscribes to stable, persisted checkpoint outputs.
- **Mid-step tap**: subscribes to transient step outputs with weaker durability guarantees.

## Required diagnostics

Any observer/tap request must fail fast with explicit diagnostics when:

1. a checkpoint observer is attached to a non-checkpoint output,
2. a required tap is configured for a shape not supported by the current runtime,
3. an observer/tap contract refers to an unresolved step/output contract.

Recommended diagnostic payload fields:

- pipeline name,
- step name,
- requested observer/tap policy,
- supported policy set,
- expected vs actual output shape.

## Policy behavior in this cycle

- **Required observer/tap**: configuration error if unsupported.
- **Optional observer/tap**: explicit warning and skip behavior.
- No implicit fallback that silently changes delivery guarantees.

## Test scope for this cycle

- Contract-level validation and diagnostics tests.
- No merge blocker on full runtime tap fan-out execution in this cycle.

## Out of scope in this cycle

- Dedicated runtime delivery semantics for observer/tap streams.
- Backpressure and durability SLA guarantees beyond existing checkpoint pipelines.
