---
search: false
---

# Observer and Tap Contract (Diagnostics-First)

Current scope covers observer/tap behavior at **contract, diagnostics, and tests** level only.
It does not ship a full runtime observer/tap execution engine.

## Contract terms

- **Checkpoint observer**: subscribes to stable, persisted checkpoint outputs.
- **Mid-step tap**: subscribes to transient step outputs with weaker durability guarantees.

## Required diagnostics

Current validator behavior fails fast with explicit diagnostics when:

1. a **required** observer/tap policy token is not in the supported policy token set,
2. a requested policy token is missing/blank (treated as unsupported),
3. supported policy token configuration includes missing/blank entries (ignored by normalization).

Recommended diagnostic payload fields:

- pipeline name,
- step name,
- requested observer/tap policy,
- supported policy set,
- requested policy token and support decision.

## Policy behavior in current scope

- **Required observer/tap**: configuration error if unsupported.
- **Optional observer/tap**: explicit warning and skip behavior.
- No implicit fallback that silently changes delivery guarantees.

## Test scope for current implementation

- Contract-level validation and diagnostics tests.
- No merge blocker on full runtime tap fan-out execution in current scope.

## Out of scope for current implementation

- Dedicated runtime delivery semantics for observer/tap streams.
- Backpressure and durability SLA guarantees beyond existing checkpoint pipelines.
- Checkpoint/non-checkpoint attachment validation.
- Unresolved-step resolution diagnostics.
- Expected-vs-actual output shape diagnostics.
