# Immutable Dataflow as Data Architecture

TPF's pipeline model is also its **data architecture**.

A pipeline does not merely describe where computation executes. Its typed, immutable values describe **how application knowledge evolves through time**.

```text
Request
   ↓
ValidatedRequest
   ↓
EnrichedRequest
   ↓
Decision
   ↓
ConfirmedDecision
   ↓
Outcome
```

Each transition produces a new fact. Each fact is explicit. Each dependency is visible.

And when those values are persisted, the pipeline becomes something more than an execution graph:

```text
                  computation
                       ↓
Request → Validated → Enriched → Decision → Outcome
             │           │          │          │
             ▼           ▼          ▼          ▼
          persisted   persisted  persisted  persisted
             fact        fact       fact       fact
```

The computation and the evolution of its data are two views of the same thing.

That observation has consequences for almost every part of TPF.

## Immutability becomes operational architecture

The benefit of immutable dataflow is therefore much broader than functional-programming aesthetics.

It gives the system:

**Data consistency.**

A downstream step receives the value produced by this execution rather than accidentally observing newer mutable state.

**Integrity.**

Historical values do not silently change beneath running or replayed computation.

**Auditability.**

Meaningful intermediate and terminal values describe what happened.

**Observability.**

The system can inspect the actual values moving through meaningful typed boundaries.

**Replayability.**

Previously computed values and captured observations can be reused deliberately.

**Performance.**

Known data can travel with the computation instead of requiring repeated synchronous lookups.

**Testability.**

Small authored steps remain functions over explicit inputs rather than miniature service locators.

These properties reinforce each other.

## The pipeline is the data architecture

This brings the argument back to the beginning.

A TPF pipeline is not merely:

```text
function
→ function
→ function
```

It is:

```text
Knowledge₀
    ↓ computation
Knowledge₁
    ↓ computation
Knowledge₂
    ↓ observation
Knowledge₃
    ↓ human decision
Knowledge₄
    ↓ external effect
Outcome
```

The values describe what the application knows.

The steps describe how that knowledge changes.

Persistence records its history.

Query introduces new observations.

Command changes the outside world.

`PayloadReference` lets large immutable data participate without being copied everywhere.

Cache and capture make computation replayable.

Seen together, these are not separate TPF features.

They are different parts of the same architecture.

> **TPF favors explicit immutable data propagation over implicit mutable-state lookup. Carry what the computation already knows. Carry references to what is too large. Query when the semantics require a new observation. Command when the outside world must change. Persist meaningful values when their history matters.**

A little duplication is not necessarily waste.

In many systems, it is substantially cheaper than another network round trip.

More importantly, it makes data coupling explicit, preserves the integrity of the computation, and leaves behind a typed account of what actually happened.

**The pipeline does not merely process the application's data.**

**The pipeline describes the evolution of the application's knowledge.**





