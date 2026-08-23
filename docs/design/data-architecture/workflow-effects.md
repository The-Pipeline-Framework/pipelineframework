# This does not create ever-growing workflow objects

Explicit propagation does not mean every value accumulates everything ever observed.

This is not the intended model:

```text
A {x}
 ↓
B {x,y}
 ↓
C {x,y,z}
 ↓
D {x,y,z,w}
 ↓
EverythingEverKnown { ... }
```

TPF encourages small steps and small types precisely because pipelines make decomposition cheap.

State can continually change shape:

```text
A {x}
 ↓
B {x,y}
 ↓
C {y,z}
 ↓
D {z,w}
 ↓
E {w,result}
```

Data can disappear once no reachable downstream computation requires it.

A useful way to think about each pipeline value is:

> **the immutable closure of data required by the computation that remains.**

That is very different from a mutable `WorkflowContext` carrying a bag of properties.

And because authored steps remain small functions, their real dependencies remain small too.

## Duplication can be cheaper than lookup

Carrying data has a cost.

So does retrieving it.

Consider two alternatives.

```text
carry immutable data
    ↓
serialize a few extra fields
    ↓
possibly compress over transport
    ↓
continue immediately
```

versus:

```text
carry ID
    ↓
acquire database connection
    ↓
network round trip
    ↓
query planning/execution
    ↓
deserialize result
    ↓
continue
```

For IDs, names, classifications, hashes, monetary values, small records and modest amounts of text, the bandwidth cost is frequently trivial.

The database round trip is not.

More importantly, carrying the value preserves **exactly what this execution knew**.

Re-querying may return what the world knows *now*.

Those are not necessarily the same thing.

So explicit propagation can simultaneously improve:

**performance, temporal consistency, determinism and architectural clarity.**

## Event-driven systems already discovered this

Kafka and other event-driven architectures make this tradeoff constantly.

An event might evolve through:

```text
OrderPlaced
    ↓
OrderValidated
    ↓
OrderPriced
    ↓
OrderReadyForFulfilment
```

Each event often repeats contextual information that could theoretically be reconstructed elsewhere.

Why?

Because downstream consumers become dramatically easier to operate when they do not need synchronous calls to several other systems before doing anything useful.

Event enrichment trades storage and bandwidth for:

- fewer synchronous dependencies;
- lower temporal coupling;
- replayability;
- observability;
- resilience;
- consistency about what a consumer actually saw.

TPF makes a similar tradeoff.

But it has an important advantage.

## TPF values are not enterprise events

Kafka events frequently become contracts for consumers the producer does not control.

Over time this can produce the notorious overloaded event:

```text
CustomerUpdatedV19 {
    everythingAnyoneHasEverNeeded
}
```

TPF operates in a different environment.

A pipeline is a **closed, compiler-known typed computation**.

```text
A → B → C → D
```

TPF knows its:

```text
types
branches
cardinalities
composition
release identity
```

So values can be shaped for the computation they actually serve.

```text
ValidatedInvoice
        ↓
InvoiceAnalysisRequest
        ↓
InvoiceReview
        ↓
ConfirmedInvoice
        ↓
ArchiveOutcome
```

These are not generic messages thrown onto a bus in the hope that unknown consumers will find them useful.

They are **purpose-built immutable states of a known computation**.

That makes explicit enrichment much less dangerous.

## Types are not merely DTOs

This leads to one of the more important consequences of the TPF model.

A type such as:

```text
InvoiceReview
```

is not merely a DTO passed between two Java methods.

It can simultaneously be:

- the output of a computation;
- the input to another computation;
- immutable pipeline state;
- a persisted historical fact;
- an observable execution value;
- a replayable result;
- something another Query can retrieve later;
- part of the release-pinned application contract.

That gives TPF types considerably more architectural significance than conventional transport objects.

The familiar application landscape of:

```text
DTO
Entity
API model
Event
Command
Persistence model
Read model
Internal model
```

does not necessarily disappear.

But neither does TPF begin by assuming all those representations must exist.

It begins with:

> **What value did the computation produce?**

That value is the primary architectural fact.

## Persistence turns computation into history

If pipeline values are persisted, execution naturally produces a temporal data model.

Consider an invoice workflow:

```text
InvoiceAnalysisRequest
        ↓
InvoiceReview
        ↓
ConfirmedInvoice
        ↓
ArchiveOutcome
        ↓
NotificationOutcome
```

Persist those values and the application acquires history almost automatically.

Suppose:

```text
InvoiceReview
    documentId = 42
    recommendedProperty = A
```

and later:

```text
ConfirmedInvoice
    documentId = 42
    recommendedProperty = A
    confirmedProperty = B
```

The application does not need a mutable workflow registry containing:

```text
recommendationChanged=true
```

The history already says what happened.

Ask the data:

```text
InvoiceReview(documentId = 42)
ConfirmedInvoice(documentId = 42)
```

and compare them.

The audit trail is not something reconstructed from logs after the fact.

**It is the computation's immutable output.**

## This is not event sourcing

There is an important boundary.

TPF does not require an application to reconstruct its entire current state by folding every pipeline value from the beginning of time.

Persisted values can serve as:

```text
business facts
execution history
audit records
replay material
observability data
queryable application state
```

without requiring strict event-sourcing architecture.

Applications remain free to create projections and query models.

The important distinction is that those projections do not need to become hidden execution authority.

The pipeline already has its facts.

## Large data travels by reference

The same architecture extends naturally to files and other large payloads.

Carrying a 50 MB document through every serialized pipeline value would be absurd.

But throwing away its identity and repeatedly locating it in external storage would recreate hidden coupling.

TPF therefore carries:

```text
PayloadReference
```

instead.

```text
PDF
 ↓
PayloadReference
 ↓
typed pipeline state
 ↓
materialize when required
```

The computation retains immutable identity and provenance without duplicating the bytes.

This produces a remarkably small data vocabulary:

```text
small known data
    → carry the value

large known data
    → carry PayloadReference

new external knowledge
    → Query

external side effect
    → Command
```

That is close to a complete data architecture.

## The cost is storage

Immutable history accumulates.

That is not a surprise, nor is it unique to TPF.

Kafka installations, event stores, analytical systems and audit-heavy architectures all eventually distinguish operational data from historical data.

TPF can do the same:

```text
HOT
────────────────
active executions
recent typed values
operational queries
fast replay
current UI

        ↓ retention / archival

COLD
────────────────
historical typed values
audit
forensics
analytics
long-term replay
```

Cold storage is not an architectural embarrassment.

It is the natural lifecycle of immutable history.

And TPF's historical data has unusually strong context available to it:

```text
release identity
contract hash
execution identity
step identity
type identity
timestamp
payload provenance
```

That can make archived pipeline history considerably more useful than unstructured application logs.
