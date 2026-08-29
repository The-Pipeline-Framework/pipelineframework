---
title: Finite streaming Query reuses ONE_TO_MANY execution
status: accepted
---

# ADR-0019: Finite streaming Query reuses ONE_TO_MANY execution

## Context

A Query may observe one row or a finite ordered sequence of rows. Returning `List<Row>` from the
unary `CompletionStage` contract would materialize the observation, hide row demand and
cancellation, and make provider fetch windows look like pipeline semantics. Creating a separate
Query streaming engine would duplicate the existing `StepOneToMany`, retry, lineage, transport,
and blocking-iterator machinery.

ONE_TO_MANY retry resubscribes the source expansion. Child identity is derived from the logical
upstream item, step, and zero-based output ordinal. A retried expansion must therefore rewind only
its own ordinal sequence, while other concurrent upstream expansions continue independently.

## Decision

`QueryOperation<I,C,O>` remains the unary `CompletionStage<QueryOutcome<O>>` contract and has
`ONE_TO_ONE` metadata. `StreamingQueryOperation<I,C,O>` returns a `QueryStream<O>` containing a JDK
`Flow.Publisher<O>` and a provider-resource termination stage; its metadata is `ONE_TO_MANY`.
`Flow.Publisher` is used because demand and cancellation are part of the provider boundary and it
does not require provider code to depend on Mutiny. It is not selected merely for being JDK-native.

Generated streaming Query clients implement `StepOneToMany` and return a runtime `Multi`, so rows
remain ordinary TPF pipeline items. Provider pagination and fetch windows are private I/O mechanics.
The publisher must be finite, demand-aware, cancellation-aware, and deterministically ordered for
the same logical observation.

On retry, TPF resets the output ordinal on the active `StepExecutionScope` selected by runtime step
and span. That scope represents one logical upstream expansion. The reset is neither static nor
step-wide, so concurrent inputs and parallel ONE_TO_MANY executions retain independent counters.
Stable ordering makes a re-emitted prefix recreate the same child identities; reordered rows cannot
be made replay-safe by resetting an ordinal.

Streaming Query capture stages ordered items for one attempt. Completion atomically commits the
whole observation, including an empty one. Failure or cancellation aborts the staged observation;
a retry re-evaluates and may re-emit its deterministic prefix. Generic Query cache semantics remain
unary and are unavailable to streaming Query.

`SerializedOperation` owns a streaming invocation from provider admission through publisher
termination and provider-resource release. LOCAL, gRPC, and REST use their existing row-streaming
paths. The FUNCTION platform path materializes a finite output under an explicit item bound and fails on
overflow unless truncation was explicitly selected. Finite never means safely materializable.

## Rationale

The operation interface states the semantic cardinality, while generated metadata lets the compiler
validate the selected operation before generating the correct existing step shape. Separating the
row publisher from resource termination lets blocking invocation ownership end after opening a
cursor while serialization, cancellation, and resource ownership continue for the whole stream.

## Consequences

- Retry correctness depends on deterministic total ordering from each streaming Query provider.
- Query capture does not roll back rows already admitted downstream on a failed attempt; ordinary
  item identity, lineage, and downstream replay handle a re-emitted prefix.
- Source checkpoint/resume is not required and remains out of scope.
- Streaming operations cannot be exposed through the unary dynamic-operation snapshot/dispatch path.
- Blocking JPA and Hibernate Reactive `find.many` providers can implement this contract separately
  without forcing either provider into list or page pipeline semantics.
