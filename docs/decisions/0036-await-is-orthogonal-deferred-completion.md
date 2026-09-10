---
title: Await is orthogonal deferred completion
status: accepted
---

# ADR-0036: Await is orthogonal deferred completion

## Context

Await was introduced as a standalone step kind even though it owned no semantic
operation. An Await step could not invoke an authored service, Query, Command,
pipeline, or remote operator; it only persisted a request, dispatched it through an
Await transport, admitted a correlated completion, projected the final value, and
resumed the execution.

This made completion timing look like a peer of computation and external effects.
It also split one business operation into a preparation step and a synthetic Await
step, obscured the operation's real input and final output, and created a standalone
compiler and replay node for lifecycle behaviour that is orthogonal to operation
kind.

## Decision

`await:` is a deferred-completion modifier on an ordinary authored operation. It is
not a `StepKind`.

The semantic operation retains the pipeline-visible `input`, final `output`,
cardinality, branching, and identity. `await.operationOutput` declares the trusted
immediate output of the authored operation. `await.completion.type` declares the
untrusted completion observation, and an optional deterministic projector combines
the operation output, completion payload, and framework metadata into the final
pipeline output.

The compiler generates the ordinary operation adapter followed by a directly
generated completion modifier and descriptor. Operation-scoped side-effect aspects
remain attached to the immediate operation boundary, so the technical order is
`operation → operation aspects → completion modifier`. The modifier consumes the
trusted immediate result and never invokes the operation itself. The durable Await coordinator, stores,
transport adapters, completion admission, signed tokens, timeout handling,
`WAITING_EXTERNAL`, duplicate handling, lifecycle events, and continuation machinery
remain the authority for deferred completion. Replay attaches that lifecycle to the
semantic operation as an overlay rather than inventing an Await operation node.

The first supported operation is an authored internal service. Every result emitted
by that operation receives exactly one deferred completion. Aggregate Await
cardinalities are removed; collection-level interactions use explicit canonical
collection types and ordinary pipeline expansion or reduction.

Commands require a different crash-safe initiation order: completion must be
registered before the consequential effect is dispatched. Command decoration is
therefore a subsequent decision and implementation slice. Query, nested pipeline,
remote operator, dynamic operation, and packaged Block support remain proof-driven
future work.

`kind: await` is rejected. There is no compatibility identity operation and no new
`interaction` kind.

## Rationale

Operation kind answers what a step does. Deferred completion answers when its final
result becomes available. Keeping those axes separate preserves the functional-core
model while reusing the proven durable Await lifecycle.

This also places application semantics in the authored operation and canonical
types, while correlation, transport, admission, recovery, and replay stay in the
framework-owned imperative shell.

## Consequences

- A pipeline contains one semantic operation where the old model required a
  preparation step followed by an Await step.
- The operation's immediate and final output contracts are distinct, typed, and
  recorded in schema-3 release metadata.
- Resumption advances after the decorated operation and must never invoke that
  operation again.
- Generated operation-scoped aspects observe the immediate operation result before
  completion is deferred; aspects do not move behind the final completion boundary.
- Streaming authored operations keep their cardinality and derive one stable
  interaction occurrence per emitted result.
- Existing releases waiting under the former topology must complete or be cancelled
  before that deployment is retired; they are not silently remapped.
- ADR-0007 remains authoritative for durable suspension. This decision refines its
  authoring and compiler model and supersedes ADR-0015's standalone Await-step shape.
