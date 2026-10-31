---
title: Definition output completes typed branches
status: accepted
---

# ADR-0082: Definition output completes typed branches

## Context

A branch-aware Pipeline previously required one final `terminal: true` operation
covering every surviving alternative. Applications introduced identity merge services
even when every branch had already produced a valid result. A nested definition could
also finish with a concrete payload while its declared return contract was a union.
Returning that payload through an unchecked Java cast does not construct the union.

## Decision

A version 3 definition with an explicit output contract can complete without an
authored terminal merge. The compiler checks all reachable branch-end types against
that output contract. Unrelated alternatives pass through ordinary steps until the
definition ends; the output declaration does not create an early jump or a new step.

The compiler emits a definition-owned completion descriptor in `branching.json`.
At the end of a complete root or nested execution range, the runtime's existing
branch-routing authority adapts the surviving value to the declared canonical output.
This includes extracting a concrete result from an earlier union and constructing a
declared return union from a concrete branch result. Already-canonical outputs retain
their identity. A value that cannot satisfy the output contract fails explicitly.

Partial ranges do not complete a definition. Completion precedes root terminal
publication, and nested completion does not gain publication ownership. Completion
does not add an authored operation, plugin observer, or replay node.

Existing explicit terminal merges retain their validation and execution semantics.
Implicit union completion requires an unambiguous concrete-payload-to-variant mapping;
authors with multiple output tags for one payload must construct that union explicitly
in a terminal operation.

## Rationale

The definition's output contract owns return shape. Keeping that boundary in compiler
metadata and the existing runtime routing path removes identity business services
without creating a second routing language or leaking raw union payloads to callers.

## Consequences

- The standalone compiler owns reachability and completion descriptors; the runtime
  loader, branching registry, and runner own their execution.
- Missing reachable branch results are build errors. An output union may name
  alternatives that a particular implementation does not produce.
- Legacy metadata without completion descriptors keeps its previous behaviour.
- This supplies one prerequisite for [ADR-0081](./0081-one-authored-consumer-per-concrete-input).
  Structural recurrence and branch-selective deferred completion remain separate work;
  this decision does not relax the authored-consumer invariant.
