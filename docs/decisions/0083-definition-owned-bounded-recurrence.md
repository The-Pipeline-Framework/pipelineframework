---
title: Definition-owned bounded recurrence
status: accepted
---

# ADR-0083: Definition-owned bounded recurrence

## Context

An explicit recursive call often consumed the same concrete type as the first
operation in its definition. Exempting that call from the authored-consumer rule
would preserve two authored consumers, while adding a nominal wrapper solely for
the call would hide the routing ambiguity.

## Decision

A local version 3 definition can select continuing concrete outcomes through
`recur: [Type]`. This is definition lifecycle data, independent of its authored
steps. Selected outcomes must be assignable to the definition input and must not
also be terminal output alternatives.

After authored declarations have been validated, the compiler lowers this selection
to one reserved `$recur` invocation at the definition exit. The existing composition
linker, local invocation renderer, recursive adapter, invocation context, and depth
limit execute it. There is no second loop executor, application registry, or new
runtime step kind. Compiled composition and branching metadata retain the generated
invocation and its continuing types.

Non-continuing outcomes complete against the explicit definition output, as described
in [ADR-0082](./0082-definition-output-completes-typed-branches). The authored definition
retains one consumer of each concrete canonical input. An explicitly authored recursive
call remains subject to [ADR-0081](./0081-one-authored-consumer-per-concrete-input).

The initial support is local, unary composition. It cannot be combined with an
authored self-call or terminal merge in the same definition. Root recurrence, nested
Await, mutual recursion, and streaming recurrence are not introduced by this decision.

## Rationale

Recurrence answers whether a completed invocation continues. Giving that decision to
the definition removes an artificial authored consumer while preserving the proven
bounded invocation path, deterministic recursive frames, context restoration, failure,
and lineage behaviour.

## Consequences

- The contracts repository owns the immutable declaration and DSL grammar; the compiler
  owns its lowering and generated invocation metadata. The runtime reuses its existing
  bounded recursion implementation.
- `pipeline.max-recursive-depth` bounds generated continuations exactly as it bounds
  explicit self-calls. No arbitrary fixed number of turns replaces the application loop.
- Continuing-type reachability and terminal-output coverage are checked by the ordinary
  branch planner after lowering.
- Compatibility fixtures must exercise the authored declaration and lowered graph,
  rather than constructing an equivalent explicit self-call by hand.
