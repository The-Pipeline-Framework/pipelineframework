---
title: One authored consumer per concrete input
status: accepted
refined_by: ADR-0086
---

# ADR-0081: One authored consumer per concrete input

::: warning Partially superseded by ADR-0086
The current rule permits one ordinary consumer and one authored self-call continuation
per concrete canonical input type, independently within each definition. The historical recurrence
restriction recorded below was superseded by
[ADR-0086](./0086-explicit-self-invocation-and-nested-await). Ordinary-consumer uniqueness
and definition-local observer occurrences remain in force.
:::

## Context

Type-based routing becomes ambiguous when two ordinary authored consumers in one Pipeline definition
consume the same concrete canonical input. Step position then chooses an operation or
authority that the type does not express. A named persistence target is also ambiguous
when aspect expansion collapses observers of that type.

The same type can legitimately occur in separate nested definitions. An implementation
shared by generated observers does not make those definition-local occurrences identical.

## Decision

Within each Pipeline definition, each concrete canonical input type has at most one
ordinary authored consumer. Operations and calls to other definitions share that
consumer namespace. As refined by [ADR-0086](./0086-explicit-self-invocation-and-nested-await),
an authored self-call continuation may accept the same concrete type. Self-calls have
a separate namespace with at most one continuation per concrete type.

Resolve aliases and expand union inputs to their accepted concrete variants before
comparing consumers within each namespace. Two ordinary consumers, or two self-call
continuations, can declare the same union input only when their accepted variants
are disjoint. Check the root and each nested definition independently.

The check belongs to compiler validation of authored declarations, before plugin or
aspect expansion. Its diagnostic identifies the concrete type and both authored step
names. Query, Command, and an operation decorated with Await obey the same rule.
Generated observers and completion modifiers are outside this authored-consumer check.

A later operation requiring different behaviour or authority should receive a meaningful
canonical phase or authority type, or reuse a separate nested definition. Identity
services and wrappers introduced solely to avoid validation are not a migration.

Recurrence remains structural and bounded. A call resolving to its enclosing definition,
whether written as `pipeline: self` or by the nested definition's name, is an authored
self-call continuation. It is checked in the continuation namespace and preserves
invocation context, limits, failure, lineage, observability and caller-return semantics.
Both ordinary calls and self-calls must satisfy the invoked definition's boundary
contracts and the compiler's ordered routing and reachability checks.

Generated observer implementation de-duplication can remain. Its occurrence key includes
the owning definition, so sharing an implementation cannot remove an observer from
another definition's execution order.

## Rationale

An operation's canonical input expresses its phase and authority. A single ordinary
consumer makes routing and targeted observation explicit without turning source order
into an additional dispatch language. Definition-local scope preserves composition.

## Consequences

- Enforcing this rule is a breaking compiler change, including for previously accepted
  flows that assign a reproduced type to a second ordinary consumer later in the definition.
- A concrete type may select one ordinary consumer and one authored self-call
  continuation. Multiple self-call continuations must have disjoint accepted types.
- PadelPot's migration uses explicit self-invocation, nested Await and definition-output
  completion without identity review services or nominal phase wrappers.
  [ADR-0086](./0086-explicit-self-invocation-and-nested-await) records the current model.
- [The migration evidence](/evolve/authored-consumer-migration) records the application
  and coordinated compatibility checks. Exact supported syntax remains with
  the DSL, compiler, runtime, and their tests.
- This governs the contracts-owned semantic model and DSL, standalone compiler and
  runtime, generated execution orders, and canonical authoring documentation. It refines
  [ADR-0002](./0002-compiler-owned-v3-type-model) and the recurrence authoring described in
  [ADR-0004](./0004-nested-composition-and-bounded-recursion).

## Historical recurrence restriction

The original decision counted authored recursive calls in the same consumer namespace
as ordinary operations. It required a structural recurrence migration whenever a
self-call repeated an ordinary consumer's concrete type. That restriction led to the
definition-owned continuation described by [ADR-0083](./0083-definition-owned-bounded-recurrence)
and the root-only durable suspension described by
[ADR-0087](./0087-root-recurrence-preserves-durable-await).

ADR-0086 superseded that restriction and those migration mechanisms with explicit
authored self-call continuations and durable nested Await. This section preserves the
earlier rationale as history. The Decision and Consequences above state the current rule.
