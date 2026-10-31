---
title: One authored consumer per concrete input
status: accepted
---

# ADR-0081: One authored consumer per concrete input

## Context

Type-based routing becomes ambiguous when two authored steps in one Pipeline definition
consume the same concrete canonical input. Step position then chooses an operation or
authority that the type does not express. A named persistence target is also ambiguous
when aspect expansion collapses observers of that type.

The same type can legitimately occur in separate nested definitions. An implementation
shared by generated observers does not make those definition-local occurrences identical.

## Decision

Within each Pipeline definition, each concrete canonical input type has one authored
consumer. Resolve aliases and expand union inputs to their accepted concrete variants
before comparing consumers. Two steps can declare the same union input only when their
accepted variants are disjoint. Check the root and each nested definition independently.

The check belongs to compiler validation of authored declarations, before plugin or
aspect expansion. Its diagnostic identifies the concrete type and both authored step
names. Query, Command, and an operation decorated with Await obey the same rule.
Generated observers and completion modifiers are outside this authored-consumer check.

A later operation requiring different behaviour or authority should receive a meaningful
canonical phase or authority type, or reuse a separate nested definition. Identity
services and wrappers introduced solely to avoid validation are not a migration.

Recurrence remains structural and bounded. An authored recursive pipeline call is not
exempt from this invariant. A replacement recurrence mechanism must re-enter a definition
through a framework-owned continuation boundary, preserving invocation context, limits,
failure, lineage, and observability. It must not disguise a second authored consumer.

Generated observer implementation de-duplication can remain. Its occurrence key includes
the owning definition, so sharing an implementation cannot remove an observer from
another definition's execution order.

## Rationale

An operation's canonical input expresses its phase and authority. A single authored
consumer makes routing and targeted observation explicit without turning source order
into an additional dispatch language. Definition-local scope preserves composition.

## Consequences

- Enforcing this rule is a breaking compiler change, including for previously accepted
  flows that reproduce a consumed type later in the definition.
- Authored self-recursion that repeats a concrete consumer requires a structural
  recurrence migration; granting recursive calls an exception would weaken the decision.
- Before enforcement, the compiler/runtime must support and test an honest PadelPot
  migration, including Await and terminal branches without identity services or type
  wrappers. This decision does not assert that those capabilities already exist.
- [The migration prerequisites](/evolve/authored-consumer-migration) record the current
  evidence and the required implementation order. Exact supported syntax remains with
  the DSL, compiler, runtime, and their tests.
- This governs the contracts-owned semantic model and DSL, standalone compiler and
  runtime, generated execution orders, and canonical authoring documentation. It refines
  [ADR-0002](./0002-compiler-owned-v3-type-model) and the recurrence authoring described in
  [ADR-0004](./0004-nested-composition-and-bounded-recursion).
