---
title: Branch routing owns Await projector input
status: accepted
---

# ADR-0015: Branch routing owns Await projector input

## Context

A v3 Await step may consume one alternative of a closed union through `accepts`. The
durable Await request must then be the concrete accepted payload, while an Await that
accepts several or all alternatives must retain the declared union value. Resolving this
at runtime would duplicate compiler routing and make durable request interpretation
depend on ad hoc extraction.

## Decision

The existing v3 branch planner owns Await applicability and the effective request type.
For one explicitly accepted union alternative, the generated Await boundary and its
`AwaitCompletionProjector<I, C, O>` use that concrete payload as `I`. For multiple or all
accepted alternatives, `I` remains the declared union type.

Compiler binding validates the projector's resolved generic arguments, completion type,
output assignability, visibility, and construction before generation. Nonaccepted union
alternatives continue through the ordinary linear branch plan unchanged. Runtime Await
owns durable request storage, completion projection, deduplication, and resumption; it
does not perform a second narrowing operation.

## Rationale

One routing authority keeps root steps and Await steps in the same v3 language. The
generated boundary then carries a precise Java type into the existing durable shell,
while application projection remains a pure typed function.

## Consequences

- V3 Await Java input/output bindings can be inferred from semantic contracts and the
  projector; explicit bindings are compatibility assertions, not conversion requests.
- Projector generic mismatches fail compilation rather than completion admission.
- Pass-through is identity routing for nonaccepted alternatives, not an Await-local
  mapping language.
- Await persistence, interaction identity, recovery, and telemetry ownership remain
  unchanged.
