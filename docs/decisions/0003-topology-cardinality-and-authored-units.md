---
title: Topology, cardinality, and authored units
status: accepted
---

# ADR-0003: Topology, cardinality, and authored units

## Context

Java collection signatures can conceal fan-out, aggregation, streaming, and blocking
behaviour that the pipeline compiler and runtime need to understand.

## Decision

Authors choose execution topology before Java shape. `ONE_TO_ONE`, `ONE_TO_MANY`,
`MANY_TO_ONE`, and `MANY_TO_MANY` describe execution cardinality independently of
value shape. Fan-out, aggregation, and streaming remain visible in pipeline topology.

An ordinary service is the default application transformation. An operator is used for
a deliberately reusable or delegated unit with an independently meaningful contract or
execution boundary—not for a helper method or one application's policy. Reactive shapes
serve asynchronous/backpressured work; synchronous libraries use explicit blocking and
framework offload. Query, Command, and Await remain semantic boundaries, not operators.

This decision governs `framework/api`, `framework/deployment`, `framework/runtime`,
`ai-sdk`, and `examples`.

## Rationale

Explicit cardinality lets generated adapters preserve backpressure, lineage, failure,
and transport behaviour instead of guessing from local Java containers.

## Consequences

- YAML cardinality and authored signatures must agree at compilation.
- List-based blocking shapes materialize; incremental shapes must be chosen explicitly.
- TPF does not create a new operator abstraction for every reusable-looking method.
