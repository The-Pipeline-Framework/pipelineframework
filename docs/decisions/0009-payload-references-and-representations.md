---
title: Payload references and separate representations
status: accepted
---

# ADR-0009: Payload references and separate representations

## Context

Large immutable content and local APIs often use files, bytes, media objects, entities,
or provider DTOs that are unsuitable as canonical durable pipeline state.

## Decision

Large immutable content travels canonically as `PayloadReference`. A canonical pipeline
value, authored Java representation, persistence representation, and wire representation
are separate identities. `Path`, bytes, media, JPA entities, and provider DTOs remain
local or boundary representations.

`java:` binds/asserts an authored execution representation. Type `mappings:` name
consumer-specific representations. Representation providers may generate adaptation;
connector mappers adapt admission/publication; runtime mappings describe placement and
do not convert values. Object Ingest admits references, including grouped references
when several objects form one typed input. Object Publish is a typed outbound boundary.

This decision governs canonical payload types, `framework/plugins` materialization,
object connectors under `framework/connectors`, representation providers, and mapping
resolution in `framework/deployment`.

## Rationale

Canonical reference identity preserves durability and portability while local
representations let authored code use appropriate APIs without polluting the contract.

## Consequences

- Search representation-provider support before adding application materializer glue.
- Grouped ingest is admission composition, not `MANY_TO_ONE` cardinality.
- Materialization is framework infrastructure and must be bounded and resource-safe.
