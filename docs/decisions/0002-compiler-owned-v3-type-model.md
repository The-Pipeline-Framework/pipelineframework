---
title: Compiler-owned v3 type model
status: accepted
---

# ADR-0002: Compiler-owned v3 type model

## Context

If DSL normalization, type compatibility, branching, or wire identity is reconstructed
by individual runtimes, generators, or applications, the same pipeline can acquire
different meanings at different boundaries.

## Decision

V3 is the canonical authored pipeline/type model. The compiler owns normalized type
identity, semantic and wire tags, compatibility, routing, composition checks, hashes,
and generated contracts. Authors declare named records, nominal wrappers, intentional
aliases, discriminated unions, and repeated fields. Unions plus `accepts` express typed
branch applicability; Java `instanceof` routing does not replace compiler-known flow.

Repeated fields are finite ordered duplicate-preserving value shape. They do not imply
streaming or fan-out, and TPF performs no implicit repeated-field/stream conversion.
This decision governs `framework/runtime-core`, `framework/runtime`,
`framework/deployment`, and generated `META-INF/pipeline/` contracts.

## Rationale

One immutable semantic model makes validation and generation deterministic and prevents
transport metadata or Java convenience types from redefining application meaning.

## Consequences

- Type/hash/tag generation must remain deterministic.
- Compatibility paths may adapt v3 but must not narrow or reinterpret it.
- Dynamic maps and reflection are not substitutes for named canonical contracts.
