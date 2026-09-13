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
Remote execution metadata selects an invocation boundary; it does not redefine the step's
types. Remote steps therefore use the same v3 logical contracts and compiler-generated Java
and wire representations as local pipeline execution.

Repeated fields are finite ordered duplicate-preserving value shape. They do not imply
streaming or fan-out, and TPF performs no implicit repeated-field/stream conversion.
This decision governs `framework/runtime-core`, `framework/runtime`,
`framework/deployment`, and generated `META-INF/pipeline/` contracts.

The `pipelineframework-semantic-model` artifact owns stable, JDK-only data
contracts produced by the compiler and consumed by compiler and runtime code.
This shared location does not transfer semantic interpretation: the compiler
remains the owner of normalization, validation, and interpretation, with JSR-269
as the production build host; runtime code consumes the resulting contracts.
Legacy authored syntax is converted by compiler/parser adapters before it enters
the shared model. The semantic model therefore does not depend on legacy authored
message, union, or field representations.

## Rationale

One immutable semantic model makes validation and generation deterministic and prevents
transport metadata or Java convenience types from redefining application meaning.

## Consequences

- Type/hash/tag generation must remain deterministic.
- Compatibility paths may adapt v3 but must not narrow or reinterpret it.
- Dynamic maps and reflection are not substitutes for named canonical contracts.
