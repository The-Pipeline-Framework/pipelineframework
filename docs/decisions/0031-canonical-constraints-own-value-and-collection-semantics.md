---
title: Canonical constraints own value and collection semantics
status: accepted
---

# ADR-0031: Canonical constraints own value and collection semantics

## Context

[ADR-0002](./0002-compiler-owned-v3-type-model.md) makes version 3 the normalized semantic type
model. External schema sources such as MCP can describe closed scalar sets and array cardinality,
but treating those rules as importer-only metadata would make generated Java, runtime validation,
compatibility, and release identity disagree.

## Decision

Canonical version 3 owns target-neutral `allowedValues` constraints on scalar wrappers and
`minItems`/`maxItems` constraints on repeated fields. They participate in normalized metadata,
deterministic hashes, compatibility classification, generated Java constructor validation, canonical
JSON validation, model-facing JSON Schema, and Connector provider manifests.

Allowed values retain their JSON scalar kind. Equivalent sets are canonicalized by value, deduplicated,
and deterministically ordered. Every allowed value must be valid for the wrapped scalar and satisfy its
other constraints. Repeated-field bounds are inclusive, non-negative, and independent of the field's
presence semantics.

External importers project supported source vocabulary into these constraints. In particular, MCP JSON
Schema `enum` becomes `allowedValues`, `const` becomes a singleton set, and array bounds become repeated-
field bounds. The importer may reject source-schema shapes it cannot project losslessly, but it does not
own or weaken canonical semantics.

## Rationale

One constraint model keeps native and imported contracts indistinguishable after normalization. It also
lets inexpensive deterministic validation handle ordinary schema differences before any LLM-based
remediation is considered.

## Consequences

- Constraint changes alter canonical contract identity even when protobuf wire encoding is unchanged.
- Tightening accepted values is narrowing; loosening them is widening; mixed changes are incomparable.
- Provider manifests carrying these constraints use schema version 6 while older manifests remain readable.
- `payload_ref` does not support `allowedValues` because its canonical JSON representation is not scalar.
- MCP remains an operation source and protocol adapter, not the authority for canonical type semantics.
