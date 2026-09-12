---
title: API owns authored compiler discovery surfaces
status: accepted
---

# ADR-0039: API owns authored compiler discovery surfaces

## Context

Authored Java discovery annotations and their portable compile-time semantics are used by the compiler,
but have historically lived with a runtime integration. That placement can pull platform-specific types
into the public authoring contract. The production compiler currently discovers Java symbols through
JSR-269. YAML owns pipeline topology, step sequence, cardinality, and types; `@PipelineStep` owns Java-local
discovery and its documented execution hints.

## Decision

The framework-neutral API owns authored compiler discovery surfaces, including `@PipelineStep`,
`OrderingRequirement`, and `ThreadSafety`. YAML owns pipeline topology, step sequence, cardinality, and types;
`@PipelineStep` owns Java-local discovery and its documented execution hints. JSR-269 is the current
production build host. Discovery and normalization produce the same compiler semantic model that later
phases consume.

Migration proceeds in sequence: first stabilize authored API surfaces while preserving JSR-269 as the
production host; next extract the JSR-269 compiler and semantic phases from Quarkus deployment; only after
that add a Jandex source-symbol adapter with parity tests. Both source-symbol adapters feed the same semantic
model; Jandex must never create a parallel semantic path. The API may expose portable class tokens, but
validation of platform-specific token types belongs to the relevant integration or capability adapter, not
to the API.

## Rationale

Authored compiler inputs are part of the framework contract even when a particular build host discovers
them. Keeping their types in the API preserves stable names and descriptors without making annotation
definitions depend on one runtime. A shared semantic model keeps build-host differences at the source-symbol
boundary while preserving YAML's authority over topology, step sequence, cardinality, and types, alongside the
annotation's ownership of Java-local hints.

## Consequences

- Applications can compile against authored discovery annotations and compile-time semantics without
  depending on a runtime integration module.
- Runtime integrations retain platform-specific implementations and validate their own capability classes.
- A future discovery adapter must normalize into the existing compiler model and preserve YAML ownership of
  topology, step sequence, cardinality, and types, alongside annotation-owned Java-local hints.
- `@PipelineStep.cacheKeyGenerator` uses `Class<?>` with `Void.class` as its no-override default; explicit
  class identity is retained until an integration or capability adapter applies its own validation.
