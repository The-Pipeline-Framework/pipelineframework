---
title: Expansions own version-aligned distributions
status: accepted
---

# ADR-0058: Expansions own version-aligned distributions

## Context

GraphQL and OpenAPI each combine independently published Blocks, Connector contracts, provider implementations,
and application build integration. Keeping only a prose catalogue made compatible combinations implicit and forced
every application to reconstruct the same dependency set. None of these combinations requires a new compiler
semantic, runtime step kind, registry, or execution host.

## Decision

The `pipelineframework-expansions` repository publishes independently versioned POM distributions over released
Block and Connector artifacts. The initial distributions are `org.pipelineframework.expansions:graphql` and
`org.pipelineframework.expansions:openapi`.

An Expansion preserves the ownership of every contained capability. Compiler and DSL semantics remain with their
existing semantic owners. Connectors retain typed I/O behavior, Blocks remain compile-time composition, and the
consuming application retains bindings, credentials, configuration, and Command authority. Build plugins and
annotation-processor paths remain explicit because a Maven dependency cannot activate application build tooling.

## Consequences

- Applications can consume and upgrade one verified capability family without copying a dependency catalogue.
- Expansions can release independently when their released Block and Connector contracts remain compatible.
- Cross-repository compatibility is proven by clean dependency resolution and framework example applications.
- Expansion artifacts must not introduce a parallel runtime, semantic model, hidden Connector binding, or
  `expansion:` DSL section.
