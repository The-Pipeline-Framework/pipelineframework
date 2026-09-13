---
title: Portable connector contracts depend on framework-neutral modules
status: accepted
---

# ADR-0040: Portable connector contracts depend on framework-neutral modules

## Context

Provider-neutral connector contracts describe typed external observations and effects. Depending on the Quarkus-backed runtime artifact couples those contracts to an integration they do not own and makes them less portable for provider implementations and application hosts.

## Decision

Portable connector contracts depend directly on `pipelineframework-runtime-core` and `pipelineframework-semantic-model` for shared runtime and semantic types. Runtime implementations, deployment tooling, Quarkus adapters, and provider-specific integrations remain dependencies of their respective implementation modules.

## Rationale

The connector contract owns portable capability vocabulary; runtime-core and semantic-model own the framework-neutral types it needs. Keeping integration dependencies at implementation boundaries lets providers and hosts consume the contracts without importing a runtime platform.

## Consequences

- Provider-neutral connector artifacts remain independent of the runtime and deployment artifacts and Quarkus.
- Provider implementations may depend on these contracts and add their own integration dependencies.
- Runtime-specific behavior remains in runtime or provider adapter modules.
