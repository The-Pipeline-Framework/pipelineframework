---
title: Secret resolution is portable runtime SPI
status: accepted
---

# ADR-0056: Secret resolution is portable runtime SPI

## Context

Runtime hosts resolve secret references when they authenticate remote transition workers and operate hosted control-plane endpoints. The resolver contract is a single operation over an opaque reference, but it lived beside the Quarkus runtime implementation and its environment, system-property, and MicroProfile Config lookup policy.

Keeping that contract in `pipelineframework` would require a self-hosted runtime to depend on the Quarkus implementation artifact merely to integrate another secret source. Moving the built-in lookup policy would instead leak host configuration mechanics into the portable boundary.

## Decision

`pipelineframework-runtime-spi` owns `ControlPlaneSecretResolver`, retaining its package and resolution signature. The reference remains opaque to the SPI.

The runtime retains `LocalControlPlaneSecretResolver`, its `env:`, `sys:`, and `config:` schemes, MicroProfile Config integration, validation and error messages. The built-in resolver is a Quarkus default bean so a host-provided implementation can replace it without changing runtime consumers.

Resolved secret values remain process-local runtime data. They must not be added to shared runtime model, protocol envelopes, pipeline contracts, release metadata, telemetry, or durable control-plane records.

## Rationale

Secret lookup is independently implementable host policy. A released SPI lets self-hosted runtimes integrate a vault or another credential source without loading or changing atomically with TPF's Quarkus implementation, while keeping secret-reference syntax and provider lifecycle out of shared contracts.

## Consequences

- Secret resolver implementations compile against `pipelineframework-runtime-spi` without depending on `pipelineframework`.
- Java source and binary compatibility apply to the resolver interface.
- Secret-reference schemes are implementation policy, not a serialized or protocol compatibility promise of the SPI.
- Provider clients, credentials, tenant and connection context, caching, refresh, auditing, and failure policy remain owned by runtime implementations.
- The built-in local resolver preserves its existing resolution behavior and remains available when no host-provided bean exists.
