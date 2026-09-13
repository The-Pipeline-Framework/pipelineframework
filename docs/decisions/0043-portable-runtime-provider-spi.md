---
title: Runtime provider contracts are portable SPI
status: accepted
---

# ADR-0043: Runtime provider contracts are portable SPI

## Context

Cache, persistence, repository, managed Command, and object mapping contracts are implemented by provider libraries and customer integrations, but their Java types lived in the Quarkus-backed runtime artifact. That location made a runtime implementation appear to own contracts that must also be usable by other runtime hosts and future infrastructure workers.

pipelineframework-runtime-api already has a narrower purpose: customer-authored reactive service and failure contracts. Expanding it with provider, storage, effect, and object-boundary contracts would blur the distinction between application service authoring and runtime extension.

## Decision

pipelineframework-runtime-spi owns the reactive contracts implemented across the runtime boundary:

- cache provider, cache-key strategy, and cache-target contracts;
- persistence and repository provider contracts, including repository request, result, and not-found types;
- managed Command connector and request contracts;
- Object Ingest projection contracts; and
- Object Publish mapper, renderer, payload, and chunk contracts.

The artifact depends only on pipelineframework-api, pipelineframework-runtime-core, and Mutiny. It must not depend on Quarkus, CDI, Spring, compiler/deployment tooling, serialization implementations, transport implementations, or the TPF runtime implementation.

Existing packages, public signatures, defaults, and authored behavior remain unchanged. Quarkus and other runtime hosts consume these released SPI types and provide discovery, lifecycle, routing, telemetry, and execution implementations around them.

## Rationale

Provider implementations and customer-authored boundary adapters can now compile against a stable released contract without loading a TPF runtime implementation. Keeping Mutiny in the SPI preserves the established reactive contract while keeping Quarkus build-time/runtime duality out of semantic ownership. CDI discovery and Quarkus extension wiring remain integration mechanics rather than reasons to place the interfaces in the runtime JAR.

The separate artifact also gives customer execution and future TPF-operated infrastructure a small shared seam for compatible request, identity, payload, and provider behavior without sharing worker implementations or tenant and connection infrastructure.

## Consequences

- pipelineframework-runtime-api remains the narrow customer service API described by ADR-0041.
- JDK and semantic runtime data shared more broadly remain in pipelineframework-runtime-core.
- Runtime managers, registries, runners, telemetry, configuration, provider implementations, and Quarkus/CDI wiring remain outside runtime-spi.
- Runtime implementations and provider plugins declare runtime-spi directly; depending on a runtime implementation is no longer required merely to implement these contracts.
- Public SPI changes require source and binary compatibility review. Command identities, repository references, and object payload behavior also carry replay and serialized-contract obligations where they cross durable or remote boundaries.
