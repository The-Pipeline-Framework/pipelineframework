---
title: Dead-letter publication is portable runtime SPI
status: accepted
---

# ADR-0052: Dead-letter publication is portable runtime SPI

## Context

Terminal failure publication is a runtime extension boundary. Provider implementations need the immutable failure envelope, selection identity, priority, readiness result, and asynchronous publication operation. Those contracts lived beside Quarkus orchestration and SQS implementation code, and provider readiness accepted the Quarkus-owned `PipelineOrchestratorConfig` type.

That ownership forced an independently implemented dead-letter destination to depend on the runtime implementation and its configuration mapping even though the provider contract itself requires only JDK types and Mutiny.

## Decision

`pipelineframework-runtime-spi` owns `DeadLetterPublisher` and `DeadLetterEnvelope`. They retain their packages, provider defaults, publication signature, envelope record components, builder, validation, and serialized shape.

Provider readiness becomes a zero-argument operation. A selected provider validates the configuration supplied when it was constructed or injected. The runtime coordinator continues to select the provider and aggregate readiness failures without passing runtime-host configuration through the portable SPI.

Logging and SQS implementations, JSON serialization, metrics, AWS clients, worker-pool offloading, configuration mapping, provider selection, CDI lifecycle, and coordinator failure policy remain in `pipelineframework`.

## Rationale

Dead-letter destinations are independently implementable runtime providers. A provider can consume a released SPI contract rather than loading or changing atomically with the Quarkus runtime. Provider construction remains the correct place to adapt host-specific configuration into provider-owned state.

The envelope is semantic publication data rather than a transport protocol: it is passed to an SPI implementation and may be encoded differently by different providers.

## Consequences

- Dead-letter providers can compile against `pipelineframework-runtime-spi` without depending on `pipelineframework`.
- Java source and binary compatibility apply to the publisher and envelope contracts.
- Serialized compatibility applies wherever a provider persists or transmits the envelope; changes to its fields require explicit compatibility review.
- Tenant, execution, correlation, transition, failure, retry, transport, platform, and timestamp values remain data supplied by the runtime; the SPI does not acquire tenant resolution, credentials, or connection lifecycle.
- Existing runtime tests continue to prove provider selection, readiness aggregation, SQS serialization, offloading, metrics, and terminal-failure routing.
