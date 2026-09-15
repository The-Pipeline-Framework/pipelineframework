---
title: Item-reject publication is portable runtime SPI
status: accepted
---

# ADR-0054: Item-reject publication is portable runtime SPI

## Context

Step-level ITEM and STREAM rejection is routed to a selected sink with explicit durability, failure-policy, payload-inclusion, and startup-readiness behavior. An independently implemented sink needs the immutable rejection envelope, provider identity, priority, durability declaration, readiness result, and asynchronous publication operation.

The sink interface and envelope lived beside Quarkus routing and the in-memory, logging, and SQS implementations. Provider readiness also accepted the runtime-owned `ItemRejectConfig` mapping, forcing provider implementations to depend on runtime configuration rather than validating the configuration supplied when they were constructed.

## Decision

`pipelineframework-runtime-spi` owns `ItemRejectSink` and `ItemRejectEnvelope`. They retain their packages, provider defaults, durability declaration, publication signature, envelope record components, validation, and serialized shape.

Provider readiness becomes a zero-argument operation. A selected provider validates the configuration supplied when it was constructed or injected. `ItemRejectRouter` continues to select providers, aggregate readiness, enforce production durability, decide failure policy and payload inclusion, construct envelopes, and record routing outcomes.

The runtime retains `ItemRejectConfig`, the in-memory, logging, and SQS implementations, JSON serialization, metrics, AWS clients, worker-pool offloading, CDI lifecycle, launch-mode policy, and routing behavior.

## Rationale

Item-reject destinations are independently implementable runtime providers. Publishing their narrow SPI lets a self-hosted runtime integrate another durable destination without loading or changing atomically with the Quarkus runtime implementation.

The envelope is semantic publication data rather than a mandated wire protocol. Different providers may persist or transmit it through different encodings while preserving the same rejection facts.

## Consequences

- Item-reject providers compile against `pipelineframework-runtime-spi` without depending on `pipelineframework`.
- Java source and binary compatibility apply to the sink and envelope contracts.
- Serialized compatibility applies wherever a provider persists or transmits the envelope; changes to record components require explicit compatibility review.
- Tenant and execution context remain optional data values in the envelope; the SPI does not acquire tenant resolution, credentials, connection lifecycle, retry policy, or routing authority.
- Runtime tests continue to prove provider selection, strict startup, durability enforcement, failure policy, payload inclusion, memory retention, SQS serialization, and publication behavior.
