---
title: Transition requests are shared runtime protocol
status: accepted
---

# ADR-0047: Transition requests are shared runtime protocol

## Context

Queue-async coordinators and transition workers exchange pipeline and release identity, execution position, durable transition identity, result shape, redrive intent, and serialized payload identity. The Java request envelope, decoded worker command, payload value, codec interface, and encoding identity lived in the Quarkus-backed runtime artifact even though they contain no runtime implementation or platform integration.

`pipelineframework-runtime-protocol` already owns the protobuf contracts used across customer-runtime and worker boundaries. Leaving the equivalent Java request contract in `pipelineframework` made the shared wire boundary depend on the runtime implementation that happens to host it today.

The current result envelope is not yet a clean protocol value: it also carries in-process decoded objects and converts failures into runtime exceptions. Moving it unchanged would publish coordinator and worker implementation details as shared protocol.

## Decision

`pipelineframework-runtime-protocol` owns `TransitionCommandEnvelope`, `TransitionWorkerCommand`, `SerializedTransitionPayload`, `TransitionPayloadCodec`, and `TransitionPayloadEncoding`.

These types retain their packages, constructors, validation, encoding identity, and conversion behaviour. The protocol artifact depends on released runtime-core and runtime-spi contracts, and its build rejects dependencies on the Quarkus runtime implementation, deployment/compiler tooling, or compiler implementation.

Transport clients and services, worker execution, provider selection, signing, nonce replay protection, telemetry, and runtime payload codec implementations remain in `pipelineframework`. `TransitionResultEnvelope` and its failure conversion remain there until wire results and in-process execution results are separated without changing authored behaviour.

ADR-0048 assigns the Quarkus Maven code-generation host and generated Mutiny gRPC adapters to the Quarkus runtime integration. They do not own the request semantics introduced here and are not dependencies of the handwritten Java request contract.

## Rationale

Customer runtimes and separately operated workers need the same released request contract and payload identity, but neither should depend on the other's runtime implementation. Moving the closed request-side contract first establishes that dependency direction without prematurely freezing the existing hybrid result representation as protocol API.

## Consequences

- Customer runtimes and workers can consume request envelopes and payload contracts from runtime-protocol without loading `pipelineframework`.
- Source, binary, serialized, and protocol compatibility apply to the moved request types and encoding identity.
- Pipeline and release identities remain release-pinned values inside the envelope; Maven versions select the compatible protocol implementation and do not replace those identities.
- The result side remains an explicit boundary defect and requires a follow-up split between portable wire results and runtime-local decoded outcomes.
- Quarkus, REST, gRPC, and SQS adapters continue to consume the same protocol types rather than defining transport-specific semantics.
