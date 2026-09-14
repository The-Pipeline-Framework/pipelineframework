---
title: Transition results separate wire data from runtime outcomes
status: accepted
---

# ADR-0049: Transition results separate wire data from runtime outcomes

## Context

Remote transition workers return outcome, serialized payloads, Await suspension data, failure data, and terminal-publication flags. Customer runtimes and separately operated workers must share those values without sharing a runtime implementation.

The existing `TransitionResultEnvelope` mixed that portable data with decoded application objects and executable failure reconstruction. Its decoded outputs cannot cross a process boundary, while its exception classification depends on runtime classes and class loading. Serializing the hybrid record happened to omit decoded objects, but the Java ownership still made transport contracts depend on `pipelineframework`.

## Decision

`pipelineframework-runtime-protocol` owns `TransitionWireResult`, `TransitionWorkerOutcome`, `TransitionAwaitSuspension`, and the value-only `TransitionFailureEnvelope`. These types contain only immutable serialized data and validation that applies equally to every transport.

`pipelineframework` retains `TransitionResultEnvelope` as the runtime-local execution result. It may carry decoded application objects for an in-process transition. It converts to and from `TransitionWireResult` only at REST, gRPC, and SQS transport edges. A result carrying decoded objects is rejected if code attempts to send it across a remote worker boundary.

Throwable inspection, retryable Command-effect extraction, failure-class loading, and reconstruction of runtime exceptions remain runtime implementation behaviour. They are not methods or dependencies of the shared protocol values.

The wire JSON field names and value shapes remain unchanged. Pipeline and release identity remain release-pinned request metadata under ADR-0047; Maven artifact versions select compatible protocol code and do not replace those identities.

## Rationale

A shared result artifact is justified because both sides can consume the same released data contract. A shared runtime result implementation is not: decoded customer classes and runtime exception behaviour belong to the process executing them. Two representations therefore describe two real process-boundary states rather than duplicating one semantic model.

## Consequences

- Customer runtimes and transition workers can exchange both request and result contracts through `pipelineframework-runtime-protocol` without loading `pipelineframework`.
- Source and binary compatibility apply to the public Java protocol records; serialized and protocol compatibility additionally apply to their JSON shape and validation.
- REST, gRPC, and SQS must serialize `TransitionWireResult`, never `TransitionResultEnvelope`.
- In-process execution keeps decoded outputs and existing authored behaviour.
- Runtime failure reconstruction can evolve with runtime policy without adding implementation dependencies to the protocol artifact.
