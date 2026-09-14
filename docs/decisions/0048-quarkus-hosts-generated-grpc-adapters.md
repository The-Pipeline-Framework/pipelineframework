---
title: Quarkus hosts generated gRPC adapters
status: accepted
---

# ADR-0048: Quarkus hosts generated gRPC adapters

## Context

`pipelineframework-runtime-protocol` owns the stable protobuf schemas and handwritten transition request contracts shared by customer runtimes and separately operated workers. It also used the Quarkus Maven plugin and `quarkus-grpc` to generate Java, gRPC, and Mutiny classes into the shared protocol artifact.

Those generated classes are Quarkus build and runtime integration mechanics. Keeping their generator and dependencies in the protocol artifact made consumers of the shared contract inherit the canonical runtime's framework choice, even though the schemas themselves are portable.

## Decision

`pipelineframework-runtime-protocol` publishes the `.proto` schemas as resources and does not generate or package Java transport adapters. It rejects Quarkus dependencies.

The Quarkus `pipelineframework` runtime scans the released protocol artifact and generates its Java, gRPC, and Mutiny adapters during its own build. Existing generated package and class names remain unchanged, so runtime services and application-authored behaviour do not change.

A separate Quarkus protocol-adapter artifact is not introduced. The adapters belong to the existing Quarkus runtime integration unless another independently versioned consumer demonstrates that a distinct artifact is necessary.

## Rationale

The schema is the stable cross-runtime and cross-process contract. Generated bindings are projections owned by each framework or language integration. Hosting generation in the Quarkus runtime preserves the familiar extension build/runtime mechanism without allowing that mechanism to determine shared semantic ownership.

## Consequences

- Customer runtimes and workers can consume protocol schemas without loading Quarkus or generated Mutiny services.
- The Quarkus runtime continues to expose the same generated Java and Mutiny types.
- Non-Quarkus consumers generate bindings from the packaged schemas using their native toolchain.
- Protobuf schema compatibility belongs to `pipelineframework-runtime-protocol`; Quarkus adapter compatibility belongs to the Quarkus runtime artifact.
- A framework-neutral pre-generated Java binding artifact may be introduced later only if a real consumer requires it.
