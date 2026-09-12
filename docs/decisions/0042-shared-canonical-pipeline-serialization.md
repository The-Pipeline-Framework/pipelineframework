---
title: Canonical pipeline serialization is a shared framework-neutral contract
status: accepted
---

# ADR-0042: Canonical pipeline serialization is a shared framework-neutral contract

## Context

Compiler and build-host code, customer-authored code, runtime implementations, and future transition workers all exchange or inspect the same pipeline JSON shapes. Keeping the canonical mapper in the Quarkus-backed runtime artifact makes runtime implementation ownership appear necessary even when a consumer needs only the shared serialization policy.

## Decision

`pipelineframework-runtime-serialization` owns `org.pipelineframework.config.pipeline.PipelineJson` and its canonical Jackson configuration. The artifact is framework-neutral and may depend only on the semantic model plus the Jackson and protobuf components required to implement that policy.

Compiler and build hosts, customer code, runtime implementations, and future workers depend on this shared contract directly when they use `PipelineJson`. The runtime implementation and deployment tooling consume the contract but do not own or redefine it. The pipeline DSL and semantic model describe pipeline meaning; they do not own JSON serialization policy.

The existing fully qualified class name and serialization behavior remain stable. Changes to mapper discovery, mix-ins, serializers, deserializers, property inclusion, protobuf JSON projection, path encoding, or other emitted and accepted JSON shapes are compatibility changes. They require focused round-trip and historical-payload coverage and, when necessary, an explicit migration strategy.

## Rationale

A small shared artifact lets every host use one policy without importing Quarkus, CDI, deployment/compiler tooling, or the runtime implementation. Keeping one implementation also prevents build-time and runtime JSON shapes from drifting while preserving existing source and binary references to `PipelineJson`.

## Consequences

- `PipelineJson` keeps its package and behavior while moving out of the runtime implementation JAR.
- Runtime, deployment, portable Blocks, and other consumers can declare the serialization contract without gaining runtime implementation ownership.
- The serialization artifact enforces a transitive dependency boundary: semantic-model is the only allowed TPF dependency, and Quarkus, CDI, runtime, and deployment dependencies are forbidden.
- Serialized bytes and accepted historical shapes remain compatibility obligations across compiler/build hosts, customer code, runtimes, and workers.
