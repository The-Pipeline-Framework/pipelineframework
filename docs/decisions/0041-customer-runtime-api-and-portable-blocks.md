---
title: Customer runtime APIs and portable Blocks exclude runtime integrations
status: accepted
---

# ADR-0041: Customer runtime APIs and portable Blocks exclude runtime integrations

## Context

Customer-authored reactive services need stable contracts without inheriting the Quarkus-backed runtime implementation. Packaged Blocks using only those contracts should remain portable across runtime hosts.

## Decision

`pipelineframework-runtime-api` owns the customer-authored reactive service interfaces and `NonRetryableException`. It may expose Mutiny types, but it does not depend on Quarkus, CDI, protobuf, deployment/compiler tooling, runtime implementations, or other TPF modules. JDK-only runtime contracts remain in `pipelineframework-runtime-core`.

Portable Blocks depend directly on runtime-api and any separately required framework-neutral contracts. CDI annotations use the standard Jakarta CDI API; runtime integrations and Quarkus remain application-host concerns.

## Rationale

Mutiny is part of the reactive authoring contract, while runtime execution and platform integration belong to the imperative shell. Direct dependencies make that boundary enforceable and keep Blocks reusable across hosts.

## Consequences

- Customer service implementations can compile without the runtime implementation artifact.
- Portable Blocks may use runtime-api and runtime-core while excluding runtime, deployment, and Quarkus dependencies.
- Blocking service adapters and runtime exception-to-transport mapping remain runtime implementation concerns.
