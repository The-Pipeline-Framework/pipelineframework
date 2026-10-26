---
title: Execution state storage is portable runtime SPI
status: accepted
---

# ADR-0046: Execution state storage is portable runtime SPI

## Context

Durable execution identity, status, leasing, redrive intent, result shape, and the operations used to persist them are shared runtime semantics. `ExecutionStateStore` and the records it exchanges lived in the Quarkus-backed runtime artifact even though their contract depends only on JDK types, Mutiny, and the framework-neutral pipeline contract descriptor.

The store SPI also accepted `PipelineOrchestratorConfig` during startup validation. That configuration mapping is owned by the Quarkus runtime host and made every execution-state provider depend on runtime implementation configuration merely to report whether its own resources were ready.

## Decision

`pipelineframework-runtime-spi` owns `ExecutionStateStore`, `CreateExecutionResult`, `ExecutionCreateCommand`, `ExecutionRecord`, `ExecutionRedriveIntent`, `ExecutionResultShape`, and `ExecutionStatus`.

The SPI types retain their packages and execution semantics. Provider readiness becomes a zero-argument operation: a selected provider validates the resources and configuration supplied when that provider was constructed or injected. The readiness default fails closed when a provider has not implemented the new operation. Runtime coordinators select providers and aggregate their readiness errors without passing a Quarkus configuration type through the portable SPI.

In-memory and Dynamo implementations, configuration mapping, provider selection, durable codecs, coordinators, telemetry, CDI and tenant wiring, and worker mechanics remain in `pipelineframework`.

Completion lifecycle observations follow the same ownership. The portable Await
contracts describe immutable first-admission facts, bounded outstanding-admission
queries, and scoped parent-release acknowledgements. `AwaitInteractionStore` owns
admission persistence; `ExecutionStateStore` owns the parent CAS and its atomic
acknowledgement. The runtime read owner checks causal coverage and consequence
convergence before reporting release or retiring an outstanding projection.
Provider callbacks, generation claims, history and best-effort telemetry cannot
establish these semantic facts. Missing historical coverage or timestamps remain
unknown; current membership and clocks cannot supply the missing relation. The
[Await occurrence decision](./0073-await-unit-occurrences-bind-completion-release.md)
defines the retained cause and exact unit identity that carry this coverage.

## Rationale

Customer-facing runtimes and separately operated workers need one released contract for durable execution state without sharing a Quarkus runtime implementation. The stable Java operation and record surface is that seam. Provider construction and runtime-host configuration are integration concerns and do not belong in the shared contract.

## Consequences

- Execution-state providers compile against runtime-spi without depending on the Quarkus runtime artifact.
- The migration from the published 26.9.1–26.9.3 `startupValidationError(PipelineOrchestratorConfig)` operation to the 26.9.4 zero-argument operation intentionally breaks source and binary compatibility for existing store providers and callers. Rebuild both against matching runtime/SPI artifacts and implement the new readiness operation; mixed old provider binaries fail closed at startup rather than silently reporting ready. Other store operations and record shapes are unchanged. After this migration, source and binary compatibility require explicit review.
- Persisted-record and replay compatibility remain owned by the runtime codecs and store implementations that write durable representations.
- Provider implementations are responsible for retaining the configuration required by their own readiness validation.
- Runtime tests continue to prove provider selection, startup validation, leasing, replay, redrive, persistence, and coordinator behaviour.
- Required lifecycle operations add a separate compatibility obligation for store
  providers and callers. Rebuild against the matching candidate contracts/runtime
  set and verify their atomic-write, retained-state and worker-import behavior.
  This ownership decision does not certify the provisional implementation; the
  [completion audit](/evolve/durable-coordinator/aws-durable-production-plan)
  records its unresolved causal-coverage and migration evidence.
