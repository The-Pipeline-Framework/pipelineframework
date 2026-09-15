---
title: Work dispatch is portable runtime SPI
status: accepted
---

# ADR-0053: Work dispatch is portable runtime SPI

## Context

Queue-async execution, retry, redrive, sweep, segmented execution, and Await continuation all enqueue the same execution identity through a selected work-dispatch provider. The provider interface and its two-field work item lived beside Quarkus CDI events and the SQS implementation, and provider readiness accepted the Quarkus-owned `PipelineOrchestratorConfig` type.

An independently implemented queue or scheduler needs the work identity, selection contract, delay semantics, readiness result, and asynchronous enqueue operations. It does not need the runtime implementation or its configuration mapping.

## Decision

`pipelineframework-runtime-spi` owns the queue-oriented `org.pipelineframework.orchestrator.WorkDispatcher` and `ExecutionWorkItem`. They retain their packages, selection defaults, immediate and delayed enqueue signatures, null-delay compatibility, work-item fields, and validation. The readiness default fails closed when a provider has not implemented the new operation.

Provider readiness becomes a zero-argument operation. A selected provider validates the configuration supplied when it was constructed or injected. The runtime coordinator continues to select providers and aggregate readiness failures without passing runtime-host configuration through the SPI.

The separate functional-core `org.pipelineframework.runtime.core.WorkDispatcher` remains unchanged; it adapts in-process pipeline work and is not the queue-provider contract.

CDI-event and SQS implementations, scheduling executors, JSON serialization, AWS clients, local loopback, worker-pool offloading, configuration mapping, provider selection, polling, and coordinator dispatch policy remain in `pipelineframework`.

## Rationale

Work dispatch is an independently implementable runtime provider boundary. Publishing the SPI lets self-hosted runtimes integrate another durable queue or scheduler without loading or changing atomically with the Quarkus implementation.

`ExecutionWorkItem` carries only stable tenant and execution identity. It does not expose payloads, credentials, connection context, runtime implementation state, or worker lifecycle details.

## Consequences

- Work-dispatch providers can compile against `pipelineframework-runtime-spi` without depending on `pipelineframework`.
- The migration from the published 26.9.1–26.9.3 `startupValidationError(PipelineOrchestratorConfig)` operation to the 26.9.4 zero-argument operation intentionally breaks source and binary compatibility for existing dispatcher providers and callers. Rebuild both against matching runtime/SPI artifacts and implement the new readiness operation; mixed old provider binaries fail closed at startup rather than silently reporting ready. Other dispatcher operations and work-item shape are unchanged. After this migration, Java source and binary compatibility require explicit review.
- Serialized compatibility applies wherever a dispatcher transmits or persists `ExecutionWorkItem`.
- The runtime remains responsible for producing replay-safe execution identity, deciding when work is due, and handling dispatch uncertainty or failure.
- Existing runtime tests continue to prove submission, retry, redrive, sweep, segmentation, Await continuation, SQS encoding, delay clamping, loopback, and provider selection.
