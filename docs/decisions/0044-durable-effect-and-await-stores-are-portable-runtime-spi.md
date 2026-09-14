---
title: Durable effect and Await stores are portable runtime SPI
status: accepted
---

# ADR-0044: Durable effect and Await stores are portable runtime SPI

## Context

Command effect and Await state must be readable and writable by runtime hosts without making a Quarkus runtime implementation the owner of their contracts. Their store interfaces and durable records lived in `pipelineframework`, even though they contain only TPF runtime identities, connector outcome metadata, JDK types, and Mutiny contracts.

Customer pipeline execution and separately operated infrastructure may need compatible implementations of these stores. Sharing a runtime implementation, CDI discovery, or provider configuration across that boundary would make deployment mechanics part of the durable contract.

## Decision

`pipelineframework-runtime-spi` owns:

- `CommandEffectStore` and the Command effect, attempt, admission, status, and outcome records it exchanges; and
- `AwaitInteractionStore`, `AwaitUnitStore`, and the Await commands, results, records, and statuses they exchange.

Their existing packages, public signatures, defaults, validation, and authored behavior remain unchanged. `pipelineframework` consumes these contracts and continues to own in-memory and Dynamo implementations, configuration mapping, provider selection, CDI wiring, telemetry, coordinators, and recovery mechanics.

Query capture storage is not included in this decision; ADR-0045 records its subsequent extraction after removing record-level Jackson coupling.

Execution-state storage is also excluded. Its interface currently accepts Quarkus runtime configuration in startup validation and requires a separate dependency inversion before extraction.

## Rationale

The stable seam is the operation and durable state exchanged with a store, not the framework host that discovers or runs the implementation. Both customer runtimes and separately deployed infrastructure can consume the released SPI while selecting different implementations and operational wiring.

Keeping the FQCNs and method contracts stable makes this an artifact-ownership correction rather than a semantic or serialized-format change. Keeping implementations and configuration outside the SPI prevents Quarkus build/runtime duality, worker topology, tenant resolution, and connection details from leaking into the shared contract.

## Consequences

- Runtime hosts and provider libraries can implement Command effect and Await persistence without depending on the Quarkus runtime artifact.
- Source, binary, replay, and serialized compatibility apply to the moved records and store methods.
- Runtime implementation tests continue to prove in-memory, Dynamo, retry, completion, timeout, and recovery behavior; pure contract validation tests live with runtime-spi.
- Execution-state storage remains an explicit follow-up boundary defect rather than broadening runtime-spi with runtime configuration dependencies.
