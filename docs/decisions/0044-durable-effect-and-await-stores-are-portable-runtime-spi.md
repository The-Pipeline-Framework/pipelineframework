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

## Bounded itemised observation

The store SPI also owns `findByUnitPage`, with the provider-neutral
`AwaitInteractionCursor` and `AwaitInteractionPage` records in runtime-core.
Each call evaluates at most its requested page limit and returns progress even
when filtering leaves the page empty. The cursor binds tenant and Await unit;
its position remains opaque outside the store implementation. Providers must
implement this operation in a coordinated contracts/runtime release. The legacy
`findByUnit` operation does not establish bounded work because it may load every
page.

Traversal follows stable provider index order, without promising a cross-page
snapshot or a globally smallest item. Following continuations over an unchanged
unit must enumerate every unfiltered member; imported delimiter-bearing identities
must not alias, and lifecycle status changes must not change positions. The runtime
orders candidates within a page before selecting an authoritative interaction.

The runtime owns current-unit selection and rechecks an indexed candidate through
the authoritative interaction read. An empty page, exhausted cursor or completed
interaction does not establish that the parent has applied all consequences.

## Retained timeout consequences

A successful interaction timeout atomically retains pending consequence work in
its semantic store. The runtime discovers a bounded page, verifies authoritative
interaction metadata, converges unit and admission state, applies the parent
transition and persists required journal facts before acknowledging that work.
Acknowledgement is scoped to the timed-out identity/version and does not change
semantic status or version. A configured journal failure must leave recovery
work pending; best-effort telemetry is insufficient evidence of convergence.
Completion and timeout remain mutually exclusive semantic decisions.

This protocol belongs to the shared Await store SPI and its runtime implementations,
not to AWS mechanical coordination. Providers implement `findDueTimeoutConsequences`
and `completeTimeoutConsequences`; `getMetadata` permits authoritative recovery
without requiring payload decoding. Dynamo reuses its existing deadline index with
a distinct work partition. Recovery is bounded by semantic retention; expiry may
retire work only after the stored retention deadline. Imported timed-out records
conservatively retain consequences for idempotent reconciliation.

Already stored timed-out records from older implementations have no such projection.
A coordinated rollout must drain old executions, isolate a fresh release, or perform
an explicitly reviewed native repair/backfill. This change does not scan or migrate
old semantic state automatically, and does not extend its retention horizon.
