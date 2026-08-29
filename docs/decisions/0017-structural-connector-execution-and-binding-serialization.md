---
title: Structural connector execution and binding serialization
status: accepted
---

# ADR-0017: Structural connector execution and binding serialization

## Context

Unary Query and Command providers expose JDK `CompletionStage` contracts. Earlier provider
metadata described blocking style and broad concurrency scopes, but the runtime did not enforce
those claims. Provider-owned asynchronous execution, framework-owned blocking adaptation, and
framework-owned serialization have different ownership and lifetime rules.

## Decision

Execution facts that change how TPF invokes an operation are structural contracts.
`BlockingOperation` marks a provider method that TPF must invoke on a worker;
`BlockingQueryOperation` and `BlockingCommandOperation` combine that fact with their typed family
contracts. Ordinary Query and Command operations are invoked directly and may complete on any
provider-owned executor without exposing Mutiny in the provider API.

`SerializedOperation` permits one admitted invocation for each connector binding, operation ID,
and operation major version. Its gate covers the complete provider `CompletionStage` lifetime,
not merely the blocking Java method call. Separate bindings remain independent. A queued cancelled
invocation is skipped. Cancellation after admission is forwarded best-effort, while the gate is
retained until the provider stage terminates.

Provider execution-style and concurrency-scope metadata is removed. Parameterized numeric,
provider-wide, or connection-wide limits will be introduced only with a concrete provider need and
a runtime/compiler enforcement contract.

## Rationale

Interfaces make executable behavior inspectable and enforceable without a second declarative
representation. A generic blocking marker keeps the coordinator independent of operation families;
family specializations keep provider APIs discoverable and typed. Binding-plus-operation identity
matches configured resource ownership and avoids accidental serialization across independent
provider instances.

## Consequences

- Blocking invocation occupies a worker only until the provider method returns its stage;
  serialization continues until that stage terminates.
- Reactive and provider-managed operations retain direct asynchronous composition.
- Connector provider manifests use schema v3 and no longer contain `executionCapabilities`.
- Command policy no longer requests execution style or concurrency scope.
- Unary `CompletionStage` Query preserves non-blocking execution but does not expose element-level
  stream backpressure.
- A finite streaming Query uses the separate publisher contract in [ADR-0019](./0019-finite-streaming-query-reuses-one-to-many.md);
  serialization covers its publisher and provider-resource lifetime.
