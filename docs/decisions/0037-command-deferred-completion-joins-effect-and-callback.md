---
title: Command deferred completion joins effect dispatch with correlated completion
status: accepted
---

# ADR-0037: Command deferred completion joins effect dispatch with correlated completion

## Context

[ADR-0036](./0036-await-is-orthogonal-deferred-completion.md) reserved Command
decoration because a consequential operation can trigger its callback before its
dispatch call returns. Registering completion after dispatch loses that callback;
resuming immediately on observation can advance before the effect outcome is durable.

## Decision

An application-bound native Command may select a declared Connector callback through
`await.callback`. The compiler retains one semantic Command and generates its
completion ordering inside the Command adapter. The existing `CommandStepSupport`
remains the sole effect execution path. The existing Await interaction owns the
correlation, deadline, callback observation, and continuation.

The decorator persists the interaction before resolving the public callback endpoint
and dispatching the effect. A signed callback URI is transient invocation context;
it is excluded from effect records, binding configuration, release metadata, and
telemetry. The application owns the endpoint and authentication implementation.

Callback observation and durable dispatch settlement form an optimistic-concurrency
gate in the existing interaction store:

| State | Event | Result |
| --- | --- | --- |
| `DISPATCHING` | Callback | `COMPLETION_OBSERVED`; no continuation |
| `DISPATCHING` | Success or ambiguity | `DISPATCHED`; await callback |
| `DISPATCHED` | Callback | `COMPLETED`; ordinary completion admission |
| `COMPLETION_OBSERVED` | Success or ambiguity | `COMPLETED`; dispatch worker returns final output |
| `DISPATCHING` | Known retryable non-acceptance | `WAITING`; retain identity and deadline |
| `DISPATCHING` | Terminal rejection or user action | `FAILED` |
| `COMPLETION_OBSERVED` | Retryable, terminal, or user-action outcome | `FAILED`; contradictory provider evidence |
| Any active state | Deadline or cancellation | Existing terminal state wins |

The completion projector receives canonical Command input and the canonical callback
payload. It does not depend on an acknowledgement, which may be unavailable when
dispatch is ambiguous. A callback may complete the pipeline while the immutable
Command effect remains `AMBIGUOUS` as historical evidence.

The worker that closes an early-callback gate owns delivery. Duplicate callback
admission acknowledges the durable result without scheduling a second continuation.
Recovery reuses the interaction and ordinary Command replay decisions.

## Rationale

The effect record answers what happened during dispatch. The interaction answers
whether the final external observation has arrived. Joining these authorities
preserves both without a second callback store, an Await transport that executes
Commands, or an application-owned continuation.

## Consequences

- Provider callback contracts, canonical context/payload/final types, application
  binding, resolver, and authenticator identities participate in release compatibility.
- Registration and completion observation survive process loss and races in both
  in-memory and Dynamo interaction implementations.
- Callback observation alone neither completes an Await unit nor emits final output.
- This refines ADR-0036 for Command initiation; its post-operation transport ordering
  remains applicable to authored internal services.
