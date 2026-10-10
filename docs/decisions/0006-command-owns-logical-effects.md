---
title: Command owns logical external effects
status: accepted
---

# ADR-0006: Command owns logical external effects

## Context

External writes may be retried after transport failures or ambiguous success. Treating
each execution attempt as a new business effect risks duplicate payments, messages,
archives, tickets, or provisioning.

## Decision

Command represents one logical external effect. Logical Command identity is distinct
from execution, dispatch, worker, retry, and transport attempt identity.
`CommandEffectStore` is the authority for recorded effect state, outcomes, and duplicate
policy. A recorded successful effect may be returned without redispatch. Ambiguous
success remains protected unless provider idempotency, reconciliation, or explicit policy
makes redispatch safe under the same logical effect identity.

Ordinary execution re-drive is not authorization to retry a retained effect. Deliberate
Command retry is an explicit control-plane intent that resumes the failed execution at
its resumable root step while carrying the exact persisted logical `CommandId` reported by
the terminal Command failure. Resume location and effect identity are separate: only that
logical effect may claim the invocation-scoped admission, regardless of traversal or
scheduling order. Successful effects encountered earlier during deterministic composition
do not consume it. The Command runtime still asks `CommandEffectStore` to atomically append
and claim the next attempt; the execution control plane cannot reset, delete, or otherwise
manufacture effect state.
One admitted execution retry deterministically identifies one logical effect attempt, so worker
recovery cannot turn the same admission into additional attempts.

The mutable admission is framework runtime state, not application execution identity.
`PipelineExecutionContext` and `CommandRequest` do not expose it. Invocation machinery may
propagate an opaque snapshot, but only the Command runtime can install, inspect, claim, or
verify the admission. This is a framework API ownership boundary; it is not a claim that
arbitrary hostile code in the same JVM is isolated by JPMS, process boundaries, or a
cryptographic capability.

This decision governs `pipelineframework-runtime-core` Command contracts,
`framework/runtime` Command execution, Command connectors, and effect stores.

## Rationale

Exactly-once cannot be manufactured after an unknowable third-party result, but stable
logical identity and recorded authority can prevent unsafe accidental redispatch.

## Consequences

- Generic cache and typed persistence cannot authorize an external effect.
- The initial provider idempotency key aligns with logical Command identity; an intentionally new
  occurrence uses the occurrence identity defined by
  [ADR-0025](./0025-command-reissue-uses-occurrence-identity.md).
- Retry/redrive support must preserve effect identity and reject unsafe unsupported paths.
- Execution stores must preserve deliberate retry intent until the targeted transition is
  claimed; effect stores remain the sole authority for whether another attempt is legal.

## Provider-reconciled native recovery

An opted-in native operation may supply authoritative success evidence for an interrupted attempt.
The effect store remains the authority: the original request, configuration, target and execution
binding is retained before reservation and checked again before a conditional settlement. A bound
`PENDING` attempt uses the same strict single-winner dispatch claim for its original executor and
its recovery executor. A bound `DISPATCHING` or `AMBIGUOUS` attempt permits only a read-only
provider inquiry and exact-binding typed-success settlement, not redispatch based on absence,
elapsed time or TTL.

This does not grant the execution control plane or application permission to manufacture effect
state, clear unrelated barriers, or create another attempt. Unsupported providers/stores, callback
invocations and old unbound records retain their existing barriers. See
[provider-reconciled recovery](/deploy/orchestrator-runtime/command#provider-reconciled-recovery)
for current support and durable-record rollout limits.
