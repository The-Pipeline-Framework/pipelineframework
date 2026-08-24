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
its current Command step. The Command runtime still asks `CommandEffectStore` to
atomically append and claim the next attempt; the execution control plane cannot reset,
delete, or otherwise manufacture effect state.
One admitted execution retry deterministically identifies one effect attempt, so worker
recovery cannot turn the same admission into additional attempts.

This decision governs `framework/runtime-core` Command contracts,
`framework/runtime` Command execution, Command connectors, and effect stores.

## Rationale

Exactly-once cannot be manufactured after an unknowable third-party result, but stable
logical identity and recorded authority can prevent unsafe accidental redispatch.

## Consequences

- Generic cache and typed persistence cannot authorize an external effect.
- Provider idempotency keys align with logical Command identity.
- Retry/redrive support must preserve effect identity and reject unsafe unsupported paths.
- Execution stores must preserve deliberate retry intent until the targeted transition is
  claimed; effect stores remain the sole authority for whether another attempt is legal.
