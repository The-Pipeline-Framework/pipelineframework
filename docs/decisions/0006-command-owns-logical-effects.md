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

This decision governs `framework/runtime-core` Command contracts,
`framework/runtime` Command execution, Command connectors, and effect stores.

## Rationale

Exactly-once cannot be manufactured after an unknowable third-party result, but stable
logical identity and recorded authority can prevent unsafe accidental redispatch.

## Consequences

- Generic cache and typed persistence cannot authorize an external effect.
- Provider idempotency keys align with logical Command identity.
- Retry/redrive support must preserve effect identity and reject unsafe unsupported paths.
