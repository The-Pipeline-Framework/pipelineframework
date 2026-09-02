---
title: Intentional Command reissue uses occurrence identity
status: accepted
---

# ADR-0025: Intentional Command reissue uses occurrence identity

## Context

[ADR-0006](./0006-command-owns-logical-effects.md) gives Command authority over one
logical external effect, while [ADR-0018](./0018-command-effects-use-immutable-durable-revisions.md)
retains that authority as immutable conditional revisions. Ordinary replay and a retry of a
failed dispatch must not manufacture another external effect. Operators nevertheless need a
controlled way to request a second effect after the first occurrence succeeded, for example when
a customer explicitly asks for another notification or archive operation.

Using either the logical Command id or a fresh attempt id as the provider idempotency key cannot
express that distinction. The logical id would suppress the intentionally new effect, while an
attempt id would make transport retries create additional effects.

## Decision

Command effect history has three identities:

- the logical Command id groups the complete retained effect history;
- the occurrence id is the provider idempotency key for one intentionally requested external
  effect;
- the attempt id identifies one dispatch attempt within that occurrence.

The initial occurrence uses the logical Command id for compatibility. Failed-attempt retry keeps
the same occurrence id and changes only the attempt id. `REISSUE_COMMAND` requires the optimistic
execution version, a nonblank audit reason, and the exact logical Command id of a retained
`SUCCEEDED` effect. Admission deterministically creates one new occurrence and attempt, reopens the
same successful execution from its persisted initial input, and authorizes only that target.

The effect store atomically appends the new pending occurrence even when the target uses duplicate
policy `FAIL`. Other successful Commands retain ordinary replay behavior. Missing targets and
targets in any state other than `SUCCEEDED` fail closed before connector dispatch. A failed reissue
becomes the current outcome, retains the prior successful occurrence, and can later use
`RETRY_FAILED_COMMAND` under the reissue occurrence id.

## Rationale

Occurrence identity makes intentional repetition explicit without weakening replay safety.
Provider deduplication remains stable across uncertain or repeated dispatch attempts, while one
operator authorization can produce at most one newly requested external effect.

## Consequences

- Native `CommandDispatchIdentity` and legacy `CommandRequest` expose logical, occurrence, and
  attempt identities. Connectors use the occurrence id as their provider idempotency key.
- Durable Command effect schema version 2 records occurrence, purpose, output, and audit reason per
  attempt. Version 1 records decode as the original occurrence with inferred initial/retry purpose.
- Custom effect stores keep ordinary and retry compatibility, but must explicitly implement atomic
  reissue admission before accepting `REISSUE_COMMAND`.
- `FAILED`, `DLQ`, `AMBIGUOUS`, `USER_ACTION_REQUIRED`, `PENDING`, and `DISPATCHING` effects remain
  reissue barriers.
