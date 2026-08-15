---
title: Remote transition outcome uncertainty belongs to the coordinator
status: accepted
---

# ADR-0014: Remote transition outcome uncertainty belongs to the coordinator

## Context

Transition workers execute one bounded continuation but do not own durable execution
state, leases, retries, or result authority. A coordinator-side REST request deadline
proves only that the caller did not receive a result. It does not prove that the remote
worker stopped or that its effects cannot still occur.

## Decision

The coordinator records a REST transition deadline as `REMOTE_OUTCOME_UNKNOWN`. That
state suppresses automatic lease claim, sweep, and retry. An operator may explicitly
re-drive only after independently confirming the original invocation's disposition; the
new attempt remains subject to ordinary idempotency safeguards.

Worker availability heartbeats remain availability evidence, not per-attempt disposition.
No process-local worker registry, result cache, or live-stream resurrection is correctness
authority. A durable remote-attempt ownership and result protocol is a separate capability.

## Rationale

Retrying after an ambiguous caller timeout could create competing physical execution
while the original transition remains live. The durable coordinator is the existing owner
of recovery authority, so it must preserve that ambiguity instead of translating it into
confirmed worker loss.

## Consequences

- Long remote transitions emit pre-deadline and outcome-unknown operational events without
  payload data.
- Operators must reconcile the old invocation before explicit re-drive.
- This decision does not promise transparent mid-transition takeover or live-stream recovery.
