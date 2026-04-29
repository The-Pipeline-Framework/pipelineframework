---
search: false
---

# Protobuf-over-HTTP Dispatch Design (Orchestrator)

## Scope

This document defines orchestrator-side dispatch changes required to make Protobuf-over-HTTP replay-safe under at-least-once delivery and multi-instance deployments.

Status in current GA track:

1. Contract parity is being aligned across REST, gRPC, Function, and Protobuf-over-HTTP.
2. Async control-plane semantics are shared across transports (same transition identity and retry model).
3. Event-sourced dispatch journaling remains a future evolution path, not a GA blocker.

## Dispatch Metadata Generation

For every outbound dispatch, orchestrator must emit:
- `x-tpf-correlation-id`
- `x-tpf-execution-id`
- `x-tpf-idempotency-key`
- `x-tpf-retry-attempt`
- `x-tpf-deadline-epoch-ms` (optional; when absent, receiver executes without deadline guard)
- `x-tpf-dispatch-ts-epoch-ms`
- `x-tpf-parent-item-id` (optional)

Idempotency keys must be deterministic from stable work identity (pipeline + step + business key + lineage index), never random UUIDs.

## Durable Dispatch State

Current baseline in GA code:

1. execution-level durable state is tracked in shared store (`ExecutionStateStore`),
2. work dispatch is durable through queue dispatcher providers (`WorkDispatcher`),
3. per-dispatch item journaling is not yet the runtime engine of record.

Future hardening target:

- Track every outbound item in durable shared storage keyed by `(executionId, stepId, idempotencyKey)`.

Required states:
- `PENDING`
- `SENT`
- `ACKED`
- `FAILED_RETRYABLE`
- `FAILED_FINAL`

Transitions:
- `PENDING -> SENT` when wire dispatch starts.
- `SENT -> ACKED` on successful response commit.
- `SENT -> FAILED_RETRYABLE` for retryable envelope codes.
- `SENT -> FAILED_FINAL` for non-retryable envelope codes.
- `FAILED_RETRYABLE -> SENT` on redelivery attempt (retry attempt incremented).

## Crash Recovery

On restart:
1. Scan records not in `ACKED`/`FAILED_FINAL`.
2. Re-enqueue deterministically honoring ordering constraints.
3. Preserve idempotency key and correlation id.
4. Increment retry attempt only for true redelivery.

## Multi-instance Coordination

- Use shared durable storage (database or consistent KV) for dispatch state.
- Leader election is optional if state transitions are CAS/transactional.
- Duplicate in-flight sends are tolerated because operator side is dedupe-safe using stable idempotency keys.

## Planned follow-up implementation work

- Add a dispatch store SPI for pluggable per-item durability.
- Add orchestrator worker reconciliation loop for orphaned `SENT` records.
- Add durable dead-letter routing for `FAILED_FINAL` with operator-facing diagnostics.

## Additional slice: dispatch parity across all transports

The dispatch metadata/state-machine contract must be enforced consistently beyond Protobuf-over-HTTP.

### Target transports

- gRPC transport
- REST/JSON transport
- FUNCTION remote invoke path
- LOCAL transport (for parity testing and deterministic simulation)

### Required parity outcomes

- Same canonical dispatch metadata keys and semantics.
- Same retry-attempt increment rules.
- Same deadline evaluation behavior (absolute deadline only).
- Same duplicate suppression contract (`executionId + stepId + idempotencyKey`).
- Same terminal classification into retryable/non-retryable outcomes.

### Parity test slice

- Add transport-parameterized tests asserting identical behavior for:
  - duplicate dispatch
  - replay after crash simulation
  - retry exhaustion handling
  - deadline-expired fast-fail
