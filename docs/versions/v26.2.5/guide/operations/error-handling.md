---
search: false
---

# Error Handling and Recovery

This guide is for operations triage and recovery.
It focuses on runtime failure channels and queue-async crash behavior.

Step-level Item Reject Sink is intentionally a business processing path, not an execution failure.
Developer implementation guidance lives in [Item Reject Sink](/versions/v26.2.5/guide/development/item-reject-sink).

## Failure Channels (Operational View)

| Channel | Scope | Trigger | Primary operational signal |
|---|---|---|---|
| Checkpoint Publication Backlog | pre-execution handoff | checkpoint publication cannot admit work into downstream orchestration quickly enough | publication lag/backlog and handoff latency |
| Item Reject Sink | individual items/streams | step-level recover-and-continue path | reject sink throughput/backlog trends |
| Execution DLQ | full async execution | terminal orchestration failure | execution DLQ backlog growth |

Triage rule:

1. An increase in item rejects with stable execution success usually indicates data quality or business-rule drift.
2. A growing execution DLQ indicates control-plane, dependency, or systemic execution failure.
3. When checkpoint publication backlog rises, it indicates throughput or admission pressure before downstream execution has started.

## Execution DLQ Configuration (Queue-Async)

```properties
pipeline.orchestrator.mode=QUEUE_ASYNC
pipeline.orchestrator.dlq-provider=sqs
pipeline.orchestrator.dlq-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-dlq
```

Execution DLQ applies to terminal execution failures only.
It does not replace item-level rejection flows.

## Execution DLQ Envelope (Terminal Metadata)

Execution DLQ entries include standardized metadata for cross-transport triage:

- execution fields: `tenantId`, `executionId`, `executionKey`, `transitionKey`
- correlation/resource fields: `correlationId`, `resourceType`, `resourceName`
- runtime identity fields: `transport`, `platform`, `terminalStatus`, `createdAtEpochMs`
- failure fields: `terminalReason`, `errorCode`, `errorMessage`, `retryable`, `retriesObserved`

Terminal reason mapping:

| `terminalReason` | Meaning | First action |
|---|---|---|
| `retry_exhausted` | retryable failure class reached terminal state after exhausting retry budget (includes zero-retry configurations (`maxRetries = 0`)) | Stabilise dependency/path, then re-drive bounded batches |
| `non_retryable` | non-retryable failure class (for example `NonRetryableException`) | Correct payload/contract issue before replay |

## Queue-Async Crash Matrix

| Crash point | Behaviour after restart/recovery | Duplicate risk | Required safeguard |
|---|---|---|---|
| Before transition state commit | Work is redelivered and re-executed from last durable version | High | Idempotent operator boundary (`executionId:stepIndex:attempt`) |
| After state commit, before next enqueue | Transition is durable, but next dispatch can stall until due sweeper re-dispatches | Low | Due-execution sweeper + durable state |
| During retry scheduling | Retry can replay from last durable version if scheduling metadata was not committed | Medium | Persist retry intent (`attempt`, `nextDue`) before enqueue |
| After external side effect, before commit | Side effect may repeat on replay because effect and commit are not one transaction | High | Downstream dedupe keyed by transition identity |
| Worker dies while lease held | Lease expires and another worker can claim execution | Low | Short lease window + conditional lease claim (OCC) |

Semantics summary:

1. committed execution state transitions are exactly-once,
2. operator invocation and dispatch are at-least-once,
3. replay is deterministic for control-plane state, not for non-idempotent external systems.

## Retry and Idempotency Defaults

```properties
pipeline.defaults.retry-limit=5
pipeline.defaults.retry-wait-ms=1000
pipeline.defaults.max-backoff=30000
pipeline.defaults.jitter=true
```

Use `NonRetryableException` to fail fast for non-transient failures.

For at-least-once boundaries (queue delivery, operator invocation, re-drive), enforce idempotency with stable transition identity (`executionId:stepIndex:attempt`).

## Operations Runbook

1. Classify incident scope first: item reject trend vs execution DLQ growth.
2. For checkpoint publication incidents, inspect publication lag, handoff latency, duplicate suppression (records intentionally skipped because a checkpoint handoff key was already seen), and delivery failure logs (publication log events emitted when downstream admission fails) before treating the incident as downstream execution failure.
3. Checkpoint publication rejects and downstream admission failures occur before downstream execution admission.
4. They are not execution DLQ events, and they do not use Item Reject Sink by default.
5. For item reject incidents, check fingerprint concentration and dominant error classes; route to business-data remediation and selective re-drive.
6. Treat item reject re-drive as application-owned: default reject envelopes are metadata-only, so replay payload reconstruction is not provided by framework runtime.
7. For execution DLQ incidents, triage terminal execution causes (`FAILED` vs `DLQ`) and validate idempotency before replay.
8. If due executions stall, verify sweeper health and dispatcher lag.
9. Re-drive in bounded batches and monitor duplicate suppression plus retry saturation (retry attempts approaching the configured retry limit).
