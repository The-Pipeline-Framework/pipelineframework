---
search: false
---

# Alerting

Alerts should be actionable, low-noise, and tied to user impact.

Batch-style pipelines behave differently than request/response APIs. Prefer run-based and item-based alerts instead of wall-clock throughput over idle time.

## Principles

1. Alert on symptoms, not every error
2. Use severity levels consistently
3. Include context in the alert payload
4. Separate SLO alerts from operational alerts

## Dashboards

Pair alerts with dashboards that show step latency, item throughput (while running), and error rates.

## Common Alerts

1. Run failure rate above threshold (orchestrator)
2. Step error rate above SLO (gRPC server spans)
3. Item latency above SLO (run average or per-step)
4. Backpressure rising (buffer queued stays high)
5. Orchestrator runtime failure or restart loops

## Practical Defaults

Start with:

1. Run failure rate > 1% over 1 day (warning)
2. Item avg latency > 2x baseline for 10 minutes (warning)
3. Buffer queued stays high for 5 minutes (warning)
4. Execution DLQ backlog growth sustained for 5 minutes (critical, provider queue-depth metric)
5. Item reject sink backlog growth sustained for 5 minutes (critical, provider queue-depth metric; in-memory sink uses retained-size logs instead of a backlog gauge)

Queue-async additions:

6. Due-sweeper recoveries stop while due backlog rises (critical)
7. Lease conflict/stale-commit rate spikes above baseline (warning)
8. Retry-saturation exceeds threshold (warning/critical by tenant tier)
9. Queue age/lag exceeds execution SLO budget (critical)

When using New Relic, derive these from `tpf.pipeline.run` spans, `tpf.step.*` metrics (for example `tpf.step.reject.total`), and provider-native queue-depth metrics for DLQ/reject backlog.

Suggested starter thresholds:

1. Queue oldest-message age > 2x target execution SLO for 10 minutes (critical).
2. Retry-saturation ratio > 0.2 for 15 minutes (warning), > 0.4 (critical).
3. Sweeper recoveries = 0 while due backlog grows for 5 minutes (critical).
4. Lease/stale conflict rate > 3x 7-day baseline for 10 minutes (warning).

## What Alerts Mean Operationally

Use channel-specific interpretation so incidents route to the right team.

### Execution DLQ Backlog Growth (Critical)

Operational meaning:

1. Terminal execution failures are accumulating faster than triage/re-drive.
2. Queue-async control plane may be healthy, but execution outcomes are failing systemically.

Business meaning:

1. End-to-end workflows are not completing.
2. Customer-visible outcomes can be delayed, missing, or inconsistent until replay.

Immediate operator actions:

1. Identify dominant terminal causes (`FAILED` vs `DLQ`) and affected contracts/steps.
2. Validate downstream idempotency before any bulk re-drive.
3. Re-drive in bounded batches and watch duplicate suppression and retry saturation.

### Item Reject Sink Backlog Growth (Critical)

Operational meaning:

1. Recover-and-continue paths are active, but rejected items are not being drained.
2. Step execution may still be healthy while reject-handling capacity is insufficient.

Business meaning:

1. Main workflows can complete, but rejected records accumulate unresolved business exceptions.
2. SLA risk is often on data completeness/quality rather than total platform availability.

Immediate operator actions:

1. Segment by reject fingerprint/error class to isolate top failure cohorts.
2. Coordinate data/business remediation for dominant reject reasons.
3. Re-drive only corrected cohorts and track repeat-reject ratio.

### Worker Lag / Queue Age Breach (Critical)

Operational meaning:

1. Dispatch and processing are behind incoming workload or dependency latency budget.
2. Recovery paths (retry/sweeper) may amplify lag if left unchecked.

Business meaning:

1. End-user latency and completion-time SLOs are at risk.
2. Time-sensitive workflows may miss windows even without outright failure.

Immediate operator actions:

1. Check dependency latency/error spikes and retry amplification signals.
2. Scale workers or reduce ingest pressure temporarily.
3. Verify sweeper activity and lease conflict levels during catch-up.
