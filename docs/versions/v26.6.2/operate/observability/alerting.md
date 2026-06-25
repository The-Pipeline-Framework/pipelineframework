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

1. Due-sweeper recoveries stop while due backlog rises (critical)
2. Lease conflict/stale-commit rate spikes above baseline (warning)
3. Retry-saturation exceeds threshold (warning/critical by tenant tier)
4. Queue age/lag exceeds execution SLO budget (critical)
5. Await dropped completions are non-zero outside known duplicate/retry windows (warning/critical by workflow)
6. Early-held await completions rise without matching resume releases (warning)
7. Object Publish failures are non-zero for terminal-output pipelines (critical)
8. Object Ingest failures or duplicate-admission spikes exceed baseline (warning)

When using New Relic, derive these from `tpf.pipeline.run` spans, `tpf.step.*` metrics (for example `tpf.step.reject.total`), and provider-native queue-depth metrics for DLQ/reject backlog.

Suggested starter thresholds:

1. Queue oldest-message age > 2x target execution SLO for 10 minutes (critical).
2. Retry-saturation ratio > 0.2 for 15 minutes (warning), > 0.4 (critical).
3. Sweeper recoveries = 0 while due backlog grows for 5 minutes (critical).
4. Lease/stale conflict rate > 3x 7-day baseline for 10 minutes (warning).
5. `rate(tpf_await_completion_dropped_total[5m]) > 0` for 10 minutes (warning; critical when paired with provider completion backlog).
6. `increase(tpf_await_completion_early_held_total[10m]) > increase(tpf_await_resume_released_total[10m])` for 10 minutes (warning; check parent wait persistence and dispatch completion).
7. `rate(tpf_object_publish_failed_total[5m]) > 0` for 5 minutes (critical for pipelines where terminal output is contractual).
8. `rate(tpf_object_ingest_failed_total[5m]) > 0` for 10 minutes (warning), or duplicate admissions > 3x baseline (warning).

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
2. Availability risk is often on data completeness/quality rather than total platform uptime.

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

### Await Boundary Flow Stall (Warning/Critical)

Operational meaning:

1. External completions are arriving, but downstream business steps are not progressing at the expected rate.
2. On the live path, the issue is usually downstream demand, worker capacity, broker lag, or state-store write latency.
3. On the durable fallback path, the parent execution may not have reached durable `WAITING_EXTERNAL`, dispatch completion may be delayed, or the worker queue may be saturated.

Business meaning:

1. Provider work may have completed, but the pipeline is not yet turning those completions into downstream business transitions.
2. User-visible completion time can breach even when the provider itself is healthy.

Immediate operator actions:

1. Compare `tpf.await.completion.admitted.total`, downstream step throughput, Object Publish progress, and provider-native completion queue age.
2. Inspect replay for `await_unit_item_completed`, downstream step events, and Object Publish events; use `await_execution_waiting` and `await_resume_released` to diagnose durable fallback.
3. Check queue-async worker lag and state-store write latency before increasing provider throughput.

### Object Publish Failure (Critical)

Operational meaning:

1. Terminal output reached the connector boundary, but the object target did not accept the write.
2. The execution should not be marked successful until configured Object Publish completes.

Business meaning:

1. The business transition completed in memory, but the expected durable output file/object is missing or delayed.
2. Downstream consumers that rely on object output can see incomplete results.

Immediate operator actions:

1. Check `tpf.object_publish.failed.total`, `tpf.object_publish.published.total`, and write-duration p95/p99 by target/provider.
2. Inspect replay for `object_publish_grouped`, `object_publish_published`, and `object_publish_failed` events; object keys are in replay, not metric labels.
3. Validate target credentials, permissions, disk/S3 availability, and idempotency before replaying failed executions.

### Object Ingest Admission Spike (Warning)

Operational meaning:

1. Source listing is returning failed or duplicate admissions above the normal baseline.
2. This can be a source-store issue, a mapping error, or expected duplicate listing after a restart.

Business meaning:

1. New inputs may not be admitted into queue-async executions, or duplicate source notifications may be creating avoidable load.

Immediate operator actions:

1. Compare listed, submitted, duplicate, and failed Object Ingest counters by source/provider.
2. Inspect replay/span events for object keys and idempotency identity.
3. Check provider-native source backlog or object-store notification health before scaling consumers.
