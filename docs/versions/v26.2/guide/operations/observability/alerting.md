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
4. Backpressure pressure rising (buffer queued stays high)
5. Orchestrator runtime failure or restart loops

## Practical Defaults

Start with:

1. Run failure rate > 1% over 1 day (warning)
2. Item avg latency > 2x baseline for 10 minutes (warning)
3. Buffer queued stays high for 5 minutes (warning)
4. DLQ growth sustained for 5 minutes (critical)

When using New Relic, derive these from `tpf.pipeline.run` spans and `tpf.step.*` metrics.
