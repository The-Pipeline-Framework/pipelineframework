---
search: false
---

# Operational Confidence

<p class="value-lead">The Pipeline Framework (TPF) is designed for production environments where teams need retries, crash recovery, failed-work handling, and runtime health to stay understandable.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Container Ready</strong> &middot; Fits naturally into container and Kubernetes deployment models.</div>
  <div class="value-glance-item"><strong>Runtime Visibility</strong> &middot; Generated endpoints, status data, and telemetry make it easier to see what is running and how work moves.</div>
  <div class="value-glance-item"><strong>Crash Survival</strong> &middot; For persistence-backed background execution such as `QUEUE_ASYNC`: accepted work can be stored outside the JVM, retried, recovered, and investigated.</div>
</div>

## Use This When

- Production incidents take too long to diagnose.
- Teams need a consistent operational baseline across services.
- Container/Kubernetes rollout has outpaced ops maturity.

For background execution, TPF can record accepted work outside the current process before it depends on that work being complete. If the JVM, container, or worker dies, another worker can pick up the stored execution and run it again after the lease expires. This is lease-based recovery, not mid-pipeline checkpoint resume, so teams should design downstream calls and writes to be safe when the same work is attempted again.

The exact TPF mode for this is `QUEUE_ASYNC`. In plain terms, this is the background execution mode where TPF stores execution state, dispatches work, retries failed transitions, recovers leased work after crashes, and can publish terminal failures to a DLQ, a dead-letter channel for investigation or replay.

Persistence and caching strengthen this recovery story. Persistence gives teams durable business records they can inspect or query after a failure. Cache reduces the cost of replaying expensive downstream work when recovery requires recomputation instead of a full rerun.

## Jump to Guides

<div class="value-links">

- [Error Handling & DLQ](/versions/v26.5.2/guide/operations/error-handling)
- [In-flight Probe](/versions/v26.5.2/guide/operations/in-flight-probe)
- [Orchestrator Runtime](/versions/v26.5.2/guide/development/orchestrator-runtime/)
- [State, Replay, and Queryable Data](/versions/v26.5.2/value/state-replay-and-queryable-data)
- [Observability](/versions/v26.5.2/guide/operations/observability/index)
- [Best Practices](/versions/v26.5.2/guide/operations/best-practices)

</div>
