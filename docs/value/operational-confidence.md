# Kube-Native

<p class="value-lead">The Pipeline Framework (TPF) is designed for containerized environments where teams need failures, retries, and runtime health to stay understandable.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Container First</strong> &middot; Works naturally with Kubernetes-style deployment models.</div>
  <div class="value-glance-item"><strong>Runtime Visibility</strong> &middot; Generated runtime files and telemetry show what is running and how calls move.</div>
  <div class="value-glance-item"><strong>Failure Handling</strong> &middot; Consistent retries, dead-letter handling, health, and observability practices.</div>
</div>

## Use This When

- Production incidents take too long to diagnose.
- Teams need a consistent operational baseline across services.
- Container/Kubernetes rollout has outpaced ops maturity.

For background execution, TPF can record accepted work outside the current process, so crashes and restarts do not lose it. Terminal execution failures can go to a DLQ, a dead-letter channel for investigation or replay.

## Jump to Guides

<div class="value-links">

- [Error Handling & DLQ](/guide/operations/error-handling)
- [In-flight Probe](/guide/operations/in-flight-probe)
- [Observability](/guide/operations/observability/index)
- [Best Practices](/guide/operations/best-practices)

</div>
