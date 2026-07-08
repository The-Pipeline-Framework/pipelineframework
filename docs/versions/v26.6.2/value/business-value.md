---
search: false
---

# Business Value

<p class="value-lead">The Pipeline Framework (TPF) helps teams ship Java business flows faster while staying on portable Quarkus foundations.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Faster Delivery</strong> &middot; Write business functions while TPF generates the calling code around them.</div>
  <div class="value-glance-item"><strong>Lower Change Cost</strong> &middot; YAML flow declarations and explicit input/output types reduce surprises when flows change.</div>
  <div class="value-glance-item"><strong>Queryable Results</strong> &middot; Persistence keeps useful business state available for APIs, reports, and UI queries after processing.</div>
</div>

## Use This When

- Teams spend significant time on glue code and keeping service-to-service contracts consistent.
- Changes ripple across multiple services just to update a Java type or API shape.
- You want an architecture path from “works as a monolith” to “split into deployable units/services” without rewriting everything.

## Observed Impact (CSV Payments)

In the CSV Payments example, the prior implementation required substantially more manual integration work.
Compared to the current TPF-based approach, it was materially weaker in maintainability, extensibility, and operational clarity.

This is not a controlled study and results vary by team and process. The signal is that the framework structure can remove enough repeated work to shift delivery timelines meaningfully.

## Expected Outcomes

Teams typically aim for:

1. Faster delivery for comparable scope, often by eliminating hand-built callers and conventions.
2. Lower cost of change because Java types, mappers, and generated function calls stay consistent.
3. Higher operational readiness because generated runtime files, retries, and failure handling make it clearer what is running.
4. Better application-state handling because processed results can be persisted and queried later without bespoke storage glue.

## What It Enables

1. Faster iteration: change one function without reworking the entire flow.
2. Predictable scaling: each function can be tuned around its own workload characteristics.
3. Better ROI: less boilerplate, shorter lead times, and fewer custom integration layers.

## Reuse Existing Compute Logic

Operators let teams plug proven Java methods into `pipeline.yaml` without rewriting them as new services first.
That reduces migration risk and preserves prior engineering investment while still benefiting from TPF build-time validation and generated callers.

Typical reuse targets include domain rule engines, validators/enrichers, and transformation libraries already used in production.

## Durable State and Replay Without Bespoke Infrastructure

Persistence and caching deserve separate attention because they do more than add side effects. Persistence gives teams durable business state they can query later from APIs or UIs. Caching protects expensive steps and makes replay or recomputation much cheaper when downstream logic changes.

Together they give TPF a practical state-and-replay story: keep the durable records you care about, then selectively recompute the parts that should change instead of inventing separate replay plumbing.

## Jump to Guides

<div class="value-links">

- [Quick Start](/versions/v26.6.2/design/pipeline-studio/)
- [Canvas Guide](/versions/v26.6.2/design/pipeline-studio/canvas-guide)
- [Runtime Layouts](/versions/v26.6.2/deploy/runtime-layouts/)
- [Operators](/versions/v26.6.2/design/operators)
- [Operator Reuse Strategy](/versions/v26.6.2/design/operator-reuse-strategy)
- [State, Replay, and Queryable Data](/versions/v26.6.2/value/state-replay-and-queryable-data)
- [Using Plugins](/versions/v26.6.2/develop/using-plugins)
- [Observability](/versions/v26.6.2/operate/observability/)

</div>
