# Business Value

<p class="value-lead">The Pipeline Framework (TPF) helps teams ship Java business flows faster while staying on portable Quarkus foundations.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Faster Delivery</strong> &middot; Write business functions while TPF generates the calling code around them.</div>
  <div class="value-glance-item"><strong>Lower Change Cost</strong> &middot; Explicit input/output types reduce surprises when flows change.</div>
  <div class="value-glance-item"><strong>Less Lock-in</strong> &middot; Plain Java, Quarkus, REST, gRPC, and local calls keep escape hatches real.</div>
</div>

## Use This When

- Teams spend significant time on glue code and keeping service boundaries consistent.
- Changes ripple across multiple services just to update a Java type or API shape.
- You want an architecture path from “works as a monolith” to “split into deployable units/services” without rewriting everything.

## Observed Impact (CSV Payments)

In the CSV Payments example, the prior implementation required substantially more manual integration work.
Compared to the current TPF-based approach, it was materially weaker in maintainability, extensibility, and operational clarity.

This is not a controlled study and results vary by team and process. The signal is that the framework structure can remove enough repeated work to shift delivery timelines meaningfully.

## Expected Outcomes

Teams typically aim for:

1. Faster delivery for comparable scope, often by eliminating hand-built adapters and conventions.
2. Lower cost of change because Java types, mappers, and generated call paths stay consistent.
3. Higher operational readiness because generated runtime files make it clearer what is running.

## What It Enables

1. Faster iteration: change one function without reworking the entire flow.
2. Predictable scaling: each function can be tuned around its own workload characteristics.
3. Better ROI: less boilerplate, shorter lead times, and fewer bespoke integration layers.

## Reuse Existing Compute Logic

Operators let teams plug proven Java methods into `pipeline.yaml` without rewriting them as new services first.
That reduces migration risk and preserves prior engineering investment while still benefiting from TPF build-time validation and generated call paths.

Typical reuse targets include domain rule engines, validators/enrichers, and transformation libraries already used in production.

## Jump to Guides

<div class="value-links">

- [Quick Start](/guide/getting-started/)
- [Canvas Guide](/guide/getting-started/canvas-guide)
- [Runtime Layouts](/guide/build/runtime-layouts/)
- [Operators](/guide/build/operators)
- [Operator Reuse Strategy](/guide/design/operator-reuse-strategy)
- [Using Plugins](/guide/development/using-plugins)
- [Observability](/guide/operations/observability/)

</div>
