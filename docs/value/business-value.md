# Business Value

<p class="value-lead">The Pipeline Framework (TPF) is built to reduce delivery overhead while keeping you on portable, mainstream runtime foundations.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Faster Delivery</strong> &middot; Generate adapters and keep steps focused on one `process()` contract.</div>
  <div class="value-glance-item"><strong>Lower Change Cost</strong> &middot; Consistency reduces integration drift and refactor fear.</div>
  <div class="value-glance-item"><strong>Less Lock-in</strong> &middot; Plain Java + Quarkus + standard transports keep escape hatches real.</div>
</div>

## Use This When

- Teams spend significant time on glue code, wiring, and “keeping things consistent”.
- Changes ripple across multiple services just to update a contract or endpoint shape.
- You want an architecture path from “works as a monolith” to “split into deployable units/services” without rewriting everything.

## Observed Impact (CSV Payments)

In the CSV Payments example, the prior implementation required substantially more manual integration work.
Compared to the current TPF-based approach, it was materially weaker in maintainability, extensibility, and operational clarity.

This is not a controlled study and results vary by team and process. The signal is that the framework structure can remove enough repeated work to shift delivery timelines meaningfully.

## Expected Outcomes

Teams typically aim for:

1. Faster delivery for comparable scope (often by eliminating hand-built adapters and conventions).
2. Lower cost of change as contracts and transport surfaces stay consistent.
3. Higher operational readiness because generated artifacts and metadata make deployments more legible.

## What It Enables

1. Faster iteration: change steps independently without reworking the entire pipeline.
2. Predictable scaling: each step scales on its own workload characteristics.
3. Better ROI: less boilerplate, shorter lead times, and fewer bespoke integration layers.

## Jump to Guides

<div class="value-links">

- [Quick Start](/guide/getting-started/)
- [Canvas Guide](/guide/getting-started/canvas-guide)
- [Runtime Layouts](/guide/build/runtime-layouts/)
- [Using Plugins](/guide/development/using-plugins)
- [Observability](/guide/operations/observability/)

</div>
