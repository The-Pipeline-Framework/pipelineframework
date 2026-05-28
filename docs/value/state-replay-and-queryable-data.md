# State, Replay, and Queryable Data

<p class="value-lead">TPF helps developers keep useful application state, replay work safely, and expose queryable results to downstream services or UIs without hand-building separate storage and replay plumbing.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Durable Business State</strong> &middot; Persistence stores business outputs so teams can query them later from APIs, reports, or user interfaces.</div>
  <div class="value-glance-item"><strong>Fast Recompute</strong> &middot; Caching reuses expensive step outputs and makes replay or rewind far cheaper.</div>
  <div class="value-glance-item"><strong>Better Together</strong> &middot; Persistence keeps the durable record; cache accelerates derived-state recomputation and selective replay.</div>
</div>

## Use This When

- A background process produces data that a UI needs to query later.
- Expensive steps should not rerun every time downstream logic changes.
- Teams want replay or recompute capabilities without inventing a separate event-storage and replay layer.

## What Persistence Gives You

Persistence stores pipeline data without changing the flow itself. In practical terms, this gives developers a durable record they can query later from downstream APIs, reports, admin tools, or user interfaces.

This matters outside microservices too. In a regular application, persistence often solves the whole "how do I show the processed results in the UI later?" problem. TPF keeps that storage concern aligned with reactive execution instead of forcing developers into custom persistence wrappers around every function.

## What Caching Gives You

Caching protects expensive steps by reusing outputs that are still valid. That helps with throughput in normal production runs, and it becomes even more valuable when you need to replay or recompute part of a flow.

If a downstream step changes, cache can let you reuse earlier stable outputs instead of rerunning the whole pipeline. This is the practical replay story: keep the durable records you care about, then selectively recompute the parts that should change.

## Why They Work Best Together

Persistence and caching solve different parts of the same problem:

1. Persistence keeps the durable business record for audit, query, and follow-on processing.
2. Cache keeps reusable derived outputs close at hand so replay and recomputation stay fast.
3. Together they reduce the need for bespoke state stores, custom read models, or one-off replay scripts.

## Concrete Examples

- **Search**: persist crawl and parse outputs, cache tokenize and index outputs, then replay downstream indexing changes without re-crawling everything.
- **Business application with a UI**: process inputs in the pipeline, persist the resulting business records, and let the UI query them later through a normal API.
- **Background processing**: keep durable state for recovery and reporting, while cache reduces the cost of replaying expensive downstream steps.

## Jump to Guides

<div class="value-links">

- [Persistence Plugin](/guide/plugins/persistence)
- [Caching](/guide/plugins/caching/)
- [Cache vs Persistence](/guide/plugins/caching/cache-vs-persistence)
- [Search Replay Walkthrough](/guide/plugins/caching/replay-walkthrough)
- [Using Plugins](/guide/development/using-plugins)
- [Orchestrator Runtime](/guide/development/orchestrator-runtime/)

</div>
