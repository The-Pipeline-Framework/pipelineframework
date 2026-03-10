# Developer Joy

<p class="value-lead">TPF removes the repetitive parts of delivery so teams can focus on business behavior instead of plumbing.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Start Fast</strong> &middot; Design in Canvas and generate a runnable baseline.</div>
  <div class="value-glance-item"><strong>Type Safety</strong> &middot; Shared `common` contracts catch drift at build time.</div>
  <div class="value-glance-item"><strong>Less Boilerplate</strong> &middot; Adapters and client layers are generated for you.</div>
</div>

## Use This When

- You want new developers productive quickly.
- Refactors are becoming risky because contracts drift across modules.
- Teams are spending too much time on integration wiring.

## Operator Reuse

When teams already have stable Java compute libraries, operators let them plug those methods directly into pipeline flow from `pipeline.yaml`.
This shortens delivery time and avoids duplicate implementations.

## Business Rejections Without Workflow Collapse

Not every failed item is a platform error. TPF lets step authors model per-item rejection as an expected business path using Item Reject Sink.
Teams can reject specific records, continue processing the rest of the workload, and keep a durable audit/re-drive trail.
This avoids custom side channels and keeps recovery logic explicit in step code.

## Jump to Guides

<div class="value-links">

- [Quick Start](/guide/getting-started/)
- [Canvas Guide](/guide/getting-started/canvas-guide)
- [Operators](/guide/build/operators)
- [Item Reject Sink](/guide/development/item-reject-sink)
- [Mappers and DTOs](/guide/development/mappers-and-dtos)
- [Testing with Testcontainers](/guide/development/testing)

</div>
