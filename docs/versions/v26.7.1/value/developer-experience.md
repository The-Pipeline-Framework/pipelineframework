---
search: false
---

# Developer Experience

<p class="value-lead">TPF lets teams write focused Java business transformations while the framework builds, checks, and runs the shell around them.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Start Fast</strong> &middot; Model the flow in YAML or sketch a baseline in Pipeline Studio.</div>
  <div class="value-glance-item"><strong>Type Safety</strong> &middot; Explicit input/output types catch function, mapper, and operator mismatches at build time.</div>
  <div class="value-glance-item"><strong>Less Boilerplate</strong> &middot; REST, gRPC, local, and cloud-function callers are generated for you.</div>
</div>

## Use This When

- You want new developers productive quickly.
- Refactors are becoming risky because Java types and API shapes drift across modules.
- Teams are spending too much time on repeated integration glue.

In practice, developers write the domain function and the supporting code that matters: Java types, optional mappers, and clear business decisions. TPF handles the repeated shell around that function, including generated callers, generated runtime files, connector boundaries, and build-time validation.

Quarkus is the mature production runtime today. Spring support has started for a limited local/REST unary path; see [Spring Support Status](/versions/v26.7.1/develop/spring-support) before planning Spring-based applications.

## Operator Reuse

When teams already have stable Java compute libraries, operators let them plug those methods directly into the pipeline flow from `pipeline.yaml`.
This shortens delivery time and avoids duplicate implementations.

## Business Rejections Without Workflow Collapse

Not every failed item is a platform error. TPF lets step authors model per-item rejection as an expected business path using Item Reject Sink, the built-in reject-and-continue mechanism for bad records.
Teams can reject specific records, continue processing the rest of the workload, and keep an audit trail plus a clear replay path that can survive process restarts when backed by a persistent provider.
This avoids custom side channels and keeps recovery logic explicit in step code.

## Jump to Guides

<div class="value-links">

- [Quick Start](/versions/v26.7.1/design/pipeline-studio/)
- [Functional Core, Imperative Shell](/versions/v26.7.1/design/fcis)
- [Operators](/versions/v26.7.1/design/operators)
- [Item Reject Sink](/versions/v26.7.1/develop/item-reject-sink)
- [Mappers and DTOs](/versions/v26.7.1/develop/mappers-and-dtos)
- [Testing](/versions/v26.7.1/develop/testing)

</div>
