---
title: Performance
search: false
---

# Performance

This guide explains how pipeline throughput is affected by parallelism, step cardinality, and execution strategy.

## Parallel Execution Model

Client steps can process multiple items from the same stream concurrently. This is especially useful when some items are slow while others are fast, because it prevents the slow items from blocking the whole stream.

Parallelism is configured at the pipeline level. See [All Settings](/versions/v26.6.2/develop/configuration/all-settings) for the exact settings.

`pipeline.parallelism` controls the execution policy:

- `SEQUENTIAL` forces ordered execution.
- `AUTO` enables parallel execution for expanding steps (1→N), unless a step advises strict ordering.
- `PARALLEL` enables parallel execution for all per-item steps, overriding advisory ordering.

`pipeline.max-concurrency` caps in-flight items during parallel execution to control backpressure and memory usage.

## Avoid Breaking Parallelism

If any step in the chain processes items sequentially, the stream becomes serialized at that point. Downstream steps cannot regain the lost concurrency, because the upstream producer is now emitting items one at a time.

Ordering requirements declared by plugins can force sequential execution or block `PARALLEL` policy entirely.

Practical guidance:

- Keep sequential stages as late as possible in the pipeline.
- Isolate slow, blocking work into dedicated steps so parallel stages can run earlier.

For step shapes and how to reason about expansion vs. reduction, see
[Expansion and Reduction](/versions/v26.6.2/design/expansion-and-reduction).

## Server Execution Strategy

Service-side execution context (event loop vs. worker threads) affects throughput for I/O-heavy steps. Prefer non-blocking I/O and offload truly blocking work using the framework or runtime facilities to avoid starving the event loop.

## Blocking Steps

TPF now supports a first-class blocking authoring path for internal `service:` steps.

- Blocking services are executed on worker threads by default.
- Quarkus YAML-declared internal blocking services that implement the existing blocking service interfaces can set `runOnVirtualThreads: true`; generated bridges pass `true` to `BlockingExecutionSupport`, and generated REST/gRPC entrypoints receive `@RunOnVirtualThread`.
- Spring YAML-only `REST` or `LOCAL` + `COMPUTE` unary blocking internal steps can also set `runOnVirtualThreads: true`; generated Spring steps use `RuntimeAdapters.executeBlocking(..., true)`.
- Blocking authoring is split into two modes:
  - materialized blocking: `BlockingStreamingService`, `BlockingStreamingClientService`, `BlockingBidirectionalStreamingService`
  - incremental blocking: `BlockingIteratorService`

Materialized blocking does not just lose reactive backpressure. It also:

- increases heap usage because full input or output collections live in memory at once
- increases GC pressure and whole-batch retry cost
- delays first output until the full blocking callback finishes
- wastes more CPU work when downstream later fails or cancels

Iterator blocking reduces those materialization costs, but it is still blocking I/O or CPU work running on an offload executor rather than on the event loop.

Use blocking steps when synchronous business code is the practical choice. Refactor to reactive services when you need:

- sustained high throughput under streaming load
- partial consumption without full list materialization and your blocking library cannot express it through an iterator
- native non-blocking I/O instead of worker or virtual-thread offload
