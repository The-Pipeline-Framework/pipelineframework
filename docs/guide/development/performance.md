---
title: Performance
---

# Performance

This guide explains how pipeline throughput is affected by parallelism, step cardinality, and execution strategy.

## Parallel Execution Model

Client steps can process multiple items from the same stream concurrently. This is especially useful when some items are slow while others are fast, because it prevents the slow items from blocking the whole stream.

Parallelism is configured at the pipeline level. See the [Configuration Reference](/guide/build/configuration/) for the exact settings.

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
[Expansion and Reduction](/guide/design/expansion-and-reduction).

## Server Execution Strategy

Service-side execution (event loop vs. blocking or virtual threads) affects throughput for I/O heavy steps. See [@PipelineStep Annotation](/guide/development/pipeline-step) for service-side execution options.
