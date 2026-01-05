---
title: Performance
---

# Performance

This guide explains how pipeline throughput is affected by parallelism, step cardinality, and execution strategy.

## Parallel Execution Model

Client steps can process multiple items from the same stream concurrently. This is especially useful when some items are slow while others are fast, because it prevents the slow items from blocking the whole stream.

Parallelism is configured at runtime per step. See the [Configuration Reference](/guide/build/configuration/) for the exact settings.

## Avoid Breaking Parallelism

If any step in the chain processes items sequentially, the stream becomes serialized at that point. Downstream steps cannot regain the lost concurrency, because the upstream producer is now emitting items one at a time.

Practical guidance:

- Keep sequential stages as late as possible in the pipeline.
- Isolate slow, blocking work into dedicated steps so parallel stages can run earlier.

For step shapes and how to reason about expansion vs. reduction, see
[Expansion and Reduction](/guide/design/expansion-and-reduction).

## Server Execution Strategy

Service-side execution (event loop vs. blocking or virtual threads) affects throughput for I/O heavy steps. See [@PipelineStep Annotation](/guide/development/pipeline-step) for service-side execution options.
