---
search: false
---

# Architecture

This page is the current architecture map for TPF internals.

Use public Design/Develop/Deploy docs for application usage. Use this page when changing framework internals, compiler behavior, runtime ownership, or portability seams.

## Current Architecture Reading Path

| Area | Start here |
| --- | --- |
| Build-time compiler phases, IR, bindings, renderers | [Annotation Processor](/versions/v26.6.2/evolve/annotation-processor/) |
| Generated artifacts and module ownership | [Pipeline Compilation](/versions/v26.6.2/develop/pipeline-compilation/) |
| Runtime layout vs build topology | [Runtime Layouts](/versions/v26.6.2/deploy/runtime-layouts/) |
| Queue-async durable execution | [Queue-Async Runtime](/versions/v26.6.2/deploy/orchestrator-runtime/queue-async) |
| Await and callback admission | [Await Boundaries](/versions/v26.6.2/design/await-boundaries) and [Await Runtime Setup](/versions/v26.6.2/deploy/orchestrator-runtime/await) |
| Checkpoint handoff | [Checkpoint Handoff](/versions/v26.6.2/deploy/orchestrator-runtime/checkpoint-handoff) |
| Brokered runtime boundaries | [Brokered Runtime Boundaries](/versions/v26.6.2/evolve/brokered-boundaries/) |
| Runtime portability and Spring status | [Runtime Core Decoupling](/versions/v26.6.2/evolve/runtime-core-decoupling) and [Spring Support Status](/versions/v26.6.2/develop/spring-support) |
| MCP/template generation boundary | [MCP and Template Generation](/versions/v26.6.2/develop/mcp-template-generation) |

## Current Control-Plane Shape

The current durable direction is a queue-driven async control plane:

1. generated ingress accepts work over the configured transport,
2. execution state is persisted,
3. work is dispatched,
4. a worker claims a lease,
5. one transition runs,
6. state is committed,
7. terminal state is exposed through status/result APIs or failures move to the configured DLQ.

This is not a full event-sourced runtime in the current milestone. New durable control-plane storage should prefer immutable records, conditional writes, and append-only event records.

## Legacy Reference

The older broad architecture overview is preserved at [Architecture Reference](/versions/v26.6.2/evolve/architecture-reference). Treat it as background, not as the canonical reading path.
