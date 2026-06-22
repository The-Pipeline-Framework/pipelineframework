# TPFGo Roadmap

TPFGo explores how typed checkpoint pipelines can model business progress without turning domain decisions into rollback orchestration.

This page is the current roadmap front door. The older detailed notebook is archived at [Roadmap Archive](/evolve/tpfgo/roadmap-archive).

## Current Direction

- Model business progress as typed checkpoints.
- Keep unhappy business paths explicit instead of encoding them as operational failures.
- Preserve backpressure when pipelines are chained synchronously.
- Treat durable queue/HA delivery as a separate compatibility epic from synchronous business correctness.
- Keep transport-contract parity visible across REST, gRPC, LOCAL, and protobuf-over-HTTP paths, while treating `FUNCTION` as a platform mode that generates function entry points and provider-specific handlers.

## Current Reading Path

| Need | Page |
| --- | --- |
| DDD and boundary alignment | [DDD Alignment](/evolve/tpfgo/ddd-alignment) |
| Application design tradeoffs | [Design Spectrum](/evolve/tpfgo/design-spectrum) |
| Example implementation notes | [TPFGo Example](/develop/tpfgo-example) |
| User-facing checkpoint boundary | [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff) |
| Public state model | [State Model](/design/state-model) |

## Open Work

- Clarify which checkpoint concepts belong in Design, Deploy, and Evolve.
- Keep durable checkpoint handoff documentation tied to current runtime support.
- Avoid presenting speculative TPFGo patterns as general TPF guidance until examples and tests prove them.
