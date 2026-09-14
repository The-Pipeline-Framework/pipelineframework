# ADR-0055: Protocol-carried lifecycle values are shared runtime model

status: accepted

## Context

The durable Await store interfaces are provider SPIs, but the immutable values they
store also cross the transition-worker protocol boundary. `TransitionAwaitSuspension`
embeds `AwaitInteractionRecord` and `AwaitUnitRecord`. Transition commands likewise
embed `ExecutionResultShape` and `ExecutionRedriveIntent`. These values currently make
`pipelineframework-runtime-protocol` depend on `pipelineframework-runtime-spi` merely
to describe wire-visible durable execution state.

That dependency points from shared protocol data toward provider operations and makes
consumers of the protocol acquire reactive storage-extension contracts they do not use.

## Decision

`pipelineframework-runtime-core` owns `AwaitInteractionRecord`, `AwaitUnitRecord`,
`AwaitInteractionStatus`, `AwaitUnitStatus`, `CommandDispatchSettlement`,
`ExecutionResultShape`, and `ExecutionRedriveIntent`. These types retain their packages,
constructors, record components, enum values, validation, state-transition behaviour,
and serialized shapes.

`pipelineframework-runtime-spi` continues to own the Await store interfaces and their
reactive operation result contracts. Runtime implementations continue to own storage,
coordination, codecs, transport adapters, telemetry, timeout processing, and recovery.

`pipelineframework-runtime-protocol` consumes the Await values from runtime-core and
no longer depends on runtime-spi. This refines the value ownership recorded in
ADR-0044; its decision that durable Await store operations belong to runtime-spi
remains in force.

## Rationale

Durable lifecycle facts are shared runtime model whether they are read from a store,
embedded in a transition envelope, or inspected by a runtime host. Provider operations
remain SPI. Separating values from operations leaves shared protocol data dependent on
the shared model rather than on a reactive extension surface.

Preserving fully qualified names makes the artifact correction transparent to existing
Java source while retaining the compatibility obligations of these highly central
records.

## Consequences

- Runtime and protocol consumers obtain Await lifecycle values from
  `pipelineframework-runtime-core` without loading reactive provider contracts.
- Await storage providers still implement `pipelineframework-runtime-spi` and receive
  the shared values through its runtime-core dependency.
- Source and binary compatibility apply to the moved Java types; serialized and replay
  compatibility additionally apply to their record components, enum names, validation,
  and state-transition behaviour.
- `PipelineTransitionWorker` remains runtime-owned: it selects local and remote
  invocation clients and returns `TransitionResultEnvelope`, which intentionally carries
  runtime-local decoded application objects. Customer runtimes and remote workers share
  `TransitionCommandEnvelope` and `TransitionWireResult` instead.
