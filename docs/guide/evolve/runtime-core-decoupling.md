# Runtime-Core Decoupling (Renderer Profile + Vert.x Seam)

## Why this slice exists

This iteration creates the minimum boundary needed to start Quarkus-Spring portability work without changing current runtime semantics.

The goal is:
- keep existing Quarkus behavior intact,
- move runtime-adapter assumptions into explicit contracts,
- and keep runtime behavior selection controlled in deployment/runtime profiles.

## New runtime boundary

`framework/runtime-core` now holds framework-neutral contracts used by execution code:

- `BeanLookup`
- `ConfigProvider`
- `ReactiveRuntime`
- `ExecutionContextCarrier`
- `SchedulerBoundary`
- `TransactionBoundary`
- `EventBusBridge`
- `WorkDispatcher`
- `RuntimeAdapters`

`framework/runtime` remains Quarkus-first today and registers concrete implementations in `RuntimeAdapterBootstrap`.

## Renderer profile plumbing

Deployment now accepts and validates `pipeline.codegen.renderer-profile` in discovery:

- `quarkus` (default)
- `spring` (currently mapped to the same renderer selection strategy as `quarkus`)

`FunctionHandlerRendererFactory` now has profile-aware factory methods used by generation.

`PipelineGenerationPhase` passes the profile through and uses renderer-instance FQCN helpers when recording role metadata for orchestrator function handlers. That avoids hard-coding a provider class name at generation time.

## Vert.x seam and context propagation

The important seam for future portability is where Vert.x is used:

- `Framework/runtime-core`: no direct `io.quarkus` / `io.vertx` references.
- `Framework/runtime`: `io.vertx.core` remains in `RuntimeAdapterBootstrap` for context capture.

To make this explicit and enforceable, this slice adds dependency-seam tests:

- `runtime-core` guard: no `io.quarkus` and no `io.vertx` in `src/main/java`.
- `runtime` guard: `io.vertx` must only appear outside a single seam (`RuntimeAdapterBootstrap`).

## Acceptance outcome for this slice

- Quarkus pipeline generation and execution paths remain unchanged.
- Renderer-profile is now a compile-time configuration surface in deployment.
- The core/runtime split becomes the stable seam for subsequent Spring adapter work.

## Notes

- No annotation-removal or async runtime-choice changes were made in this slice.
- Vert.x is treated as container runtime execution plumbing, not as core runtime contract.
