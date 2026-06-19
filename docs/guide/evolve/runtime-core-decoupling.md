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
- `PipelineRunnerCore`

`framework/runtime` remains Quarkus-first today and registers concrete implementations in `RuntimeAdapterBootstrap`.
`framework/runtime-spring` provides the first Spring Boot adapter for these same contracts without depending on the
Quarkus runtime artifact.

The Spring adapter currently proves host integration only:

- Spring bean and config lookup through `ApplicationContext`.
- Thread-local execution context propagation.
- Spring task execution and Java virtual-thread blocking offload.
- Optional Spring transaction manager integration.
- Spring application-event publishing for event and work dispatch seams.

It does not yet provide production Spring parity, Reactor context propagation, persistence providers, broker integration,
or gRPC transport parity.

Kafka await envelope records live on the runtime-core side of the seam so future Spring support can reuse the same request/completion contract. The concrete Kafka await publisher and completion consumer remain Quarkus runtime adapters backed by SmallRye Reactive Messaging.

## Renderer profile plumbing

Deployment now accepts and validates `pipeline.codegen.rendererProfile` in discovery:

- `quarkus` (default)
- `spring`

For the surrounding configuration model, see [build configuration](/guide/build/configuration/) and
[application configuration](/guide/application/configuration).

`FunctionHandlerRendererFactory` now has profile-aware factory methods used by generation. The generation phase context is described in
[annotation processor generation and rendering](/guide/evolve/annotation-processor/generation-and-rendering).

`PipelineGenerationPhase` passes the profile through and uses renderer-instance FQCN helpers when recording role metadata for orchestrator function handlers. That avoids hard-coding a provider class name at generation time.

## Spring unary smoke

The `spring` renderer profile now has two narrow generated execution proofs:

- `pipeline.transport=LOCAL`
- `pipeline.platform=COMPUTE`
- YAML-declared internal services
- reactive-authored `UNARY_UNARY` steps
- generated Spring `@Component` local step beans

Generated Spring local step beans implement the neutral `runtime-core` `PipelineUnaryStep<I, O>` contract and adapt the authored Mutiny service boundary to `CompletionStage` at the generated-code edge. `framework/runtime-spring` contributes `SpringPipelineRunner`, an adapter over the shared `PipelineRunnerCore`; the Quarkus `PipelineRunner` also delegates ordered step sequencing to that same core.

The same profile also supports a constrained `pipeline.transport=REST`, `pipeline.platform=COMPUTE` unary smoke. Generation emits Spring WebFlux `@RestController` resources and the matching Spring unary step beans, then routes the HTTP request through `SpringPipelineRunner` and the shared runner core.

`framework/api` now carries the tiny framework-neutral generated-code surface (`Mapper` and `GeneratedRole`) used by Spring generated applications. `framework/runtime` depends on that API module so existing Quarkus users remain source-compatible through the current runtime artifact.

`framework/spring-smoke-tests` is the first real generated Spring Boot application smoke. It compiles a YAML-only `REST + COMPUTE` unary pipeline with `pipeline.codegen.rendererProfile=spring`, starts a Spring Boot WebFlux test context, invokes the generated REST endpoint, and verifies execution through `SpringPipelineRunner`. The authored service is a plain Spring component with `process(In): Uni<Out>` and no `@PipelineStep` or `ReactiveService` dependency.

Unsupported Spring profile combinations fail at build time instead of falling back to Quarkus generation. The unsupported set still includes Reactor-native generated service contracts, gRPC, function handlers, await/durable/checkpoint/broker paths, persistence, delegated/operator steps, side effects, REST client-step remote boundaries, and non-unary streaming shapes.

## Vert.x seam and context propagation

The important seam for future portability is where Vert.x is used:

- `framework/runtime-core`: no direct `io.quarkus` / `io.vertx` references.
- `framework/runtime`: `io.vertx.core` remains in `RuntimeAdapterBootstrap` for context capture.

To make this explicit and enforceable, this slice adds dependency-seam tests:

- `runtime-core` guard: no `io.quarkus` and no `io.vertx` in `src/main/java`.
- `runtime` guard: `io.vertx` must only appear outside a single seam (`RuntimeAdapterBootstrap`).

## Acceptance outcome for this slice

- Quarkus pipeline generation and execution paths remain unchanged.
- Renderer-profile is now a compile-time configuration surface in deployment.
- The core/runtime split becomes the stable seam for subsequent Spring adapter work.
- Spring Boot can now host the neutral adapter registry without importing the Quarkus runtime module.

## Notes

- No annotation-removal or async runtime-choice changes were made in this slice.
- Vert.x is treated as container runtime execution plumbing, not as core runtime contract.
