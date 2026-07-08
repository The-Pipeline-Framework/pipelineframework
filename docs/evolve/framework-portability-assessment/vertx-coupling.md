# Vert.x Coupling

Vert.x is not a pure Quarkus concern and must be treated separately from CDI/Mutiny lock-in.

| Area | Current behavior | Why it matters for Spring |
| --- | --- | --- |
| Request/runtime context | `PipelineContextHolder`, `PipelineCacheStatusHolder`, `TransportDispatchMetadataHolder`, and `AwaitExecutionContextHolder` use Vert.x locals | Spring WebFlux uses Reactor context; context must move through Reactor signals |
| Persistence context safety | `PersistenceService` uses Vert.x context types for Hibernate Reactive safety | Spring R2DBC should not inherit this path |
| Event-loop safety | Generated/offload guidance relies on Vert.x patterns and `@RunOnVirtualThread` | Spring needs Reactor/event-loop compatible offload policy |
| Renderer output | REST/gRPC renderers emit `io.smallrye.common.annotation.RunOnVirtualThread` in cases | Spring renderer needs scheduler or virtual-thread policy |
| Tests/examples | Some tests use `quarkus-test-vertx` and Vert.x-specific logging | Spring path needs dedicated async/reactive validation |

Keep Vert.x coupling in a clear seam:

- Quarkus path: Vert.x local context + thread locals
- Spring path: Reactor `ContextView` + thread locals where safe

The portability interface should be explicit, e.g. `ExecutionContextCarrier`, to carry pipeline context, cache status, transport metadata, and await context across async boundaries.
