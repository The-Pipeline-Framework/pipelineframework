# Roadmap and Guardrails

| Slice | Complexity | Risk | Impact |
| --- | --- | --- | --- |
| Add dependency-guard tests for proposed `runtime-core` packages | Low | Low | Prevents new Quarkus/Vert.x/CDI leakage |
| Introduce `BeanLookup` and replace direct `Arc.container()` calls | Low | Medium | Removes direct Quarkus container binding |
| Introduce `RuntimeProfile` and remove direct launch-mode reads in core candidates | Low | Low | Makes config/profile portable |
| Add `ExecutionContextCarrier` abstraction for Vert.x locals | Medium | Medium | Required for Reactors context propagation |
| Make YAML services valid without `@PipelineStep` | Medium | Medium | Unlocks annotation-removal trajectory |
| Split `framework/runtime` into core plus Quarkus runtime artifact | Medium-High | High | Creates real portability boundary |
| Extract neutral runner sequencing core | Medium | High | Lets Quarkus and Spring share execution ordering instead of forking runners |
| Convert store SPIs to neutral async types | Medium | High | Enables non-Mutiny providers |
| Add renderer-profile registry | Medium | Medium | Keeps semantic model stable while adding Spring generation |
| Add Spring Boot runtime adapter skeleton | Medium | Medium | Proves Spring can host runtime-core seams without Quarkus runtime dependencies |
| Build minimal Spring Boot unary/local pipeline | Medium | High | First generated Spring local unary proof |
| Build minimal Spring Boot unary/REST pipeline | Medium | High | First generated Spring WebFlux unary proof |
| Add full Spring WebFlux/Reactor/gRPC/await/checkpoint parity | High | High | Production-capable portability |

First portability PR gates:

1. Framework tests pass for Quarkus runtime and deployment.
2. Dependency guard: no `io.quarkus`, `jakarta.enterprise`, `io.vertx`, or `io.smallrye.mutiny` leaks in selected core packages.
3. Existing Quarkus example compiles unchanged.

Guardrails during any portability work:

- Build-time validation stays primary.
- Mapper pair matching remains deterministic.
- Cardinality and split/merge lineage stays replay-safe.
- Quarkus behavior remains source-compatible through extraction.
- Transport/platform remain orthogonal.
- Vert.x context migration is explicit, not accidental.
- Spring support does not force Reactor as a hard requirement for Quarkus users.
