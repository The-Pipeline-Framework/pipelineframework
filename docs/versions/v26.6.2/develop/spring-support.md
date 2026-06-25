---
search: false
---

# Spring Support Status

Quarkus is the mature production runtime for TPF today.

Spring support has started, but it is intentionally limited. Treat it as an emerging adapter path, not as production parity with the Quarkus runtime.

## What Works Today

Current Spring work proves a narrow generated application path:

- YAML-declared unary local steps,
- constrained `REST + COMPUTE` unary smoke coverage,
- generated Spring `@Component` local step beans,
- Spring WebFlux `@RestController` resources for the supported smoke path,
- `Mono<Out>` authored service methods for YAML-declared Spring-profile unary services,
- shared runner-core sequencing through the neutral runtime seam.

## Not Yet Supported

Spring does not yet have parity for:

- gRPC,
- function handlers,
- await, checkpoint, durable coordinator, or broker paths,
- persistence providers,
- delegated/operator steps,
- side effects and plugins,
- REST client-step remote boundaries,
- streaming or non-unary shapes,
- production Spring observability and deployment guidance.

Unsupported combinations should fail at build time instead of silently falling back to Quarkus generation.

## Authoring Guidance

Use Quarkus for production TPF applications today.

Use the Spring path only when you are validating the emerging renderer/runtime seam or contributing to portability work. Keep business contracts neutral: typed inputs, typed outputs, mappers, and YAML declarations should not rely on Quarkus-only application code unless the application is explicitly Quarkus-targeted.

## Deeper Architecture

- [Runtime Core Decoupling](/versions/v26.6.2/evolve/runtime-core-decoupling)
- [Framework Portability Assessment](/versions/v26.6.2/evolve/framework-portability-assessment/)
- [Runtime Split](/versions/v26.6.2/evolve/framework-portability-assessment/runtime-split)
- [Code Generation Portability](/versions/v26.6.2/evolve/framework-portability-assessment/code-generation)

