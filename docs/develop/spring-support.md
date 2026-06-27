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
- unary delegated Spring bean steps for local pipeline execution,
- shared runner-core sequencing through the neutral runtime seam.

## Not Yet Supported

Spring does not yet have parity for:

- gRPC,
- function handlers,
- await, checkpoint, durable coordinator, or broker paths,
- persistence providers,
- arbitrary `Class::method` operator invokers and non-local delegated/operator paths,
- side effects and plugins,
- REST client-step remote boundaries,
- streaming or non-unary shapes,
- production Spring observability and deployment guidance.

Unsupported combinations should fail at build time instead of silently falling back to Quarkus generation.

## Authoring Guidance

Use Quarkus for production TPF applications today.

Use the Spring path only when you are validating the emerging renderer/runtime seam or contributing to portability work. Keep business contracts neutral: typed inputs, typed outputs, mappers, and YAML declarations should not rely on Quarkus-only application code unless the application is explicitly Quarkus-targeted.

Delegated Spring beans are supported only for unary local steps declared from YAML with a Spring bean class and a supported service shape such as `process(In): Mono<Out>` or `processBlocking(In): Out`. They do not imply gRPC, function, await/checkpoint, broker, durable coordinator, or production parity with the Quarkus runtime.

## Deeper Architecture

- [Runtime Core Decoupling](/evolve/runtime-core-decoupling)
- [Framework Portability Assessment](/evolve/framework-portability-assessment/)
- [Runtime Split](/evolve/framework-portability-assessment/runtime-split)
- [Code Generation Portability](/evolve/framework-portability-assessment/code-generation)
