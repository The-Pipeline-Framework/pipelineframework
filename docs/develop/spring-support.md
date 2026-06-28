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
- unary `Class::method` Spring operator beans for local pipeline execution,
- shared runner-core sequencing through the neutral runtime seam.

## Not Yet Supported

Spring does not yet have parity for:

- gRPC,
- function handlers,
- await, checkpoint, durable coordinator, or broker paths,
- persistence providers,
- non-local delegated/operator paths,
- side effects and plugins,
- REST client-step remote boundaries,
- streaming or non-unary shapes,
- production Spring observability and deployment guidance.

Unsupported combinations should fail at build time instead of silently falling back to Quarkus generation.

## Authoring Guidance

Use Quarkus for production TPF applications today.

Use the Spring path only when you are validating the emerging renderer/runtime seam or contributing to portability work. Keep business contracts neutral: typed inputs, typed outputs, mappers, and YAML declarations should not rely on Quarkus-only application code unless the application is explicitly Quarkus-targeted.

Delegated Spring beans are supported only for unary local steps declared from YAML with either a Spring bean class or an explicit `Class::method` operator reference. Class-only delegates use supported TPF-style service shapes such as `process(In): Mono<Out>` or `processBlocking(In): Out`. `Class::method` delegates support narrow unary Spring bean methods: `Out method(In)`, `Mono<Out> method(In)`, or `CompletionStage<Out> method(In)`.

This support does not imply gRPC, function, await/checkpoint, broker, durable coordinator, streaming/non-unary, or production parity with the Quarkus runtime.

## Deeper Architecture

- [Runtime Core Decoupling](/evolve/runtime-core-decoupling)
- [Framework Portability Assessment](/evolve/framework-portability-assessment/)
- [Runtime Split](/evolve/framework-portability-assessment/runtime-split)
- [Code Generation Portability](/evolve/framework-portability-assessment/code-generation)
